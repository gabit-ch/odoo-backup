import bz2
import gzip
import io
import lzma
import os
import pathlib
import tarfile
import tempfile
import tracemalloc
import unittest
import zlib
from compression import zstd

from odoo_backup import verify
from odoo_backup.config import ALLOWED_FORMATS
from odoo_backup.verify import (
    ArchiveVerificationError,
    StreamVerifier,
    check_magic,
    extract_html_error,
    looks_like_html,
    verify_zip_file,
)
from tests.http_stub import build_backup, build_tar, build_zip, odoo_error_page, odoo_like_members

COMPRESSED_FORMATS = ("tar.gz", "tar.bz2", "tar.xz", "tar.zst")
TAR_FORMATS = ("tar", *COMPRESSED_FORMATS)
MODES = {"tar": "w", "tar.gz": "w:gz", "tar.bz2": "w:bz2", "tar.xz": "w:xz", "tar.zst": "w:zst"}
RECORD = 10240


# --------------------------------------------------------------------------
# helpers
# --------------------------------------------------------------------------


def verify_bytes(fmt: str, data: bytes, chunk_size: int | None = None):
    verifier = StreamVerifier(fmt)
    if chunk_size is None:
        verifier.feed(data)
    else:
        for start in range(0, len(data), chunk_size):
            verifier.feed(data[start : start + chunk_size])
    return verifier.finish()


def compress(fmt: str, data: bytes) -> bytes:
    if fmt == "tar":
        return data
    if fmt == "tar.gz":
        return gzip.compress(data, mtime=0)
    if fmt == "tar.bz2":
        return bz2.compress(data)
    if fmt == "tar.xz":
        return lzma.compress(data)
    return zstd.compress(data)


def raw_header(
    name: str,
    size: int = 0,
    typeflag: bytes = b"0",
    *,
    size_field: bytes | None = None,
    prefix: str = "",
    signed_checksum: bool = False,
    gnu: bool = False,
) -> bytes:
    """A hand-made tar header with a correct checksum (unless the caller breaks it)."""
    block = bytearray(512)
    encoded = name.encode()
    block[0 : len(encoded)] = encoded
    block[100:108] = b"0000644\0"
    block[108:116] = b"0000000\0"
    block[116:124] = b"0000000\0"
    block[124:136] = size_field if size_field is not None else b"%011o\0" % size
    block[136:148] = b"%011o\0" % 1_790_000_000
    block[148:156] = b" " * 8
    block[156:157] = typeflag
    block[257:265] = b"ustar  \0" if gnu else b"ustar\x0000"
    encoded_prefix = prefix.encode()
    block[345 : 345 + len(encoded_prefix)] = encoded_prefix
    checksum = sum(byte - 256 if byte > 127 else byte for byte in block) if signed_checksum else sum(block)
    block[148:156] = b"%06o\0 " % checksum
    return bytes(block)


def pad(data: bytes) -> bytes:
    return data + bytes(-len(data) % 512)


def member(name: str, data: bytes, typeflag: bytes = b"0", **header_options) -> bytes:
    return raw_header(name, len(data), typeflag, **header_options) + pad(data)


def pax_records(records: dict[str, str]) -> bytes:
    out = b""
    for key, value in records.items():
        keyword, raw_value = key.encode(), value.encode()
        length = len(keyword) + len(raw_value) + 3
        n = p = 0
        while True:
            n = length + len(str(p))
            if n == p:
                break
            p = n
        out += str(p).encode() + b" " + keyword + b"=" + raw_value + b"\n"
    return out


def pax_header(records: dict[str, str], typeflag: bytes = b"x") -> bytes:
    data = pax_records(records)
    return raw_header("./PaxHeaders/x", len(data), typeflag) + pad(data)


def end_of_archive(body: bytes) -> bytes:
    data = body + bytes(1024)
    return data + bytes(-len(data) % RECORD)


SQL = b"PGDMP" + os.urandom(3000)
MANIFEST = b'{"version": "19.0"}'


def minimal_tar(*extra: bytes) -> bytes:
    return end_of_archive(member("sql.dump", SQL) + member("manifest.json", MANIFEST) + b"".join(extra))


def tar_layout(data: bytes) -> tuple[list[int], int]:
    """Header offsets of all members and the offset of the end-of-archive marker."""
    with tarfile.open(fileobj=io.BytesIO(data), mode="r:") as archive:
        members = archive.getmembers()
    offsets = [info.offset for info in members]
    last = members[-1]
    end = last.offset_data + (-(-last.size // 512) * 512 if last.isreg() else 0)
    return offsets, end


# --------------------------------------------------------------------------
# magic and HTML
# --------------------------------------------------------------------------


class CheckMagicTests(unittest.TestCase):
    def test_accepts_every_format(self):
        for fmt in verify.SUPPORTED_FORMATS:
            with self.subTest(fmt=fmt):
                check_magic(fmt, build_backup(fmt)[:512])

    def test_html_page_is_reported_with_the_odoo_error(self):
        page = odoo_error_page("Database backup error: Access Denied").encode()
        for fmt in verify.SUPPORTED_FORMATS:
            with self.subTest(fmt=fmt), self.assertRaises(ArchiveVerificationError) as ctx:
                check_magic(fmt, page)
            self.assertIn("HTML page", str(ctx.exception))
            self.assertIn("Database backup error: Access Denied", str(ctx.exception))

    def test_raw_pg_dump_instead_of_tar_points_at_web_backup(self):
        # Odoo core answers every non-zip format with a raw pg_dump when web_backup is missing.
        with self.assertRaises(ArchiveVerificationError) as ctx:
            check_magic("tar.gz", build_backup("dump")[:512])
        self.assertIn("web_backup", str(ctx.exception))

    def test_other_archive_format_is_named(self):
        with self.assertRaises(ArchiveVerificationError) as ctx:
            check_magic("tar.zst", build_backup("tar.gz")[:512])
        self.assertIn("'tar.gz'", str(ctx.exception))
        self.assertIn("ODOO_BACKUP_FORMAT", str(ctx.exception))

    def test_empty_and_short_input(self):
        with self.assertRaisesRegex(ArchiveVerificationError, "empty"):
            check_magic("zip", b"")
        with self.assertRaisesRegex(ArchiveVerificationError, "only 100 bytes"):
            check_magic("tar", build_backup("tar")[:100])
        with self.assertRaises(ArchiveVerificationError):
            check_magic("tar.xz", b"\xfd7z")

    def test_tar_header_checksum_is_validated(self):
        block = bytearray(build_backup("tar")[:512])
        block[0] ^= 0x01
        with self.assertRaises(ArchiveVerificationError):
            check_magic("tar", bytes(block))

    def test_unsupported_format(self):
        with self.assertRaises(ValueError):
            check_magic("rar", b"Rar!")
        with self.assertRaises(ValueError):
            StreamVerifier("tar.lz4")


class LooksLikeHtmlTests(unittest.TestCase):
    def test_html_variants(self):
        for head in (
            b"<!DOCTYPE html><html>",
            b"\n  <html>\n<head>",
            b"\xef\xbb\xbf<!doctype html>",
            b"<html lang=en><title>502</title>",
            b"<!-- x -->\n<html>",
        ):
            with self.subTest(head=head):
                self.assertTrue(looks_like_html(head))

    def test_archives_and_other_text(self):
        for fmt in verify.SUPPORTED_FORMATS:
            with self.subTest(fmt=fmt):
                self.assertFalse(looks_like_html(build_backup(fmt)[:512]))
        self.assertFalse(looks_like_html(b'{"jsonrpc": "2.0"}'))
        self.assertFalse(looks_like_html(b"<?xml version='1.0'?><methodResponse>"))
        self.assertFalse(looks_like_html(b""))


class ExtractHtmlErrorTests(unittest.TestCase):
    def test_wrong_master_password_page(self):
        page = odoo_error_page("Database backup error: Access Denied").encode()
        self.assertEqual(extract_html_error(page), "Database backup error: Access Denied")

    def test_unknown_database_page_decodes_entities(self):
        page = odoo_error_page("Database backup error: Database 'nope' is not known").encode()
        self.assertIn("&#39;nope&#39;", page.decode())  # escaped like QWeb t-out
        self.assertEqual(extract_html_error(page), "Database backup error: Database 'nope' is not known")

    def test_disabled_database_manager_lists_the_error_first(self):
        page = odoo_error_page("Database backup error: Access Denied", list_db=False).encode()
        self.assertEqual(
            extract_html_error(page),
            "Database backup error: Access Denied | The database manager has been disabled by the administrator",
        )

    def test_pg_dump_failure_multiline_error(self):
        error = "Database backup error: Command `pg_dump` failed\n   exit status 1\n  <stderr>"
        page = odoo_error_page(error).encode()
        self.assertEqual(
            extract_html_error(page), "Database backup error: Command `pg_dump` failed exit status 1 <stderr>"
        )

    def test_proxy_error_page_falls_back_to_page_text(self):
        page = (
            b"<html>\r\n<head><title>502 Bad Gateway</title></head>\r\n<body>\r\n"
            b"<center><h1>502 Bad Gateway</h1></center>\r\n<hr><center>nginx</center>\r\n</body>\r\n</html>"
        )
        self.assertEqual(extract_html_error(page), "502 Bad Gateway nginx")

    def test_error_marker_without_alert(self):
        page = b"<html><body><p>Oops.</p><p>Database backup error: disk full</p></body></html>"
        self.assertEqual(extract_html_error(page), "Database backup error: disk full")

    def test_plain_text_title_only_and_empty(self):
        self.assertEqual(extract_html_error(b"Internal   Server\nError"), "Internal Server Error")
        self.assertEqual(extract_html_error(b"<html><head><title>Odoo</title></head><body></body></html>"), "Odoo")
        self.assertEqual(extract_html_error(b""), "(no readable text in the response)")

    def test_result_is_limited_to_500_characters(self):
        text = extract_html_error(odoo_error_page("Database backup error: " + "x " * 2000).encode())
        self.assertEqual(len(text), 500)
        self.assertTrue(text.endswith("..."))

    def test_body_cut_inside_the_alert(self):
        page = odoo_error_page("Database backup error: Access Denied").encode()
        cut = page.index(b"Access Denied") + len(b"Access Denied")
        self.assertEqual(extract_html_error(page[:cut]), "Database backup error: Access Denied")


# --------------------------------------------------------------------------
# valid archives
# --------------------------------------------------------------------------


class ValidArchiveTests(unittest.TestCase):
    def test_every_tar_format_and_tar_dialect(self):
        dialects = {"pax": tarfile.PAX_FORMAT, "gnu": tarfile.GNU_FORMAT}
        for fmt in TAR_FORMATS:
            for dialect, tar_format in dialects.items():
                with self.subTest(fmt=fmt, dialect=dialect):
                    data = build_tar(odoo_like_members(), MODES[fmt], format=tar_format)
                    result = verify_bytes(fmt, data)
                    self.assertEqual(result.fmt, fmt)
                    self.assertEqual(result.size, len(data))
                    self.assertEqual(result.members, 8)
                    self.assertTrue(result.has_sql_dump)
                    self.assertTrue(result.has_manifest)
                    self.assertTrue(result.has_filestore)
                    self.assertEqual(result.notes, [])

    def test_ustar_prefix_field(self):
        long_dir = "filestore/" + "d" * 90
        members = [("sql.dump", SQL), ("manifest.json", MANIFEST), (long_dir + "/" + "f" * 60, b"x")]
        data = build_tar(members, "w", format=tarfile.USTAR_FORMAT)
        offsets, _end = tar_layout(data)
        self.assertEqual(data[offsets[2] + 345 : offsets[2] + 355], long_dir.encode()[:10], "ustar prefix expected")
        self.assertTrue(verify_bytes("tar", data).has_filestore)

    def test_database_only_archive(self):
        for fmt in TAR_FORMATS:
            with self.subTest(fmt=fmt):
                result = verify_bytes(fmt, build_backup(fmt, with_filestore=False))
                self.assertEqual(result.members, 2)
                self.assertFalse(result.has_filestore)

    def test_chunk_boundaries_do_not_matter(self):
        for fmt in ("tar", "tar.gz", "tar.zst"):
            data = build_backup(fmt)
            expected = verify_bytes(fmt, data)
            for chunk_size in (1, 7, 511, 512, 513, 4096, 8 * 1024 * 1024):
                with self.subTest(fmt=fmt, chunk_size=chunk_size):
                    self.assertEqual(verify_bytes(fmt, data, chunk_size), expected)

    def test_concatenated_members_and_frames(self):
        tar = build_tar(odoo_like_members())
        middle = len(tar) // 2
        for fmt in COMPRESSED_FORMATS:
            with self.subTest(fmt=fmt):
                data = compress(fmt, tar[:middle]) + compress(fmt, tar[middle:])
                self.assertEqual(verify_bytes(fmt, data, 1000).members, 8)

    def test_pax_global_header_and_long_path(self):
        data = build_tar(odoo_like_members(), "w", format=tarfile.PAX_FORMAT, pax_headers={"comment": "odoo"})
        self.assertEqual(data[156:157], b"g")
        self.assertEqual(verify_bytes("tar", data).members, 8)

    def test_pax_size_overrides_the_header_size(self):
        payload = os.urandom(3000)
        data = minimal_tar(pax_header({"size": str(len(payload))}) + raw_header("filestore/big", 0) + pad(payload))
        self.assertEqual(verify_bytes("tar", data).members, 3)

    def test_pax_path_names_the_member(self):
        data = end_of_archive(
            pax_header({"path": "sql.dump"}) + member("placeholder-name", SQL) + member("manifest.json", MANIFEST)
        )
        self.assertTrue(verify_bytes("tar", data).has_sql_dump)

    def test_gnu_long_name(self):
        name = "filestore/" + "n" * 200
        data = minimal_tar(
            raw_header("././@LongLink", len(name) + 1, b"L", gnu=True)
            + pad(name.encode() + b"\0")
            + member(name[:99], b"data", gnu=True)
        )
        result = verify_bytes("tar", data)
        self.assertEqual(result.members, 3)
        self.assertTrue(result.has_filestore)

    def test_base256_size_field(self):
        payload = os.urandom(1500)
        size_field = b"\x80" + len(payload).to_bytes(11, "big")
        data = minimal_tar(raw_header("filestore/b256", size_field=size_field) + pad(payload))
        self.assertEqual(verify_bytes("tar", data).members, 3)

    def test_signed_checksum_variant(self):
        data = minimal_tar(member("filestore/été", b"accent", signed_checksum=True))
        self.assertEqual(verify_bytes("tar", data).members, 3)

    def test_directories_carry_no_data_even_with_a_size(self):
        # tarfile skips no data for directories (also V7 regular files named "x/"), whatever the size field says.
        for typeflag in (b"\0", b"5"):
            with self.subTest(typeflag=typeflag):
                data = minimal_tar(raw_header("filestore/", 512, typeflag), member("filestore/ab/x", b"x"))
                self.assertEqual(verify_bytes("tar", data).members, 4)

    def test_plain_tar_cut_inside_the_final_record_padding_is_complete(self):
        data = build_backup("tar")
        _offsets, end = tar_layout(data)
        result = verify_bytes("tar", data[: end + 1024 + 700])
        self.assertTrue(result.has_sql_dump)
        self.assertEqual(len(result.notes), 1)
        self.assertIn("10240-byte tar record", result.notes[0])

    def test_zip_stream_then_file(self):
        data = build_backup("zip")
        partial = verify_bytes("zip", data, 4096)
        self.assertEqual((partial.fmt, partial.size), ("zip", len(data)))
        self.assertIsNone(partial.members)
        self.assertIsNone(partial.has_sql_dump)
        with tempfile.TemporaryDirectory() as tmp:
            path = pathlib.Path(tmp, "backup.zip")
            path.write_bytes(data)
            result = verify_zip_file(path)
        self.assertEqual(result.members, 8)
        self.assertTrue(result.has_sql_dump and result.has_manifest and result.has_filestore)

    def test_dump_format_notes_the_limitation(self):
        result = verify_bytes("dump", build_backup("dump"))
        self.assertEqual(result.notes, ["dump format: truncation cannot be detected"])
        self.assertTrue(result.has_sql_dump)
        self.assertFalse(result.has_filestore)
        self.assertIsNone(result.members)

    def test_supported_formats_match_the_configuration(self):
        self.assertEqual(tuple(ALLOWED_FORMATS), verify.SUPPORTED_FORMATS)


# --------------------------------------------------------------------------
# truncated and corrupt archives
# --------------------------------------------------------------------------


class TruncationTests(unittest.TestCase):
    def assert_rejected(self, fmt: str, data: bytes, chunk_size: int | None = None):
        with self.assertRaises(ArchiveVerificationError):
            verify_bytes(fmt, data, chunk_size)

    def test_plain_tar_truncated_anywhere_before_the_end_marker(self):
        data = build_backup("tar")
        offsets, end = tar_layout(data)
        positions = set(range(0, end + 1024, 97)) | set(offsets) | {o + 1 for o in offsets} | {o + 511 for o in offsets}
        positions |= {end, end + 1, end + 511, end + 512, end + 513, end + 1023}  # inside the two zero blocks
        for position in sorted(p for p in positions if p < end + 1024):
            with self.subTest(position=position):
                self.assert_rejected("tar", data[:position])

    def test_error_names_the_truncated_member(self):
        data = build_backup("tar")
        offsets, _end = tar_layout(data)
        with self.assertRaisesRegex(ArchiveVerificationError, r"inside member 'sql\.dump'"):
            verify_bytes("tar", data[: offsets[0] + 512 + 100])
        with self.assertRaisesRegex(ArchiveVerificationError, "end-of-archive marker missing"):
            verify_bytes("tar", data[: offsets[2]])  # exactly at a member boundary

    def test_compressed_formats_truncated_anywhere(self):
        for fmt in COMPRESSED_FORMATS:
            data = build_backup(fmt)
            positions = set(range(1, len(data), max(1, len(data) // 150))) | set(range(len(data) - 12, len(data)))
            for position in sorted(positions):
                with self.subTest(fmt=fmt, position=position):
                    self.assert_rejected(fmt, data[:position], 4096)

    def test_gzip_trailer_truncation(self):
        data = build_backup("tar.gz")
        for missing in range(1, 9):  # CRC32 + ISIZE
            with self.subTest(missing=missing), self.assertRaisesRegex(ArchiveVerificationError, "truncated"):
                verify_bytes("tar.gz", data[:-missing])

    def test_gzip_cut_cleanly_at_a_member_boundary(self):
        # A deflate stream flushed exactly at a tar member boundary decompresses cleanly but has no end.
        tar = build_backup("tar")
        offsets, _end = tar_layout(tar)
        compressor = zlib.compressobj(9, zlib.DEFLATED, -15)
        body = compressor.compress(tar[: offsets[3]]) + compressor.flush(zlib.Z_SYNC_FLUSH)
        with self.assertRaisesRegex(ArchiveVerificationError, "truncated"):
            verify_bytes("tar.gz", b"\x1f\x8b\x08\x00\x00\x00\x00\x00\x02\xff" + body)

    def test_zip_truncated(self):
        data = build_backup("zip")
        with tempfile.TemporaryDirectory() as tmp:
            path = pathlib.Path(tmp, "backup.zip")
            for position in (100, len(data) // 2, len(data) - 30, len(data) - 1):
                with self.subTest(position=position):
                    path.write_bytes(data[:position])
                    with self.assertRaises(ArchiveVerificationError):
                        verify_zip_file(path)

    def test_empty_stream(self):
        for fmt in verify.SUPPORTED_FORMATS:
            with self.subTest(fmt=fmt), self.assertRaisesRegex(ArchiveVerificationError, "empty"):
                StreamVerifier(fmt).finish()


class CorruptionTests(unittest.TestCase):
    def test_flipped_byte_in_tar_headers(self):
        data = build_backup("tar")
        offsets, _end = tar_layout(data)
        for offset in offsets:
            for field in (0, 100, 124, 130, 148, 156, 257, 300):
                with self.subTest(offset=offset, field=field):
                    corrupt = bytearray(data)
                    corrupt[offset + field] ^= 0x04
                    with self.assertRaises(ArchiveVerificationError):
                        verify_bytes("tar", bytes(corrupt))

    def test_flipped_byte_in_compressed_data(self):
        for fmt in COMPRESSED_FORMATS:
            options = {"options": {zstd.CompressionParameter.checksum_flag: 1}} if fmt == "tar.zst" else {}
            data = build_tar(odoo_like_members(), MODES[fmt], **options)
            for position in (len(data) // 3, len(data) // 2, len(data) - 5):
                with self.subTest(fmt=fmt, position=position):
                    corrupt = bytearray(data)
                    corrupt[position] ^= 0x10
                    with self.assertRaises(ArchiveVerificationError):
                        verify_bytes(fmt, bytes(corrupt))

    def test_missing_or_bad_required_members(self):
        cases = {
            "no sql dump": end_of_archive(member("manifest.json", MANIFEST)),
            "no manifest": end_of_archive(member("sql.dump", SQL)),
            "empty sql dump": end_of_archive(member("sql.dump", b"") + member("manifest.json", MANIFEST)),
            "two sql dumps": minimal_tar(member("sql.dump", SQL)),
            "sql dump is a directory": end_of_archive(
                raw_header("sql.dump", 0, b"5") + member("manifest.json", MANIFEST)
            ),
        }
        for label, data in cases.items():
            for fmt in ("tar", "tar.gz"):
                with self.subTest(case=label, fmt=fmt), self.assertRaises(ArchiveVerificationError):
                    verify_bytes(fmt, compress(fmt, data))

    def test_data_after_the_compressed_stream(self):
        data = build_backup("tar.gz")
        for trailer in (b"garbage!", bytes(16), b"\x1f"):
            with self.subTest(trailer=trailer), self.assertRaises(ArchiveVerificationError):
                verify_bytes("tar.gz", data + trailer)
        with self.assertRaisesRegex(ArchiveVerificationError, "after the end of the tar.zst stream"):
            verify_bytes("tar.zst", build_backup("tar.zst") + b"not a zstd frame")

    def test_non_zero_data_after_the_end_of_archive(self):
        data = bytearray(build_backup("tar"))
        data[-5] = 0x41
        with self.assertRaisesRegex(ArchiveVerificationError, "after the tar end-of-archive marker"):
            verify_bytes("tar", bytes(data))

    def test_single_zero_block_inside_the_archive(self):
        data = end_of_archive(member("sql.dump", SQL) + bytes(512) + member("manifest.json", MANIFEST))
        with self.assertRaisesRegex(ArchiveVerificationError, "single zero block"):
            verify_bytes("tar", data)

    def test_end_of_archive_right_after_an_extended_header(self):
        data = end_of_archive(member("sql.dump", SQL) + member("manifest.json", MANIFEST) + pax_header({"path": "x"}))
        with self.assertRaisesRegex(ArchiveVerificationError, "extended header"):
            verify_bytes("tar", data)

    def test_invalid_numbers(self):
        cases = {
            "negative base-256 size": raw_header("filestore/neg", size_field=b"\xff" * 12),
            "non-octal size": raw_header("filestore/bad", size_field=b"00000000009\0"),
        }
        for label, header in cases.items():
            with self.subTest(case=label), self.assertRaises(ArchiveVerificationError):
                verify_bytes("tar", minimal_tar(header))
        with self.assertRaisesRegex(ArchiveVerificationError, "PAX size"):
            verify_bytes("tar", minimal_tar(pax_header({"size": "12x"}) + raw_header("filestore/p", 0)))

    def test_invalid_pax_records(self):
        bad_records = [b"99 path=x\n", b"3 p\n", b"10 pathxx\n\n", b"9 path=x\x00"]
        for records in bad_records:
            header = raw_header("./PaxHeaders/x", len(records), b"x") + pad(records)
            with self.subTest(records=records), self.assertRaisesRegex(ArchiveVerificationError, "PAX"):
                verify_bytes("tar", minimal_tar(header + raw_header("filestore/p", 0)))

    def test_oversized_extended_header_is_rejected_early(self):
        verifier = StreamVerifier("tar")
        verifier.feed(member("sql.dump", SQL))
        with self.assertRaisesRegex(ArchiveVerificationError, "exceeds"):
            verifier.feed(raw_header("./PaxHeaders/x", 2 * 1024 * 1024, b"x"))

    def test_gnu_sparse_is_rejected(self):
        with self.assertRaisesRegex(ArchiveVerificationError, "sparse"):
            verify_bytes("tar", minimal_tar(raw_header("filestore/sparse", 0, b"S", gnu=True)))
        with self.assertRaisesRegex(ArchiveVerificationError, "sparse"):
            verify_bytes("tar", minimal_tar(pax_header({"GNU.sparse.major": "1"}) + raw_header("f", 0)))

    def test_decompressed_data_that_is_not_tar(self):
        with self.assertRaisesRegex(ArchiveVerificationError, "not a tar archive"):
            verify_bytes("tar.gz", gzip.compress(b"PGDMP" + bytes(5000)))


class VerifierBehaviourTests(unittest.TestCase):
    def test_html_is_rejected_on_feed_not_only_at_finish(self):
        page = odoo_error_page("Database backup error: Access Denied").encode()
        self.assertGreater(len(page), 1024)
        verifier = StreamVerifier("tar.gz")
        with self.assertRaisesRegex(ArchiveVerificationError, "HTML page: Database backup error: Access Denied$"):
            verifier.feed(page + b"x" * 70_000)  # longer than the HTML buffer: raised on feed

    def test_html_error_text_survives_small_chunks(self):
        page = odoo_error_page("Database backup error: Access Denied").encode()
        with self.assertRaisesRegex(ArchiveVerificationError, "Database backup error: Access Denied"):
            verify_bytes("tar", page, 100)  # shorter than the HTML buffer: raised at finish

    def test_corruption_is_reported_as_early_as_possible(self):
        data = bytearray(build_backup("tar"))
        offsets, _end = tar_layout(bytes(data))
        data[offsets[1] + 10] ^= 0x01  # manifest header
        verifier = StreamVerifier("tar")
        verifier.feed(bytes(data[: offsets[1]]))
        with self.assertRaisesRegex(ArchiveVerificationError, "checksum"):
            verifier.feed(bytes(data[offsets[1] : offsets[1] + 512]))

    def test_errors_are_sticky_and_feed_after_finish_fails(self):
        verifier = StreamVerifier("tar")
        with self.assertRaises(ArchiveVerificationError) as first:
            verifier.feed(b"x" * 600)
        with self.assertRaises(ArchiveVerificationError) as second:
            verifier.feed(build_backup("tar"))
        self.assertIs(first.exception, second.exception)
        with self.assertRaises(ArchiveVerificationError):
            verifier.finish()
        done = StreamVerifier("tar")
        done.feed(build_backup("tar"))
        done.finish()
        with self.assertRaises(RuntimeError):
            done.feed(b"more")

    def test_memory_stays_bounded_for_highly_compressible_members(self):
        # 64 MiB of zeros compress to ~64 KiB: output must be produced in bounded slices.
        size = 64 * 1024 * 1024
        compressor = zlib.compressobj(1, zlib.DEFLATED, 31)
        parts = [compressor.compress(member("sql.dump", SQL) + member("manifest.json", MANIFEST))]
        info = tarfile.TarInfo("filestore/zeros")
        info.size = size
        parts.append(compressor.compress(info.tobuf()))
        zeros = bytes(1024 * 1024)
        for _ in range(size // len(zeros)):
            parts.append(compressor.compress(zeros))
        parts.append(compressor.compress(bytes(1024)))  # end-of-archive marker
        parts.append(compressor.flush())
        data = b"".join(parts)
        self.assertLess(len(data), 2 * 1024 * 1024)
        tracemalloc.start()
        try:
            result = verify_bytes("tar.gz", data)
            _current, peak = tracemalloc.get_traced_memory()
        finally:
            tracemalloc.stop()
        self.assertEqual(result.members, 3)
        self.assertLess(peak, 24 * 1024 * 1024, f"peak {peak} bytes")


class VerifyZipFileTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.path = pathlib.Path(self.tmp.name, "backup.zip")

    def check(self, members):
        self.path.write_bytes(build_zip(members))
        return verify_zip_file(self.path)

    def test_database_only_zip(self):
        result = verify_zip_file(self._write(build_backup("zip", with_filestore=False)))
        self.assertEqual(result.members, 2)
        self.assertFalse(result.has_filestore)

    def _write(self, data: bytes) -> pathlib.Path:
        self.path.write_bytes(data)
        return self.path

    def test_missing_or_bad_members(self):
        cases = {
            "no dump.sql": [("manifest.json", MANIFEST)],
            "empty dump.sql": [("dump.sql", b""), ("manifest.json", MANIFEST)],
            "no manifest": [("dump.sql", b"-- sql")],
            "manifest not json": [("dump.sql", b"-- sql"), ("manifest.json", b"{broken")],
            "manifest not an object": [("dump.sql", b"-- sql"), ("manifest.json", b"[1]")],
        }
        for label, members in cases.items():
            with self.subTest(case=label), self.assertRaises(ArchiveVerificationError):
                self.check(members)

    def test_corrupt_manifest_crc(self):
        data = bytearray(build_zip([("dump.sql", b"-- sql"), ("manifest.json", b'{"a": 1}')]))
        index = data.index(b"manifest.json") + len(b"manifest.json")  # stored data of the local entry follows
        data[index + 2] ^= 0x01
        with self.assertRaises(ArchiveVerificationError):
            verify_zip_file(self._write(bytes(data)))

    def test_not_a_zip(self):
        with self.assertRaisesRegex(ArchiveVerificationError, "invalid zip backup"):
            verify_zip_file(self._write(build_backup("tar")))
        with self.assertRaises(ArchiveVerificationError):
            verify_zip_file(pathlib.Path(self.tmp.name, "missing.zip"))


if __name__ == "__main__":
    unittest.main()
