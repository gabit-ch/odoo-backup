"""Streaming verification of Odoo backup archives.

Odoo's ``/web/database/backup`` answers with a close-delimited HTTP/1.0 body
(no ``Content-Length``): when the Odoo worker dies half-way, the download simply
ends and looks complete.  Every backup is therefore verified while it streams:

* tar formats (``tar``, ``tar.gz``, ``tar.bz2``, ``tar.xz``, ``tar.zst``): the
  compressed layer is decompressed incrementally and must end with a complete
  end-of-stream marker (gzip CRC32/ISIZE, bz2/xz checksums, zstd frame end);
  the tar layer is walked header by header (checksums, PAX and GNU extended
  headers, member sizes) and must end with the end-of-archive marker.  The
  archive must contain the pg_dump (``sql.dump``) and ``manifest.json``.
* ``zip``: only the leading magic is checked while streaming; the central
  directory (which sits at the very end of the file) is verified afterwards
  with :func:`verify_zip_file`.
* ``dump`` (raw ``pg_dump -Fc`` stream): only the magic can be checked; a
  truncated dump cannot be detected.

Limits of the verification: member *data* of a plain tar archive carries no
checksum, and tar.zst archives written by :mod:`tarfile` (web_backup) carry no
zstd content checksum, so a flipped byte inside member data is only detected
when it breaks the compressed structure.  Truncation is always detected for
the tar formats.

Memory use is bounded: the tar walker only buffers one header block or one
extended header (at most :data:`MAX_EXTENDED_HEADER` bytes), and
decompression output is produced in slices of at most
:data:`DECOMPRESS_OUTPUT_LIMIT` bytes.
"""

import bz2
import json
import lzma
import os
import re
import zipfile
import zlib
from collections.abc import Callable
from compression import zstd
from dataclasses import dataclass, field
from html.parser import HTMLParser

__all__ = [
    "ArchiveVerificationError",
    "StreamVerifier",
    "SUPPORTED_FORMATS",
    "VerificationResult",
    "check_magic",
    "extract_html_error",
    "looks_like_html",
    "verify_zip_file",
]

# Must stay equal to odoo_backup.config.ALLOWED_FORMATS (a test asserts it);
# kept here so that this module has no dependency on the configuration.
SUPPORTED_FORMATS = ("zip", "dump", "tar", "tar.gz", "tar.bz2", "tar.xz", "tar.zst")

BLOCK_SIZE = 512
RECORD_SIZE = 20 * BLOCK_SIZE  # tarfile.RECORDSIZE: tarfile pads archives to this size
HEAD_SIZE = BLOCK_SIZE  # leading bytes collected before the magic check
HTML_HEAD_LIMIT = 64 * 1024  # an HTML error page is collected up to this size for its message
MAX_EXTENDED_HEADER = 1024 * 1024  # PAX header / GNU long name payload limit
MAX_PENDING_EXTENDED_HEADERS = 16
DECOMPRESS_INPUT_SLICE = 1024 * 1024
DECOMPRESS_OUTPUT_LIMIT = 4 * 1024 * 1024
HTML_SCAN_LIMIT = 256 * 1024
MAX_ERROR_TEXT = 500

SQL_DUMP_NAMES = frozenset({"sql.dump", "dump.sql"})
MANIFEST_NAME = "manifest.json"
FILESTORE_NAME = "filestore"

_MAGIC = {
    "tar.gz": b"\x1f\x8b\x08",
    "tar.bz2": b"BZh",
    "tar.xz": b"\xfd7zXZ\x00",
    "tar.zst": b"\x28\xb5\x2f\xfd",
    "zip": b"PK\x03\x04",
    "dump": b"PGDMP",
}
_FORMAT_DESCRIPTIONS = {
    "tar.gz": "a gzip stream",
    "tar.bz2": "a bzip2 stream",
    "tar.xz": "an xz stream",
    "tar.zst": "a zstd frame",
    "zip": "a zip archive",
    "dump": "a pg_dump custom-format dump",
    "tar": "an uncompressed tar archive",
}

_ZERO_BLOCK = bytes(BLOCK_SIZE)
_TAR_MAGIC = b"ustar"
_REGULAR_TYPES = frozenset({b"0", b"\0", b"7"})
_DIRECTORY_TYPE = b"5"
# Types tarfile knows; members of any other type are treated as regular files
# (their data is skipped), exactly like tarfile does.
_SUPPORTED_TYPES = _REGULAR_TYPES | {b"1", b"2", b"3", b"4", b"5", b"6", b"L", b"K", b"S"}
_PAX_LOCAL_TYPES = frozenset({b"x", b"X"})  # 'X' = Solaris variant, handled like 'x'
_PAX_GLOBAL_TYPE = b"g"
_GNU_LONGNAME_TYPE = b"L"
_GNU_LONGLINK_TYPE = b"K"
_GNU_SPARSE_TYPE = b"S"
_EXTENDED_TYPES = _PAX_LOCAL_TYPES | {_PAX_GLOBAL_TYPE, _GNU_LONGNAME_TYPE, _GNU_LONGLINK_TYPE}
_PAX_LENGTH_RE = re.compile(rb"([0-9]{1,20}) ")

_DECOMPRESSORS: dict[str, Callable[[], object]] = {
    "tar.gz": lambda: zlib.decompressobj(wbits=31),  # 31 = gzip container, CRC32 + ISIZE checked
    "tar.bz2": bz2.BZ2Decompressor,
    "tar.xz": lambda: lzma.LZMADecompressor(format=lzma.FORMAT_XZ),
    "tar.zst": zstd.ZstdDecompressor,
}
_DECOMPRESSION_ERRORS = (zlib.error, OSError, EOFError, ValueError, lzma.LZMAError, zstd.ZstdError)


class ArchiveVerificationError(Exception):
    """The data is not a complete, well-formed backup archive of the expected format."""


@dataclass
class VerificationResult:
    """Outcome of a successful verification.

    ``None`` means "not checked / not applicable" (e.g. members of a zip before
    :func:`verify_zip_file` ran, or the manifest of a raw ``dump``).  ``notes``
    are limitations the caller should log as warnings.
    """

    fmt: str
    size: int
    members: int | None = None
    has_sql_dump: bool | None = None
    has_manifest: bool | None = None
    has_filestore: bool | None = None
    notes: list[str] = field(default_factory=list)


# --------------------------------------------------------------------------
# Magic and HTML helpers
# --------------------------------------------------------------------------

def looks_like_html(head: bytes) -> bool:
    """Return True if the leading bytes of a response are an HTML page.

    No supported archive format starts with ``<``, so this cannot misfire on a
    real backup.
    """
    text = bytes(head[:1024]).lstrip(b" \t\r\n\x0c").removeprefix(b"\xef\xbb\xbf").lstrip().lower()
    if not text.startswith(b"<"):
        return False
    return any(tag in text for tag in (b"<!doctype html", b"<html", b"<head", b"<body", b"<title", b"<div"))


def check_magic(fmt: str, head: bytes) -> None:
    """Raise :class:`ArchiveVerificationError` unless ``head`` starts like a ``fmt`` backup.

    ``head`` should hold at least the first 512 bytes (a whole tar header for
    the ``tar`` format).  The message names what was received instead (an HTML
    page, a different archive format) because that usually points at the
    configuration problem; for an HTML page, pass as much of it as available
    (Odoo's error text follows a longer ``<head>``).
    """
    _require_supported(fmt)
    page = bytes(head)
    head = page[:HEAD_SIZE]
    if not head:
        raise ArchiveVerificationError(f"the {fmt} backup is empty (0 bytes received)")
    if fmt == "tar":
        if len(head) >= BLOCK_SIZE and _is_tar_header(head[:BLOCK_SIZE]):
            return
    elif head.startswith(_MAGIC[fmt]):
        return
    if looks_like_html(head):
        raise ArchiveVerificationError(
            f"expected a {fmt} backup but received an HTML page: {extract_html_error(page)}"
        )
    raise ArchiveVerificationError(_describe_bad_magic(fmt, head))


def _require_supported(fmt: str) -> None:
    if fmt not in SUPPORTED_FORMATS:
        raise ValueError(f"unsupported backup format {fmt!r} (supported: {', '.join(SUPPORTED_FORMATS)})")


def _is_tar_header(block: bytes) -> bool:
    if block[257:262] != _TAR_MAGIC:
        return False
    try:
        return _checksum_matches(block)
    except ValueError:
        return False


def _guess_format(head: bytes) -> str | None:
    if len(head) >= BLOCK_SIZE and _is_tar_header(head[:BLOCK_SIZE]):
        return "tar"
    for fmt, magic in _MAGIC.items():
        if head.startswith(magic):
            return fmt
    return None


def _describe_bad_magic(fmt: str, head: bytes) -> str:
    if fmt == "tar" and len(head) < BLOCK_SIZE and not _guess_format(head):
        return f"not a tar backup: only {len(head)} bytes received, a tar header needs {BLOCK_SIZE}"
    message = (
        f"not a {fmt} backup: expected {_FORMAT_DESCRIPTIONS[fmt]}, "
        f"but the data starts with {head[:16].hex(' ')}"
    )
    actual = _guess_format(head)
    if actual == "dump" and fmt != "dump":
        message += (
            f"; this is a raw pg_dump custom-format dump: Odoo ignored backup_format {fmt!r} "
            "(is the web_backup module loaded as a server-wide module?)"
        )
    elif actual is not None:
        message += f"; this looks like {_FORMAT_DESCRIPTIONS[actual]} ({actual!r}), check ODOO_BACKUP_FORMAT"
    return message


class _HTMLTextExtractor(HTMLParser):
    """Collects the visible text of a page and the text of ``alert-danger`` blocks."""

    _INVISIBLE = frozenset({"script", "style", "head", "title", "noscript", "template"})
    _BREAKS = frozenset({"br", "p", "li", "tr", "h1", "h2", "h3", "h4", "h5", "h6", "hr"})

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.text: list[str] = []
        self.title: list[str] = []
        self.alerts: list[str] = []
        self._open_alerts: list[tuple[int, list[str]]] = []
        self._div_depth = 0
        self._invisible_depth = 0
        self._in_title = False

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag in self._INVISIBLE:
            self._invisible_depth += 1
            self._in_title = self._in_title or tag == "title"
        elif tag == "div":
            self._div_depth += 1
            classes = (dict(attrs).get("class") or "").split()
            if "alert-danger" in classes:
                self._open_alerts.append((self._div_depth, []))
        elif tag in self._BREAKS:
            self._add(" ")

    def handle_endtag(self, tag: str) -> None:
        if tag in self._INVISIBLE:
            self._invisible_depth = max(0, self._invisible_depth - 1)
            if tag == "title":
                self._in_title = False
        elif tag == "div":
            if self._open_alerts and self._open_alerts[-1][0] == self._div_depth:
                self.alerts.append("".join(self._open_alerts.pop()[1]))
            self._div_depth = max(0, self._div_depth - 1)

    def handle_data(self, data: str) -> None:
        if self._in_title:
            self.title.append(data)
        if not self._invisible_depth:
            self._add(data)

    def _add(self, data: str) -> None:
        self.text.append(data)
        for _depth, parts in self._open_alerts:
            parts.append(data)

    def close(self) -> None:
        super().close()
        while self._open_alerts:  # body cut off inside an alert
            self.alerts.append("".join(self._open_alerts.pop()[1]))


def _collapse(text: str) -> str:
    return " ".join(text.split())


def _shorten(text: str, limit: int = MAX_ERROR_TEXT) -> str:
    return text if len(text) <= limit else text[: limit - 3] + "..."


def extract_html_error(body: bytes) -> str:
    """Return the human readable error of an Odoo error page (or any HTML/text body).

    Odoo's database manager renders controller errors into
    ``<div class="alert alert-danger">`` (e.g. ``Database backup error: Access
    Denied``).  All such alerts are returned, error messages first; without
    alerts, the text from ``Database backup error:`` on or else a snippet of
    the visible page text is returned.  Whitespace is collapsed and the result
    is limited to 500 characters.
    """
    source = bytes(body[:HTML_SCAN_LIMIT]).decode("utf-8", "replace")
    parser = _HTMLTextExtractor()
    parser.feed(source)
    parser.close()

    alerts = [text for text in (_collapse(alert) for alert in parser.alerts) if text]
    if alerts:
        alerts.sort(key=lambda text: "error:" not in text.lower())  # stable: errors first
        return _shorten(" | ".join(alerts))
    text = _collapse("".join(parser.text))
    marker = text.find("Database backup error:")
    if marker >= 0:
        return _shorten(text[marker:])
    if text:
        return _shorten(text)
    title = _collapse("".join(parser.title))
    return _shorten(title) if title else "(no readable text in the response)"


# --------------------------------------------------------------------------
# tar header parsing
# --------------------------------------------------------------------------

def _parse_number(field_bytes: bytes) -> int:
    """Decode a tar numeric field (octal text or GNU base-256), like tarfile.nti()."""
    if field_bytes[0] in (0x80, 0xFF):
        value = int.from_bytes(field_bytes[1:], "big")
        if field_bytes[0] == 0xFF:
            value -= 256 ** (len(field_bytes) - 1)
        return value
    text = field_bytes.split(b"\0", 1)[0].strip()
    if not text:
        return 0
    if not text.isdigit() or b"8" in text or b"9" in text:
        raise ValueError(f"invalid octal number {text!r}")
    return int(text, 8)


def _checksum_matches(block: bytes) -> bool:
    """Compare the stored header checksum with the unsigned and the signed sum.

    The checksum is the sum of all header bytes with the checksum field itself
    counted as eight spaces (8 * 32 = 256).  Some historic tar implementations
    summed signed chars, so that variant is accepted as well (like tarfile).
    """
    stored = _parse_number(block[148:156])
    unsigned = 256 + sum(block) - sum(block[148:156])
    if stored == unsigned:
        return True
    high = sum(1 for byte in block[:148] if byte & 0x80) + sum(1 for byte in block[156:] if byte & 0x80)
    return high > 0 and stored == unsigned - 256 * high


def _cstring(data: bytes) -> str:
    return data.split(b"\0", 1)[0].decode("utf-8", "surrogateescape")


def _round_up(size: int) -> int:
    return -(-size // BLOCK_SIZE) * BLOCK_SIZE


def _parse_pax_records(data: bytes, offset: int) -> dict[str, str]:
    """Parse PAX records (``"%d %s=%s\\n"``) with the framing rules of tarfile."""
    records: dict[str, str] = {}
    pos = 0
    while pos < len(data) and data[pos] != 0:
        match = _PAX_LENGTH_RE.match(data, pos)
        if not match:
            raise ArchiveVerificationError(f"invalid PAX extended header at tar offset {offset}")
        length = int(match.group(1))
        end = match.start(1) + length - 1  # index of the record's trailing newline
        if length < 5 or pos + length > len(data) or data[end] != 0x0A:
            raise ArchiveVerificationError(f"invalid PAX record framing at tar offset {offset}")
        keyword, equals, value = data[match.end(1) + 1:end].partition(b"=")
        if not keyword or equals != b"=":
            raise ArchiveVerificationError(f"invalid PAX record at tar offset {offset}")
        records[keyword.decode("utf-8", "surrogateescape")] = value.decode("utf-8", "surrogateescape")
        pos += length
    if any(key.startswith("GNU.sparse.") for key in records):
        raise ArchiveVerificationError(f"GNU sparse members are not supported (tar offset {offset})")
    return records


class _TarWalker:
    """Incremental tar parser that validates structure without keeping member data.

    It mirrors the reading rules of :mod:`tarfile` (which is what restores the
    backups) closely enough that an archive it accepts can be read by tarfile
    completely: header checksums, PAX ``x``/``g`` headers (``path`` and
    ``size``), GNU ``L``/``K`` long names, octal and base-256 numbers, which
    member types carry data, and the end-of-archive marker of two zero blocks.
    A single zero block followed by more data is rejected because tarfile stops
    reading at the first zero block and would silently drop the rest.
    """

    def __init__(self) -> None:
        self.offset = 0  # tar stream bytes consumed by previous feed() calls
        self.members = 0
        self.has_filestore = False
        self.sql_dumps = 0
        self.sql_dump_size = 0
        self.manifests = 0
        self.eoa_offset: int | None = None  # stream offset right after the two zero blocks
        self._header = bytearray()  # partial header block
        self._skip = 0  # member data + padding still to skip
        self._current = ""  # member whose data is being skipped (for messages)
        self._collect_left = 0  # extended header payload bytes still to collect
        self._collect_padding = 0
        self._collect_type = b""
        self._collect_offset = 0
        self._collected = bytearray()
        self._pending: list[tuple[str, object]] = []  # extended headers for the next member
        self._awaiting_member = False  # an extended header was read, a member must follow
        self._global_pax: dict[str, str] = {}
        self._zero_blocks = 0
        self._seen_header = False

    def feed(self, data: bytes | bytearray | memoryview) -> None:
        view = memoryview(data).cast("B")
        end = len(view)
        pos = 0
        while pos < end:
            if self._skip:
                step = min(self._skip, end - pos)
                self._skip -= step
                pos += step
                continue
            if self._collect_left:
                step = min(self._collect_left, end - pos)
                if self._collect_type != _GNU_LONGLINK_TYPE:  # link names are not needed
                    self._collected += view[pos:pos + step]
                self._collect_left -= step
                pos += step
                if not self._collect_left:
                    self._skip = self._collect_padding
                    self._finish_extended_header()
                continue
            if self.eoa_offset is not None:
                self._check_trailing_zeros(view[pos:end], self.offset + pos)
                pos = end
                continue
            if not self._header and end - pos >= BLOCK_SIZE:
                block = view[pos:pos + BLOCK_SIZE].tobytes()
                pos += BLOCK_SIZE
            else:
                step = min(BLOCK_SIZE - len(self._header), end - pos)
                self._header += view[pos:pos + step]
                pos += step
                if len(self._header) < BLOCK_SIZE:
                    continue
                block = bytes(self._header)
                self._header.clear()
            self._process_header(block, self.offset + pos - BLOCK_SIZE)
        self.offset += end

    def finish(self) -> None:
        """Raise unless the stream ended cleanly after the end-of-archive marker."""
        if self._header:
            raise ArchiveVerificationError(
                f"tar archive truncated inside a header block at tar offset {self.offset - len(self._header)}"
            )
        if self._collect_left or (self._skip and self._awaiting_member):
            raise ArchiveVerificationError("tar archive truncated inside an extended header")
        if self._skip:
            raise ArchiveVerificationError(
                f"tar archive truncated inside member {self._current!r} ({self._skip} bytes missing)"
            )
        if self._awaiting_member:
            raise ArchiveVerificationError("tar archive truncated after an extended header")
        if self.eoa_offset is None:
            if self._zero_blocks:
                raise ArchiveVerificationError("tar archive truncated inside the end-of-archive marker")
            raise ArchiveVerificationError(
                f"tar archive truncated: end-of-archive marker missing after {self.members} members "
                f"({self.offset} bytes)"
            )

    def _check_trailing_zeros(self, tail: memoryview, offset: int) -> None:
        data = tail.tobytes()
        stripped = data.lstrip(b"\0")
        if stripped:
            position = offset + len(data) - len(stripped)
            raise ArchiveVerificationError(
                f"unexpected non-zero data after the tar end-of-archive marker at tar offset {position}"
            )

    def _process_header(self, block: bytes, offset: int) -> None:
        if block == _ZERO_BLOCK:
            self._process_zero_block(offset)
            return
        if self._zero_blocks:
            raise ArchiveVerificationError(
                f"single zero block at tar offset {offset - BLOCK_SIZE} followed by more data: tar "
                "readers stop at the zero block and would miss the rest of the archive"
            )
        if not self._seen_header:
            if not _is_tar_header(block):
                raise ArchiveVerificationError(
                    f"the data is not a tar archive: no valid ustar header at the start "
                    f"(starts with {block[:16].hex(' ')})"
                )
            self._seen_header = True
        try:
            valid = _checksum_matches(block)
            typeflag = block[156:157]
            size = _parse_number(block[124:136])
        except ValueError as exc:
            raise ArchiveVerificationError(f"invalid tar header at tar offset {offset}: {exc}") from None
        if not valid:
            raise ArchiveVerificationError(
                f"tar header checksum mismatch at tar offset {offset} (archive corrupt or truncated)"
            )
        if size < 0:
            raise ArchiveVerificationError(f"negative member size in the tar header at tar offset {offset}")
        if typeflag in _EXTENDED_TYPES:
            self._start_extended_header(typeflag, size, offset)
        elif typeflag == _GNU_SPARSE_TYPE:
            raise ArchiveVerificationError(f"GNU sparse members are not supported (tar offset {offset})")
        else:
            self._process_member(block, typeflag, size, offset)

    def _process_zero_block(self, offset: int) -> None:
        if not self._seen_header:
            raise ArchiveVerificationError("the tar archive starts with an end-of-archive block (no members)")
        if self._awaiting_member:
            raise ArchiveVerificationError(
                f"end-of-archive marker directly after an extended header at tar offset {offset}"
            )
        self._zero_blocks += 1
        if self._zero_blocks == 2:
            self.eoa_offset = offset + BLOCK_SIZE

    def _start_extended_header(self, typeflag: bytes, size: int, offset: int) -> None:
        if typeflag != _GNU_LONGLINK_TYPE and size > MAX_EXTENDED_HEADER:
            raise ArchiveVerificationError(
                f"extended tar header of {size} bytes at tar offset {offset} exceeds {MAX_EXTENDED_HEADER} bytes"
            )
        if len(self._pending) >= MAX_PENDING_EXTENDED_HEADERS:
            raise ArchiveVerificationError(f"too many consecutive extended tar headers at tar offset {offset}")
        self._awaiting_member = True
        self._collect_type = typeflag
        self._collect_offset = offset
        self._collect_left = size
        self._collect_padding = _round_up(size) - size
        self._collected.clear()
        if size == 0:
            self._finish_extended_header()

    def _finish_extended_header(self) -> None:
        data = bytes(self._collected)
        self._collected.clear()
        if self._collect_type in _PAX_LOCAL_TYPES:
            self._pending.append(("pax", _parse_pax_records(data, self._collect_offset)))
        elif self._collect_type == _PAX_GLOBAL_TYPE:
            self._global_pax.update(_parse_pax_records(data, self._collect_offset))
        elif self._collect_type == _GNU_LONGNAME_TYPE:
            self._pending.append(("longname", _cstring(data)))

    def _process_member(self, block: bytes, typeflag: bytes, size: int, offset: int) -> None:
        name = _cstring(block[0:100])
        prefix = _cstring(block[345:500])
        # tarfile turns a V7 regular file with a trailing slash into a directory,
        # but not for headers following an extended header (dircheck=False).
        if not self._awaiting_member and typeflag == b"\0" and name.endswith("/"):
            typeflag = _DIRECTORY_TYPE
        is_directory = typeflag == _DIRECTORY_TYPE
        if is_directory:
            name = name.rstrip("/")
        if prefix:
            name = f"{prefix}/{name}"
        if "path" in self._global_pax:
            name = self._global_pax["path"].rstrip("/")
        # Apply extended headers innermost first so that the outermost wins, as in tarfile.
        for kind, value in reversed(self._pending):
            if kind == "longname":
                name = value.removesuffix("/") if is_directory else value
                continue
            pax = self._global_pax | value
            if "path" in pax:
                name = pax["path"].rstrip("/")
            if "size" in pax:
                try:
                    size = int(pax["size"])
                except ValueError:
                    raise ArchiveVerificationError(f"invalid PAX size at tar offset {offset}") from None
                if size < 0:
                    raise ArchiveVerificationError(f"negative PAX size at tar offset {offset}")
        self._pending.clear()
        self._awaiting_member = False

        self.members += 1
        is_regular = typeflag in _REGULAR_TYPES
        if is_regular or typeflag not in _SUPPORTED_TYPES:
            self._skip = _round_up(size)
            self._current = name
        if is_regular and name in SQL_DUMP_NAMES:
            self.sql_dumps += 1
            self.sql_dump_size = size
        elif is_regular and name == MANIFEST_NAME:
            self.manifests += 1
        if name == FILESTORE_NAME or name.startswith(FILESTORE_NAME + "/"):
            self.has_filestore = True


# --------------------------------------------------------------------------
# Compressed layer
# --------------------------------------------------------------------------

class _Decompressor:
    """Stream decompressor that accepts concatenated members/frames.

    When one member ends (``eof``) and input is left (``unused_data``), a new
    decompressor continues with it, so ``cat a.gz b.gz`` style streams are
    accepted; bytes after the last member that do not form a valid member
    raise.  :meth:`finish` requires the last member to be complete, which is
    what detects a truncated download (and, via the format's checksums, most
    corruption).
    """

    def __init__(self, fmt: str, sink: Callable[[bytes], None]) -> None:
        self._fmt = fmt
        self._sink = sink
        self._factory = _DECOMPRESSORS[fmt]
        self._is_zlib = fmt == "tar.gz"
        self._decompressor = None
        self._completed = 0
        self._member_output = 0

    def feed(self, data: bytes | bytearray | memoryview) -> None:
        view = memoryview(data).cast("B")
        # Slicing the input keeps zlib's unconsumed_tail copies small.
        for start in range(0, len(view), DECOMPRESS_INPUT_SLICE):
            self._decompress(view[start:start + DECOMPRESS_INPUT_SLICE])

    def finish(self) -> None:
        if self._decompressor is not None:
            raise ArchiveVerificationError(
                f"the {self._fmt} stream is truncated: the compressed data ends before its end-of-stream "
                "marker (download cut off?)"
            )
        if not self._completed:
            raise ArchiveVerificationError(f"the {self._fmt} stream contains no compressed data")

    def _decompress(self, piece: bytes | memoryview) -> None:
        while True:
            if self._decompressor is None:
                if not piece:
                    return
                self._decompressor = self._factory()
                self._member_output = 0
            decompressor = self._decompressor
            try:
                output = decompressor.decompress(piece, DECOMPRESS_OUTPUT_LIMIT)
            except _DECOMPRESSION_ERRORS as exc:
                raise self._corruption_error(exc) from None
            if output:
                self._member_output += len(output)
                self._sink(output)
            if decompressor.eof:
                self._completed += 1
                self._decompressor = None
                piece = decompressor.unused_data  # start of the next member, if any
                continue
            if self._is_zlib:
                piece = decompressor.unconsumed_tail
                # A full output slice may leave output pending inside zlib: call again.
                if not piece and len(output) < DECOMPRESS_OUTPUT_LIMIT:
                    return
            else:
                if decompressor.needs_input:
                    return
                piece = b""

    def _corruption_error(self, exc: Exception) -> ArchiveVerificationError:
        if self._completed and not self._member_output:
            return ArchiveVerificationError(
                f"unexpected data after the end of the {self._fmt} stream ({type(exc).__name__}: {exc})"
            )
        return ArchiveVerificationError(f"the {self._fmt} stream is corrupt: {type(exc).__name__}: {exc}")


# --------------------------------------------------------------------------
# Public verifier
# --------------------------------------------------------------------------

class StreamVerifier:
    """Verify a backup while it is downloaded; see the module docstring.

    Call :meth:`feed` with every chunk in order and :meth:`finish` at the end.
    Both raise :class:`ArchiveVerificationError`; after an error the verifier
    keeps raising the same error.
    """

    def __init__(self, fmt: str) -> None:
        _require_supported(fmt)
        self.fmt = fmt
        self.size = 0
        self._head = bytearray()
        self._head_checked = False
        self._head_is_html = False
        self._finished = False
        self._error: ArchiveVerificationError | None = None
        self._walker = _TarWalker() if fmt.startswith("tar") else None
        self._decompressor = _Decompressor(fmt, self._walker.feed) if fmt in _DECOMPRESSORS else None

    def feed(self, data: bytes | bytearray | memoryview) -> None:
        if self._error is not None:
            raise self._error
        if self._finished:
            raise RuntimeError("StreamVerifier.feed() called after finish()")
        view = memoryview(data).cast("B")
        if not view:
            return
        self.size += len(view)
        try:
            if not self._head_checked:
                view = self._collect_head(view)
                if view is None:
                    return
            self._process(view)
        except ArchiveVerificationError as exc:
            self._error = exc
            raise

    def finish(self) -> VerificationResult:
        if self._error is not None:
            raise self._error
        self._finished = True
        try:
            if not self._head_checked:
                self._check_head()
            return self._result()
        except ArchiveVerificationError as exc:
            self._error = exc
            raise

    def _collect_head(self, view: memoryview) -> memoryview | None:
        """Buffer the leading bytes for :func:`check_magic`; return the rest, or None while buffering.

        An HTML page is buffered up to HTML_HEAD_LIMIT bytes (or its end) so
        that the error carries Odoo's message, which follows the page head.
        """
        while True:
            limit = HTML_HEAD_LIMIT if self._head_is_html else HEAD_SIZE
            missing = limit - len(self._head)
            self._head += view[:missing]
            view = view[missing:]
            if len(self._head) < limit:
                return None
            if not self._head_is_html and looks_like_html(self._head):
                self._head_is_html = True
                continue
            self._check_head()  # raises for an HTML page
            return view

    def _check_head(self) -> None:
        head = bytes(self._head)
        check_magic(self.fmt, head)
        self._head_checked = True
        self._head = bytearray()
        self._process(head)

    def _process(self, data: bytes | memoryview) -> None:
        if self._decompressor is not None:
            self._decompressor.feed(data)
        elif self._walker is not None:
            self._walker.feed(data)
        # zip and dump: the magic check is all that can be done while streaming

    def _result(self) -> VerificationResult:
        if self.fmt == "zip":
            # The central directory is at the end: verify_zip_file() runs after the download.
            return VerificationResult(fmt="zip", size=self.size)
        if self.fmt == "dump":
            return VerificationResult(
                fmt="dump",
                size=self.size,
                has_sql_dump=True,
                has_filestore=False,
                notes=["dump format: truncation cannot be detected"],
            )
        if self._decompressor is not None:
            self._decompressor.finish()
        walker = self._walker
        walker.finish()
        if not walker.sql_dumps:
            raise ArchiveVerificationError("the backup archive contains no sql.dump (or dump.sql)")
        if walker.sql_dumps > 1:
            raise ArchiveVerificationError("the backup archive contains more than one sql.dump")
        if not walker.sql_dump_size:
            raise ArchiveVerificationError("the sql.dump in the backup archive is empty")
        if not walker.manifests:
            raise ArchiveVerificationError("the backup archive contains no manifest.json")
        notes = []
        if walker.offset % RECORD_SIZE:
            notes.append(
                f"tar stream length {walker.offset} is not a multiple of the {RECORD_SIZE}-byte tar record "
                "(the archive content is complete; the download may have been cut inside the final padding)"
            )
        return VerificationResult(
            fmt=self.fmt,
            size=self.size,
            members=walker.members,
            has_sql_dump=True,
            has_manifest=True,
            has_filestore=walker.has_filestore,
            notes=notes,
        )


def verify_zip_file(path: str | os.PathLike) -> VerificationResult:
    """Verify a downloaded Odoo zip backup (central directory, dump.sql, manifest.json).

    A truncated zip lacks the end-of-central-directory record and fails to
    open.  ``manifest.json`` is read (its CRC is checked) and must be a JSON
    object; ``dump.sql`` must be a non-empty file.  The CRCs of the other
    members are not checked (that would mean re-reading the whole archive).
    """
    try:
        size = os.path.getsize(path)
        with zipfile.ZipFile(path) as archive:
            infos = archive.infolist()
            by_name = {info.filename: info for info in infos}
            sql_dump = next((by_name[name] for name in ("dump.sql", "sql.dump") if name in by_name), None)
            if sql_dump is None or sql_dump.is_dir():
                raise ArchiveVerificationError("the zip backup contains no dump.sql")
            if not sql_dump.file_size:
                raise ArchiveVerificationError(f"the {sql_dump.filename} in the zip backup is empty")
            manifest = by_name.get(MANIFEST_NAME)
            if manifest is None or manifest.is_dir():
                raise ArchiveVerificationError("the zip backup contains no manifest.json")
            if manifest.file_size > MAX_EXTENDED_HEADER:
                raise ArchiveVerificationError("the manifest.json in the zip backup is implausibly large")
            if not isinstance(json.loads(archive.read(manifest)), dict):
                raise ArchiveVerificationError("the manifest.json in the zip backup is not a JSON object")
            end_of_data = max(info.header_offset + info.compress_size for info in infos)
            if end_of_data > size:
                raise ArchiveVerificationError("the zip backup's central directory points beyond the end of the file")
    except ArchiveVerificationError:
        raise
    except (zipfile.BadZipFile, zlib.error, EOFError, UnicodeDecodeError, ValueError, OSError) as exc:
        raise ArchiveVerificationError(f"invalid zip backup: {type(exc).__name__}: {exc}") from None
    return VerificationResult(
        fmt="zip",
        size=size,
        members=len(infos),
        has_sql_dump=True,
        has_manifest=True,
        has_filestore=any(
            info.filename.rstrip("/") == FILESTORE_NAME or info.filename.startswith(FILESTORE_NAME + "/")
            for info in infos
        ),
    )
