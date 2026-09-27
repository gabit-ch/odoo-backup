#!/usr/bin/env python3
"""End-to-end test of the odoo-backup image against Odoo 19, PostgreSQL 18 and an SFTP server.

The driver generates every credential and SSH host key it uses, starts
``tests/e2e/docker-compose.yml``, creates an Odoo database (``-i base``, no demo data) with a
binary attachment in the filestore and runs the backup image with ``docker run`` on the compose
network through these scenarios:

a. ``--check``: exit 0, only ``OK`` lines, the pinned host key fingerprint is the generated one.
b. ``--once``: exactly one ``odoo19.0-e2e-<ts>.zip`` in SFTP_PATH with ``dump.sql``,
   ``manifest.json`` and the attachment under ``filestore/``.
c. ``--once --database-only``: exactly one zip without ``filestore/`` in the db-only directory.
d. Wrong ODOO_MASTER_PWD: ``--check`` exits 1 with a ``FAIL master-password`` line, ``--once``
   fails with Odoo's "Access Denied", the SFTP server is unchanged.
e. SFTP_HOST_KEY of another key: ``--check`` exits 1 with ``FAIL sftp`` and ``--once`` exits 1
   in the SFTP preflight, both refusing the server's (generated) host key; nothing is uploaded.
f. Retention on a seeded directory (50 backups over the last 100 days, a foreign file, another
   database's backups and partial uploads): ``--retention-plan``, then ``RETENTION_DRY_RUN=true``
   (nothing deleted), then the real run leaves exactly the expected files.
g. The zip of (b) restored through ``/web/database/restore`` is listed and its attachment is
   byte-identical.
h. ``--health`` exits 0 after the successful runs.

Usage::

    docker build -t odoo-backup:e2e .
    python3 tests/e2e/run_e2e.py --image odoo-backup:e2e

Needs Docker with the compose plugin, ``ssh-keygen`` and Python 3.12 or newer (standard library
only). Exits 0 when every scenario passed, else 1. Every command (logged before it starts) and
its output (secrets redacted) is written to ``<work dir>/logs``; on a failure the compose logs
are printed as well. ``--deadline`` bounds the whole run (default 25 minutes): a hang fails the
run with the logs collected, including the output of a backup container that is still running.
``--prepare-only`` generates the work directory (keys, configuration, secrets) without Docker.
"""

import argparse
import base64
import contextlib
import dataclasses
import datetime
import email.parser
import email.policy
import hashlib
import http.client
import json
import os
import pathlib
import random
import re
import secrets
import shutil
import signal
import subprocess
import sys
import tempfile
import time
import traceback
import uuid
import xmlrpc.client
import zipfile
from collections.abc import Iterable, Mapping, Sequence

HERE = pathlib.Path(__file__).resolve().parent
COMPOSE_FILE = HERE / "docker-compose.yml"
NETWORK = "odoo-backup-e2e"  # networks.default.name in docker-compose.yml
STATE_VOLUME = "odoo-backup-e2e-state"  # BACKUP_TMP_DIR and BACKUP_STATE_DIR of the backup container
DB_NAME = "e2e"
RESTORED_DB = "e2e_restored"
ODOO_SERIE = "19.0"
SFTP_USER = "e2e"
SFTP_ROOT = "/upload"  # sftp-data as seen inside the SFTP user's chroot
BACKUP_DIR = "backups"
DB_ONLY_DIR = f"{BACKUP_DIR}/db-only"  # the default DB_ONLY_BACKUP_PATH
RETENTION_DIR = "retention"
ATTACHMENT_NAME = "odoo-backup-e2e.bin"
# Incompressible, deterministic content, so the filestore entry in the zip is easy to identify.
ATTACHMENT_BYTES = random.Random(2417).randbytes(96 * 1024)

KEEP_DAILY, KEEP_MONTHLY, KEEP_YEARLY = 5, 2, 1  # the retention policy of scenario (f)
FOREIGN_FILE = "notes-20250101-010000.txt"
OTHER_DB = "otherdb"

COMMAND_TIMEOUT = 600  # seconds for one docker command
CLEANUP_TIMEOUT = 120  # seconds for one command that collects logs or removes containers
RPC_TIMEOUT = 300  # socket timeout of one HTTP, JSON-RPC or XML-RPC request to Odoo
ODOO_START_TIMEOUT = 240
DEFAULT_DEADLINE = 25 * 60  # seconds for the whole run (CI: step timeout 30 min, job 35 min)
# Every backup container carries this label, so one that outlives its `docker run` (a hang, the
# deadline) can be found, logged and removed.
BACKUP_LABEL = "odoo-backup-e2e.backup"

# The backup file name contract (README "Retention"), written independently of odoo_backup so
# the expectations below do not reuse the code under test.
BACKUP_NAME_RE = re.compile(
    r"odoo(?P<serie>[^-/]+)-(?P<db>[^/]+)-(?P<ts>[0-9]{8}-[0-9]{6})"
    r"\.(?P<fmt>zip|dump|tar|tar\.gz|tar\.bz2|tar\.xz|tar\.zst)(?P<partial>\.upload)?",
    re.ASCII,
)
TS_FORMAT = "%Y%m%d-%H%M%S"
CHECK_LINE_RE = re.compile(r"(?P<status>OK|FAIL) (?P<component>[a-z-]+): (?P<detail>.*)")


class E2EFailure(Exception):
    """A scenario did not behave as expected."""


class DeadlineExceeded(Exception):
    """The run took longer than ``--deadline``.

    Deliberately not an E2EFailure: the wait loops that retry on E2EFailure must not swallow it.
    """


def expect(condition: object, message: str) -> None:
    if not condition:
        raise E2EFailure(message)


@contextlib.contextmanager
def deadline(seconds: float):
    """Raise DeadlineExceeded in the main thread once ``seconds`` have passed (0: no deadline).

    SIGALRM interrupts whatever blocks: a socket read, ``time.sleep`` or ``subprocess.run`` (which
    kills its child before the exception propagates). The previous handler is restored on exit.
    """
    if seconds <= 0:
        yield
        return

    def expired(_signum, _frame) -> None:
        raise DeadlineExceeded(f"the end-to-end run exceeded its deadline of {seconds:g} s")

    previous = signal.signal(signal.SIGALRM, expired)
    signal.setitimer(signal.ITIMER_REAL, seconds)
    try:
        yield
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)
        signal.signal(signal.SIGALRM, previous)


# ------------------------------------------------------------------------------------------
# Pure helpers (unit tested in test_run_e2e.py)
# ------------------------------------------------------------------------------------------


def ssh_fingerprint(public_key_line: str) -> str:
    """OpenSSH SHA256 fingerprint (``ssh-keygen -l -E sha256``) of a ``<type> <base64>`` line."""
    blob = base64.b64decode(public_key_line.split()[1], validate=True)
    return "SHA256:" + base64.b64encode(hashlib.sha256(blob).digest()).decode().rstrip("=")


@dataclasses.dataclass(frozen=True)
class BackupName:
    name: str
    db: str
    ts: datetime.datetime
    partial: bool


def parse_backup_name(name: str) -> BackupName | None:
    match = BACKUP_NAME_RE.fullmatch(name)
    if match is None:
        return None
    try:
        ts = datetime.datetime.strptime(match["ts"], TS_FORMAT)
    except ValueError:
        return None
    return BackupName(name, match["db"], ts, match["partial"] is not None)


def backup_name(db: str, ts: datetime.datetime, *, serie: str = ODOO_SERIE, fmt: str = "zip") -> str:
    return f"odoo{serie}-{db}-{ts.strftime(TS_FORMAT)}.{fmt}"


@dataclasses.dataclass(frozen=True)
class RetentionSeed:
    """Files placed in the retention directory before scenario (f)."""

    own: tuple[str, ...]  # backups of DB_NAME
    stale_partial: str  # an interrupted upload of DB_NAME: removed by a real run
    untouchable: tuple[str, ...]  # never deleted: foreign file, other database, its partial

    @property
    def all(self) -> tuple[str, ...]:
        return (*self.own, self.stale_partial, *self.untouchable)


def retention_seed(now: datetime.datetime) -> RetentionSeed:
    """50 backups of DB_NAME, one every other day over the last 100 days (01:00 local time).

    Every seventh one is an Odoo 17 tar.gz backup: all series and formats of a database form
    one timeline.
    """
    day = now.replace(hour=1, minute=0, second=0, microsecond=0)
    own = []
    for index, days_back in enumerate(range(1, 100, 2)):
        ts = day - datetime.timedelta(days=days_back)
        own.append(backup_name(DB_NAME, ts, serie="17.0", fmt="tar.gz") if index % 7 == 6 else backup_name(DB_NAME, ts))
    untouchable = (
        FOREIGN_FILE,
        backup_name(OTHER_DB, day - datetime.timedelta(days=2)),
        backup_name(OTHER_DB, datetime.datetime(2024, 1, 1, 1)),
        backup_name(OTHER_DB, day - datetime.timedelta(days=4)) + ".upload",
    )
    return RetentionSeed(tuple(own), backup_name(DB_NAME, day - datetime.timedelta(days=5)) + ".upload", untouchable)


def expected_retention(
    names: Iterable[str],
    db: str,
    *,
    keep_daily: int,
    keep_monthly: int,
    keep_yearly: int,
    protect: Iterable[str] = (),
) -> tuple[set[str], set[str]]:
    """(kept, deleted) backups of ``db`` for BACKUP_TIME=00:00 in daily mode (no hourly rule).

    With the anchor at midnight the daily representative is the earliest backup of the day,
    the monthly one the earliest backup of the month and the yearly one the earliest of the
    year; the newest and the protected backups are always kept (README "Retention"). Files
    that are not complete backups of ``db`` are neither kept nor deleted here. No backup may be
    dated in the future.
    """
    backups = sorted(
        (parsed.ts, parsed.name)
        for parsed in map(parse_backup_name, set(names))
        if parsed is not None and parsed.db == db and not parsed.partial
    )
    if not backups:
        return set(), set()
    # Backups with the same timestamp (other series or format): the name sorting last wins.
    earliest: dict[object, tuple[datetime.datetime, str]] = {}
    for ts, name in backups:
        for bucket in (ts.date(), (ts.year, ts.month), ts.year):
            current = earliest.get(bucket)
            if current is None or (current[0] == ts and name > current[1]):
                earliest[bucket] = (ts, name)

    def newest(buckets: Iterable[object], count: int) -> list[object]:
        ordered = sorted(set(buckets))  # dates, (year, month) tuples or years: chronological
        if count == -1:
            return ordered
        return ordered[-count:] if count > 0 else []

    days = [ts.date() for ts, _ in backups]
    months = [(ts.year, ts.month) for ts, _ in backups]
    years = [ts.year for ts, _ in backups]
    kept = {backups[-1][1]} | (set(protect) & {name for _, name in backups})
    for buckets, count in ((days, keep_daily), (months, keep_monthly), (years, keep_yearly)):
        kept |= {earliest[bucket][1] for bucket in newest(buckets, count)}
    return kept, {name for _, name in backups} - kept


def multipart_form(fields: Mapping[str, str], files: Mapping[str, tuple[str, bytes]]) -> tuple[str, bytes]:
    """Encode a multipart/form-data body; returns (content type, body)."""
    boundary = uuid.uuid4().hex
    parts: list[bytes] = []
    for name, value in fields.items():
        parts.append(f'--{boundary}\r\nContent-Disposition: form-data; name="{name}"\r\n\r\n{value}\r\n'.encode())
    for name, (filename, data) in files.items():
        head = (
            f'--{boundary}\r\nContent-Disposition: form-data; name="{name}"; filename="{filename}"\r\n'
            "Content-Type: application/octet-stream\r\n\r\n"
        )
        parts.append(head.encode() + data + b"\r\n")
    parts.append(f"--{boundary}--\r\n".encode())
    return f"multipart/form-data; boundary={boundary}", b"".join(parts)


def parse_multipart(content_type: str, body: bytes) -> dict[str, bytes]:
    """Inverse of multipart_form() for the tests: field name -> raw value."""
    message = email.parser.BytesParser(policy=email.policy.HTTP).parsebytes(
        f"Content-Type: {content_type}\r\n\r\n".encode() + body
    )
    return {
        part.get_param("name", header="content-disposition"): part.get_payload(decode=True)
        for part in message.iter_parts()
    }


def parse_check_lines(stdout: str) -> dict[str, tuple[bool, str]]:
    """``--check`` output as {component: (ok, detail)}; raises E2EFailure on any other line."""
    results: dict[str, tuple[bool, str]] = {}
    for line in stdout.splitlines():
        if not line.strip():
            continue
        match = CHECK_LINE_RE.fullmatch(line)
        expect(match is not None, f"--check printed a line that is neither OK nor FAIL: {line!r}")
        assert match is not None
        results[match["component"]] = (match["status"] == "OK", match["detail"])
    return results


def expect_host_key_refusal(text: str, presented: str, what: str) -> None:
    """``text`` says the service refused an SFTP server that presented the Ed25519 key ``presented``
    because it matches no SFTP_HOST_KEY entry (not an authentication error, a timeout or the like).
    """
    expect(
        f"the server presented ssh-ed25519 {presented}, which matches none of the" in text
        and "refusing to authenticate" in text,
        f"{what} did not refuse the SFTP server for its unpinned host key {presented}:\n{text}",
    )


@dataclasses.dataclass(frozen=True)
class ZipSummary:
    names: tuple[str, ...]
    manifest: dict

    @property
    def filestore(self) -> tuple[str, ...]:
        return tuple(name for name in self.names if name.startswith("filestore/"))


def summarize_zip(path: pathlib.Path) -> ZipSummary:
    """Check the archive CRCs and read the member list and manifest.json of an Odoo zip backup."""
    with zipfile.ZipFile(path) as archive:
        broken = archive.testzip()
        expect(broken is None, f"{path.name}: member {broken} fails its CRC check")
        names = tuple(archive.namelist())
        manifest = json.loads(archive.read("manifest.json")) if "manifest.json" in names else {}
    return ZipSummary(names, manifest)


def redact(text: str, secret_values: Iterable[str]) -> str:
    for value in secret_values:
        if value:
            text = text.replace(value, "***")
    return text


def snapshot(root: pathlib.Path) -> dict[str, int]:
    """Relative path -> size of every file below ``root``."""
    return {str(path.relative_to(root)): path.stat().st_size for path in sorted(root.rglob("*")) if path.is_file()}


# ------------------------------------------------------------------------------------------
# Work directory: secrets, keys and configuration files
# ------------------------------------------------------------------------------------------


@dataclasses.dataclass(frozen=True)
class Secrets:
    master_password: str
    wrong_master_password: str
    pg_password: str
    sftp_password: str
    admin_password: str

    def values(self) -> tuple[str, ...]:
        return dataclasses.astuple(self)


def generate_secrets() -> Secrets:
    """Fresh random credentials for one run (URL-safe, so they fit users.conf and odoo.conf)."""
    return Secrets(*(secrets.token_urlsafe(24) for _ in dataclasses.fields(Secrets)))


@dataclasses.dataclass(frozen=True)
class WorkDir:
    root: pathlib.Path
    host_key_fingerprint: str  # ed25519 key of the SFTP server (the one it offers first)
    foreign_fingerprint: str  # a key the SFTP server does not have

    @property
    def sftp_data(self) -> pathlib.Path:
        return self.root / "sftp-data"

    @property
    def logs(self) -> pathlib.Path:
        return self.root / "logs"


def ssh_keygen(path: pathlib.Path, key_type: str) -> str:
    """Create an unencrypted key pair (mode 0600) and return the fingerprint, cross-checked with ssh-keygen."""
    args = ["-t", key_type] + (["-b", "3072"] if key_type == "rsa" else [])
    subprocess.run(
        ["ssh-keygen", "-q", *args, "-N", "", "-C", "odoo-backup-e2e", "-f", str(path)],
        check=True,
        capture_output=True,
        timeout=60,
    )
    computed = ssh_fingerprint(path.with_suffix(".pub").read_text())
    listed = subprocess.run(
        ["ssh-keygen", "-l", "-E", "sha256", "-f", str(path.with_suffix(".pub"))],
        check=True,
        capture_output=True,
        text=True,
        timeout=60,
    ).stdout.split()[1]
    expect(computed == listed, f"fingerprint mismatch for {path.name}: computed {computed}, ssh-keygen {listed}")
    return computed


def prepare_work_dir(root: pathlib.Path, secret: Secrets) -> WorkDir:
    """Write odoo.conf, the SFTP user and host keys, and the empty SFTP data directory."""
    root.mkdir(parents=True, exist_ok=True)
    (root / "logs").mkdir(exist_ok=True)
    (root / "odoo.conf").write_text(
        "[options]\n"
        "; Generated by tests/e2e/run_e2e.py for one end-to-end run.\n"
        "addons_path = /mnt/extra-addons\n"
        "data_dir = /var/lib/odoo\n"
        f"admin_passwd = {secret.master_password}\n"
        "list_db = True\n"
    )
    (root / "odoo.conf").chmod(0o644)  # read by the odoo user (UID 101) of the Odoo image
    sftp = root / "sftp"
    sftp.mkdir(exist_ok=True)
    for name in ("ssh_host_ed25519_key", "ssh_host_rsa_key", "foreign_ed25519_key"):
        for path in (sftp / name, sftp / f"{name}.pub"):
            path.unlink(missing_ok=True)
    host_fingerprint = ssh_keygen(sftp / "ssh_host_ed25519_key", "ed25519")
    ssh_keygen(sftp / "ssh_host_rsa_key", "rsa")
    foreign_fingerprint = ssh_keygen(sftp / "foreign_ed25519_key", "ed25519")
    # <user>:<password>:<uid>:<gid> (atmoz/sftp): the uploads belong to the account running us.
    (sftp / "users.conf").write_text(f"{SFTP_USER}:{secret.sftp_password}:{os.getuid()}:{os.getgid()}\n")
    data = root / "sftp-data"
    if data.exists():
        shutil.rmtree(data)
    data.mkdir(mode=0o755)
    return WorkDir(root, host_fingerprint, foreign_fingerprint)


# ------------------------------------------------------------------------------------------
# Docker, Odoo and backup runs
# ------------------------------------------------------------------------------------------


@dataclasses.dataclass(frozen=True)
class Result:
    code: int
    stdout: str
    stderr: str

    @property
    def output(self) -> str:
        return self.stdout + self.stderr


class Shell:
    """Runs commands with a fixed extra environment and logs them (secrets redacted)."""

    def __init__(self, log_dir: pathlib.Path, env: Mapping[str, str], secret_values: Sequence[str]) -> None:
        self.log = log_dir / "commands.log"
        self.env = {**os.environ, **env}
        self.secret_values = secret_values
        self.count = 0

    def run(
        self, args: Sequence[str], *, env: Mapping[str, str] | None = None, timeout: float = COMMAND_TIMEOUT
    ) -> Result:
        self.count += 1
        number = self.count
        started = time.monotonic()
        # Written before the command starts, so a command that hangs is in the log as well.
        self._write(f"\n### [{number}] {self.redact(' '.join(args))}\n")
        try:
            completed = subprocess.run(
                list(args),
                env={**self.env, **(env or {})},
                capture_output=True,
                text=True,
                timeout=timeout,
                check=False,
            )
            result = Result(completed.returncode, completed.stdout, completed.stderr)
        except subprocess.TimeoutExpired as exc:
            result = Result(-1, _text(exc.stdout), _text(exc.stderr) + f"\nTIMEOUT after {timeout} s")
        except BaseException as exc:  # the deadline, Ctrl-C: subprocess.run has killed the command
            self._write(
                f"### [{number}] interrupted after {time.monotonic() - started:.1f} s: "
                f"{type(exc).__name__}: {self.redact(str(exc))}\n"
            )
            raise
        self._write(
            f"### [{number}] exit {result.code} after {time.monotonic() - started:.1f} s\n"
            f"--- stdout\n{self.redact(result.stdout)}--- stderr\n{self.redact(result.stderr)}"
        )
        return result

    def _write(self, text: str) -> None:
        with self.log.open("a") as fh:
            fh.write(text)

    def must(self, args: Sequence[str], **kwargs) -> Result:
        result = self.run(args, **kwargs)
        expect(
            result.code == 0, f"{' '.join(args[:4])} ... failed with exit {result.code}:\n{self.redact(result.output)}"
        )
        return result

    def redact(self, text: str) -> str:
        return redact(text, self.secret_values)


def _text(value: str | bytes | None) -> str:
    if value is None:
        return ""
    return value.decode(errors="replace") if isinstance(value, bytes) else value


class Compose:
    def __init__(self, shell: Shell) -> None:
        self.shell = shell

    def __call__(self, *args: str, timeout: float = COMMAND_TIMEOUT) -> Result:
        return self.shell.must(["docker", "compose", "--file", str(COMPOSE_FILE), *args], timeout=timeout)

    def logs(self) -> str:
        result = self.shell.run(
            ["docker", "compose", "--file", str(COMPOSE_FILE), "logs", "--no-color", "--timestamps"],
            timeout=CLEANUP_TIMEOUT,
        )
        return self.shell.redact(result.output)

    def down(self) -> None:
        self.shell.run(
            ["docker", "compose", "--file", str(COMPOSE_FILE), "down", "--volumes", "--remove-orphans"],
            timeout=CLEANUP_TIMEOUT,
        )


def remove_backup_containers(shell: Shell, log_dir: pathlib.Path) -> int:
    """Save the output of every backup container still present (it outlived its `docker run`)
    to ``log_dir/backup-container-<id>.log`` and remove it; returns how many were found."""
    listed = shell.run(
        ["docker", "ps", "--all", "--quiet", "--no-trunc", "--filter", f"label={BACKUP_LABEL}"],
        timeout=CLEANUP_TIMEOUT,
    )
    containers = listed.stdout.split() if listed.code == 0 else []
    for container in containers:
        output = shell.run(["docker", "logs", "--timestamps", container], timeout=CLEANUP_TIMEOUT)
        (log_dir / f"backup-container-{container[:12]}.log").write_text(shell.redact(output.output))
        shell.run(["docker", "rm", "--force", container], timeout=CLEANUP_TIMEOUT)
    return len(containers)


class TimeoutTransport(xmlrpc.client.Transport):
    """XML-RPC over HTTP with a socket timeout (xmlrpc.client's own transport waits forever)."""

    def __init__(self, timeout: float) -> None:
        super().__init__()
        self.timeout = timeout

    def make_connection(self, host):
        connection = super().make_connection(host)
        connection.timeout = self.timeout  # used when the connection opens its socket
        return connection


class OdooAPI:
    """The Odoo endpoints the driver needs, on 127.0.0.1:<port> (no redirects are followed).

    Every request has a socket timeout, so an Odoo that accepts a connection but never answers
    fails the run instead of blocking it.
    """

    def __init__(self, port: int, *, timeout: float = RPC_TIMEOUT) -> None:
        self.port = port
        self.base = f"http://127.0.0.1:{port}"
        self.timeout = timeout

    def _proxy(self, endpoint: str) -> xmlrpc.client.ServerProxy:
        return xmlrpc.client.ServerProxy(
            f"{self.base}/xmlrpc/2/{endpoint}", transport=TimeoutTransport(self.timeout), allow_none=True
        )

    def request(self, method: str, path: str, body: bytes = b"", headers: Mapping[str, str] | None = None):
        connection = http.client.HTTPConnection("127.0.0.1", self.port, timeout=self.timeout)
        try:
            connection.request(method, path, body=body, headers=dict(headers or {}))
            response = connection.getresponse()
            return response.status, dict(response.getheaders()), response.read()
        finally:
            connection.close()

    def jsonrpc(self, path: str, params: Mapping[str, object] | None = None) -> object:
        payload = json.dumps({"jsonrpc": "2.0", "method": "call", "params": dict(params or {}), "id": 1}).encode()
        status, _headers, body = self.request("POST", path, payload, {"Content-Type": "application/json"})
        expect(status == 200, f"{path} answered HTTP {status}")
        answer = json.loads(body)
        expect("error" not in answer, f"{path} answered an error: {answer.get('error')}")
        return answer["result"]

    def wait_until_ready(self, timeout: float) -> str:
        deadline = time.monotonic() + timeout
        last_error = "no answer"
        while time.monotonic() < deadline:
            try:
                info = self.jsonrpc("/web/webclient/version_info")
                if isinstance(info, dict) and info.get("server_serie"):
                    return str(info["server_serie"])
            except (OSError, http.client.HTTPException, ValueError, E2EFailure) as exc:
                last_error = f"{type(exc).__name__}: {exc}"
            time.sleep(2)
        raise E2EFailure(f"Odoo did not answer /web/webclient/version_info within {timeout} s ({last_error})")

    def execute(self, db: str, uid: int, password: str, model: str, method: str, *args, **kwargs):
        with self._proxy("object") as proxy:
            return proxy.execute_kw(db, uid, password, model, method, list(args), kwargs)

    def authenticate(self, db: str, login: str, password: str) -> int:
        with self._proxy("common") as proxy:
            uid = proxy.authenticate(db, login, password, {})
        expect(isinstance(uid, int) and uid > 0, f"login {login!r} on database {db!r} failed")
        return uid

    def restore(self, master_password: str, name: str, backup: pathlib.Path) -> tuple[int, str, bytes]:
        content_type, body = multipart_form(
            {"master_pwd": master_password, "name": name}, {"backup_file": (backup.name, backup.read_bytes())}
        )
        status, headers, answer = self.request("POST", "/web/database/restore", body, {"Content-Type": content_type})
        return status, headers.get("Location", ""), answer


class BackupImage:
    """``docker run`` of the backup image on the compose network, one container per call."""

    def __init__(self, shell: Shell, image: str, base_env: Mapping[str, str]) -> None:
        self.shell = shell
        self.image = image
        self.base_env = dict(base_env)

    def run(self, *args: str, **overrides: str) -> Result:
        env = {**self.base_env, **overrides}
        command = ["docker", "run", "--rm", "--network", NETWORK, "--volume", f"{STATE_VOLUME}:/var/lib/odoo-backup"]
        command += ["--cap-drop", "ALL", "--security-opt", "no-new-privileges", "--label", BACKUP_LABEL]
        for key in sorted(env):
            command += ["--env", key]  # the value comes from the environment, never from the command line
        return self.shell.run([*command, self.image, "python", "backup.py", *args], env=env)


# ------------------------------------------------------------------------------------------
# Scenarios
# ------------------------------------------------------------------------------------------


@dataclasses.dataclass
class Context:
    work: WorkDir
    secret: Secrets
    odoo: OdooAPI
    backup: BackupImage
    attachment_id: int = 0
    attachment: dict = dataclasses.field(default_factory=dict)
    full_backup: pathlib.Path | None = None

    def listing(self, directory: str) -> set[str]:
        path = self.work.sftp_data / directory
        return {entry.name for entry in path.iterdir() if entry.is_file()} if path.is_dir() else set()


def require_ok(result: Result, what: str) -> None:
    expect(result.code == 0, f"{what}: exit {result.code}, expected 0\n{result.output}")


def new_backup(before: set[str], after: set[str], directory: str) -> str:
    added = sorted(after - before)
    expect(len(added) == 1, f"expected exactly one new file in {directory}, got {added}")
    parsed = parse_backup_name(added[0])
    expect(
        parsed is not None and parsed.db == DB_NAME and not parsed.partial and added[0].endswith(".zip"),
        f"unexpected file name {added[0]!r} in {directory}",
    )
    assert parsed is not None
    expect(added[0].startswith(f"odoo{ODOO_SERIE}-{DB_NAME}-"), f"{added[0]!r} does not name Odoo {ODOO_SERIE}")
    age = abs(datetime.datetime.now(datetime.UTC).replace(tzinfo=None) - parsed.ts)
    expect(age < datetime.timedelta(minutes=15), f"{added[0]!r} is not dated now (TZ=UTC): {age} off")
    return added[0]


def prepare_odoo(ctx: Context) -> None:
    """Replace the default admin password and store a binary attachment in the filestore."""
    uid = ctx.odoo.authenticate(DB_NAME, "admin", "admin")
    ctx.odoo.execute(DB_NAME, uid, "admin", "res.users", "write", [uid], {"password": ctx.secret.admin_password})
    uid = ctx.odoo.authenticate(DB_NAME, "admin", ctx.secret.admin_password)
    values = {"name": ATTACHMENT_NAME, "datas": base64.b64encode(ATTACHMENT_BYTES).decode(), "type": "binary"}
    ctx.attachment_id = ctx.odoo.execute(DB_NAME, uid, ctx.secret.admin_password, "ir.attachment", "create", values)
    fields = ["store_fname", "checksum"]
    (record,) = ctx.odoo.execute(
        DB_NAME, uid, ctx.secret.admin_password, "ir.attachment", "read", [ctx.attachment_id], fields=fields
    )
    expect(record["store_fname"], "the attachment is not stored in the filestore (store_fname is empty)")
    sha1 = hashlib.sha1(ATTACHMENT_BYTES, usedforsecurity=False).hexdigest()  # Odoo's attachment checksum
    expect(record["checksum"] == sha1, f"unexpected attachment checksum {record}")
    ctx.attachment = record


def scenario_a_check(ctx: Context) -> None:
    result = ctx.backup.run("--check")
    require_ok(result, "--check")
    lines = parse_check_lines(result.stdout)
    expected = {"config", "retention", "odoo", "master-password", "database", "sftp", "target-dir"}
    expect(set(lines) == expected, f"--check components {sorted(lines)}, expected {sorted(expected)}")
    expect(all(ok for ok, _ in lines.values()), f"--check reported a failure: {lines}")
    expect(f"Odoo {ODOO_SERIE}" in lines["odoo"][1], f"unexpected odoo line: {lines['odoo'][1]}")
    sftp_detail = lines["sftp"][1]
    expect(
        f"ssh-ed25519 {ctx.work.host_key_fingerprint} pinned" in sftp_detail,
        f"--check did not verify the generated Ed25519 host key: {sftp_detail}",
    )


def scenario_b_full_backup(ctx: Context) -> None:
    before = ctx.listing(BACKUP_DIR)
    result = ctx.backup.run("--once")
    require_ok(result, "--once")
    after = ctx.listing(BACKUP_DIR)
    name = new_backup(before, after, BACKUP_DIR)
    expect(after == {name}, f"{BACKUP_DIR} should hold only {name}: {sorted(after)}")
    path = ctx.work.sftp_data / BACKUP_DIR / name
    summary = summarize_zip(path)
    expect({"dump.sql", "manifest.json"} <= set(summary.names), f"{name} lacks dump.sql or manifest.json")
    expect(summary.manifest.get("db_name") == DB_NAME, f"manifest.json names {summary.manifest.get('db_name')!r}")
    member = f"filestore/{ctx.attachment['store_fname']}"
    expect(member in summary.filestore, f"{name} lacks the attachment {member}")
    with zipfile.ZipFile(path) as archive:
        expect(archive.read(member) == ATTACHMENT_BYTES, f"{member} in {name} differs from the attachment")
    expect("backup finished:" in result.stderr, "the run did not log 'backup finished'")
    ctx.full_backup = path


def scenario_c_database_only(ctx: Context) -> None:
    full_before = ctx.listing(BACKUP_DIR)
    before = ctx.listing(DB_ONLY_DIR)
    result = ctx.backup.run("--once", "--database-only")
    require_ok(result, "--once --database-only")
    name = new_backup(before, ctx.listing(DB_ONLY_DIR), DB_ONLY_DIR)
    summary = summarize_zip(ctx.work.sftp_data / DB_ONLY_DIR / name)
    expect({"dump.sql", "manifest.json"} <= set(summary.names), f"{name} lacks dump.sql or manifest.json")
    expect(not summary.filestore, f"the database-only backup {name} contains {len(summary.filestore)} filestore files")
    expect(ctx.listing(BACKUP_DIR) == full_before, "the database-only run changed the full backup directory")


def scenario_d_wrong_master_password(ctx: Context) -> None:
    before = snapshot(ctx.work.sftp_data)
    check = ctx.backup.run("--check", ODOO_MASTER_PWD=ctx.secret.wrong_master_password)
    expect(check.code == 1, f"--check with a wrong master password: exit {check.code}, expected 1\n{check.output}")
    lines = parse_check_lines(check.stdout)
    expect(lines.get("master-password", (True, ""))[0] is False, f"no FAIL master-password line: {lines}")
    expect(lines.get("odoo", (False, ""))[0], f"the odoo check should still pass: {lines}")
    once = ctx.backup.run("--once", ODOO_MASTER_PWD=ctx.secret.wrong_master_password)
    expect(once.code != 0, "--once with a wrong master password succeeded")
    expect("Access Denied" in once.output, f"--once did not report Odoo's 'Access Denied':\n{once.output}")
    expect(snapshot(ctx.work.sftp_data) == before, "a failed run changed the SFTP server")


def scenario_e_wrong_host_key(ctx: Context) -> None:
    before = snapshot(ctx.work.sftp_data)
    check = ctx.backup.run("--check", SFTP_HOST_KEY=ctx.work.foreign_fingerprint)
    expect(check.code == 1, f"--check with a wrong host key: exit {check.code}, expected 1\n{check.output}")
    lines = parse_check_lines(check.stdout)
    sftp_ok, sftp_detail = lines.get("sftp", (True, ""))
    expect(sftp_ok is False, f"no FAIL sftp line: {lines}")
    # The server presents its generated Ed25519 key, which the pinned foreign fingerprint rejects.
    expect_host_key_refusal(sftp_detail, ctx.work.host_key_fingerprint, "--check")
    once = ctx.backup.run("--once", SFTP_HOST_KEY=ctx.work.foreign_fingerprint)
    # Exit 1 is a failed run; 2 would be a configuration error, which is not what this scenario tests.
    expect(once.code == 1, f"--once with a wrong host key: exit {once.code}, expected 1\n{once.output}")
    expect_host_key_refusal(once.output, ctx.work.host_key_fingerprint, "--once")
    expect("stage=sftp-preflight" in once.output, f"--once did not fail in the SFTP preflight:\n{once.output}")
    expect(snapshot(ctx.work.sftp_data) == before, "a run with a wrong host key changed the SFTP server")


def scenario_f_retention(ctx: Context) -> None:
    directory = ctx.work.sftp_data / RETENTION_DIR
    directory.mkdir()
    seed = retention_seed(datetime.datetime.now(datetime.UTC).replace(tzinfo=None))
    for name in seed.all:
        (directory / name).write_bytes(b"odoo-backup e2e retention seed\n")
    settings = {
        "SFTP_PATH": f"{SFTP_ROOT}/{RETENTION_DIR}",
        "BACKUP_TIME": "00:00",  # daily representative = earliest backup of the day
        "DAILY_BACKUP_KEEP": str(KEEP_DAILY),
        "MONTHLY_BACKUP_KEEP": str(KEEP_MONTHLY),
        "YEARLY_BACKUP_KEEP": str(KEEP_YEARLY),
    }
    keep = {"keep_daily": KEEP_DAILY, "keep_monthly": KEEP_MONTHLY, "keep_yearly": KEEP_YEARLY}

    # --retention-plan prints what the current listing would lose and deletes nothing.
    listing = ctx.listing(RETENTION_DIR)
    plan = ctx.backup.run("--retention-plan", **settings)
    require_ok(plan, "--retention-plan")
    planned = set(re.findall(r"^  would delete (\S+)$", plan.stdout, re.M))
    _kept, deleted = expected_retention(listing, DB_NAME, **keep)
    expect(planned == deleted, f"--retention-plan would delete {sorted(planned)}, expected {sorted(deleted)}")
    expect(ctx.listing(RETENTION_DIR) == listing, "--retention-plan changed the directory")

    # RETENTION_DRY_RUN=true: the backup is uploaded, nothing else changes.
    dry = ctx.backup.run("--once", RETENTION_DRY_RUN="true", **settings)
    require_ok(dry, "--once with RETENTION_DRY_RUN=true")
    after_dry = ctx.listing(RETENTION_DIR)
    first = new_backup(listing, after_dry, RETENTION_DIR)
    _kept, deleted = expected_retention(after_dry, DB_NAME, **keep, protect={first})
    expect(after_dry == listing | {first}, "RETENTION_DRY_RUN=true deleted files")
    expect(
        f"RETENTION_DRY_RUN: would delete {len(deleted)} backup(s)" in dry.stderr,
        f"the dry run does not announce {len(deleted)} deletions",
    )
    expect("would remove the stale partial upload" in dry.stderr, "the dry run does not announce the stale partial")

    # The real run removes the stale partial and exactly the backups outside the policy.
    real = ctx.backup.run("--once", **settings)
    require_ok(real, "--once with retention")
    after = ctx.listing(RETENTION_DIR)
    second = new_backup(after_dry, after, RETENTION_DIR)
    kept, deleted = expected_retention(after_dry | {second}, DB_NAME, **keep, protect={second})
    expected = (after_dry | {second}) - deleted - {seed.stale_partial}
    expect(len(deleted) >= 40, f"the seed should lose most backups, the expectation deletes only {len(deleted)}")
    expect({first, second} <= kept and set(seed.untouchable) <= expected, "inconsistent expectation")
    expect(
        after == expected,
        f"retention result differs: unexpected {sorted(after - expected)}, missing {sorted(expected - after)}",
    )
    expect(
        f"retention=deleted {len(deleted)}/failed 0" in real.stderr, f"the run did not report {len(deleted)} deletions"
    )


def scenario_g_restore(ctx: Context) -> None:
    expect(ctx.full_backup is not None, "no full backup from scenario b")
    assert ctx.full_backup is not None
    status, location, body = ctx.odoo.restore(ctx.secret.master_password, RESTORED_DB, ctx.full_backup)
    expect(
        status in (302, 303) and location.endswith("/web/database/manager"),
        f"restore answered HTTP {status} {location!r}: {body[:2000].decode(errors='replace')}",
    )
    databases = ctx.odoo.jsonrpc("/web/database/list")
    expect(isinstance(databases, list) and RESTORED_DB in databases, f"{RESTORED_DB} is not listed: {databases}")
    uid = ctx.odoo.authenticate(RESTORED_DB, "admin", ctx.secret.admin_password)
    (record,) = ctx.odoo.execute(
        RESTORED_DB,
        uid,
        ctx.secret.admin_password,
        "ir.attachment",
        "read",
        [ctx.attachment_id],
        fields=["datas", "checksum"],
    )
    expect(base64.b64decode(record["datas"]) == ATTACHMENT_BYTES, "the restored attachment differs")
    expect(record["checksum"] == ctx.attachment["checksum"], "the restored attachment has another checksum")


def scenario_h_health(ctx: Context) -> None:
    result = ctx.backup.run("--health")
    require_ok(result, "--health")
    expect(result.stdout.startswith("healthy"), f"--health printed {result.stdout!r}")


SCENARIOS = (
    ("a --check", scenario_a_check),
    ("b --once (full backup)", scenario_b_full_backup),
    ("c --once --database-only", scenario_c_database_only),
    ("g restore of the full backup", scenario_g_restore),
    ("d wrong ODOO_MASTER_PWD", scenario_d_wrong_master_password),
    ("e wrong SFTP_HOST_KEY", scenario_e_wrong_host_key),
    ("f retention", scenario_f_retention),
    ("h --health", scenario_h_health),
)


# ------------------------------------------------------------------------------------------
# Main
# ------------------------------------------------------------------------------------------


def parse_args(argv: Sequence[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--image", help="backup image to test, e.g. odoo-backup:e2e (required unless --prepare-only)")
    parser.add_argument("--work-dir", type=pathlib.Path, help="directory for keys, configuration and logs")
    parser.add_argument("--odoo-port", type=int, default=18069, help="host port for Odoo on 127.0.0.1")
    parser.add_argument("--keep", action="store_true", help="leave the containers running afterwards")
    parser.add_argument(
        "--deadline",
        type=float,
        default=DEFAULT_DEADLINE,
        metavar="SECONDS",
        help=f"fail the run after this many seconds, logs collected (default {DEFAULT_DEADLINE}; 0: no deadline)",
    )
    parser.add_argument("--prepare-only", action="store_true", help="only generate the work directory")
    args = parser.parse_args(argv)
    if not args.image and not args.prepare_only:
        parser.error("--image is required")
    if args.deadline < 0:
        parser.error("--deadline must not be negative")
    return args


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    missing = [tool for tool in ("ssh-keygen", *(() if args.prepare_only else ("docker",))) if not shutil.which(tool)]
    if missing:
        print(f"Missing on PATH: {', '.join(missing)}", file=sys.stderr)
        return 1
    root = args.work_dir or pathlib.Path(tempfile.mkdtemp(prefix="odoo-backup-e2e-"))
    secret = generate_secrets()
    if os.environ.get("GITHUB_ACTIONS") == "true":
        for value in secret.values():
            print(f"::add-mask::{value}", flush=True)
    work = prepare_work_dir(root.resolve(), secret)
    print(f"Work directory: {work.root} (SFTP host key {work.host_key_fingerprint})", flush=True)
    if args.prepare_only:
        return 0

    shell = Shell(
        work.logs,
        {"E2E_WORK_DIR": str(work.root), "E2E_PG_PASSWORD": secret.pg_password, "E2E_ODOO_PORT": str(args.odoo_port)},
        secret.values(),
    )
    compose = Compose(shell)
    backup = BackupImage(
        shell,
        args.image,
        {
            "ODOO_URL": "http://odoo:8069",
            "ODOO_MASTER_PWD": secret.master_password,
            "ODOO_DB_NAME": DB_NAME,
            "ODOO_BACKUP_FORMAT": "zip",
            "TZ": "UTC",
            "SFTP_HOST": "sftp",
            "SFTP_PORT": "22",
            "SFTP_USER": SFTP_USER,
            "SFTP_PASSWORD": secret.sftp_password,
            "SFTP_HOST_KEY": work.host_key_fingerprint,
            "SFTP_PATH": f"{SFTP_ROOT}/{BACKUP_DIR}",
            "BACKUP_TMP_DIR": "/var/lib/odoo-backup/tmp",
            "BACKUP_STATE_DIR": "/var/lib/odoo-backup/state",
        },
    )
    ctx = Context(work, secret, OdooAPI(args.odoo_port), backup)
    failed = True
    try:
        # The deadline covers the run, not the log collection below: a hang still leaves its logs.
        with deadline(args.deadline):
            # Leftovers of an aborted local run.
            remove_backup_containers(shell, work.logs)
            compose.down()
            shell.run(["docker", "volume", "rm", "--force", STATE_VOLUME])
            step("Starting PostgreSQL and the SFTP server")
            compose("up", "--detach", "--wait", "--wait-timeout", "120", "db", "sftp")
            step(f"Creating database {DB_NAME!r} (odoo -i base, no demo data)")
            compose("run", "--rm", "-T", "odoo", "odoo", "--database", DB_NAME, "--init", "base", "--stop-after-init")
            step("Starting Odoo")
            compose("up", "--detach", "--wait", "--wait-timeout", str(ODOO_START_TIMEOUT), "odoo")
            serie = ctx.odoo.wait_until_ready(ODOO_START_TIMEOUT)
            expect(serie == ODOO_SERIE, f"Odoo reports server_serie {serie!r}, expected {ODOO_SERIE!r}")
            prepare_odoo(ctx)
            for title, scenario in SCENARIOS:
                step(f"Scenario {title}")
                scenario(ctx)
        failed = False
        print("\nAll end-to-end scenarios passed.", flush=True)
    except (E2EFailure, DeadlineExceeded) as exc:
        print(f"\nFAILED: {shell.redact(str(exc))}", file=sys.stderr, flush=True)
    except Exception:
        print(
            f"\nFAILED with an unexpected error:\n{shell.redact(traceback.format_exc())}", file=sys.stderr, flush=True
        )
    finally:
        if not args.keep:
            # Before the compose logs: a backup container that outlived its `docker run` (a hang,
            # the deadline) still holds the output that explains it.
            leftover = remove_backup_containers(shell, work.logs)
            if leftover:
                print(f"Saved and removed {leftover} backup container(s) that were still running", flush=True)
        logs = compose.logs()
        (work.logs / "compose.log").write_text(logs)
        if failed:
            print("\n----- docker compose logs -----\n" + logs, flush=True)
            print(f"Command log: {work.logs / 'commands.log'}", flush=True)
        if args.keep:
            print(
                "Containers kept running (--keep); remove them with: "
                f"docker compose --project-name odoo-backup-e2e down --volumes && docker volume rm {STATE_VOLUME}"
                f" (a backup container still running: docker ps --all --filter label={BACKUP_LABEL})"
            )
        else:
            compose.down()
            shell.run(["docker", "volume", "rm", "--force", STATE_VOLUME], timeout=CLEANUP_TIMEOUT)
    return 1 if failed else 0


def step(message: str) -> None:
    print(f"==> {message}", flush=True)


if __name__ == "__main__":
    sys.exit(main())
