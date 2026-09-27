"""Local stand-in for the Odoo HTTP endpoints used by odoo-backup (tests only).

:class:`OdooHTTPStub` runs a threaded ``http.server`` on ``127.0.0.1:0`` that
speaks HTTP/1.0 like Odoo's prefork workers: without a ``Content-Length`` the
body ends when the connection closes, so a truncated backup looks like a
normal end of file.  It emulates:

* ``POST /web/database/backup`` - octet-stream backup, or Odoo's HTML database
  manager page with ``Database backup error: ...`` (wrong master password,
  unknown database, disabled database manager)
* ``POST /web/webclient/version_info`` - JSON-RPC version dict
* ``POST /jsonrpc`` - ``db.migrate_databases`` (master password check),
  ``common.version``
* ``POST /web/database/list`` - JSON-RPC database list
* ``POST /xmlrpc/2/common`` - XML-RPC ``version()``

Every handler can be replaced (``stub.set_handler(path, handler)``), extra
paths can be added the same way (e.g. a heartbeat URL, GET works too), and
all requests are recorded in ``stub.requests``.  Nothing here talks to a real
service.
"""

import dataclasses
import io
import json
import random
import sys
import tarfile
import threading
import time
import traceback
import urllib.parse
import xmlrpc.client
import zipfile
from collections.abc import Callable, Iterable
from dataclasses import dataclass, field
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

BACKUP_PATH = "/web/database/backup"
VERSION_INFO_PATH = "/web/webclient/version_info"
JSONRPC_PATH = "/jsonrpc"
DATABASE_LIST_PATH = "/web/database/list"
XMLRPC_COMMON_PATH = "/xmlrpc/2/common"

TAR_MODES = {"tar": "w", "tar.gz": "w:gz", "tar.bz2": "w:bz2", "tar.xz": "w:xz", "tar.zst": "w:zst"}


# --------------------------------------------------------------------------
# Requests and responses
# --------------------------------------------------------------------------


@dataclass
class StubRequest:
    method: str
    path: str  # without query string
    query: dict[str, list[str]]
    headers: dict[str, str]  # lower-case names
    body: bytes

    @property
    def form(self) -> dict[str, str]:
        """URL-encoded form fields (last value wins)."""
        parsed = urllib.parse.parse_qs(self.body.decode("utf-8"), keep_blank_values=True)
        return {key: values[-1] for key, values in parsed.items()}

    @property
    def json(self) -> Any:
        return json.loads(self.body)


@dataclass
class StubResponse:
    """What the stub sends back.

    ``content_length``: ``None`` = no header (close-delimited body, like Odoo
    prefork), ``True`` = the real length (bytes bodies only), an int = that
    value verbatim (to fake a mismatch).  ``truncate_after``: send only that
    many body bytes, then close.  ``delay``: seconds to wait before the status
    line.
    """

    status: int = 200
    body: bytes | Iterable[bytes] = b""
    headers: list[tuple[str, str]] = field(default_factory=list)
    content_length: bool | int | None = None
    truncate_after: int | None = None
    delay: float = 0.0


Handler = Callable[[StubRequest], StubResponse]


def html_response(body: bytes | str, status: int = 200) -> StubResponse:
    data = body.encode("utf-8") if isinstance(body, str) else body
    return StubResponse(status, data, [("Content-Type", "text/html; charset=utf-8")], content_length=True)


def not_found(req: StubRequest | None = None) -> StubResponse:
    """Werkzeug-style 404 page (what Odoo answers for an unknown route)."""
    page = (
        "<!doctype html>\n<html lang=en>\n<title>404 Not Found</title>\n<h1>Not Found</h1>\n"
        "<p>The requested URL was not found on the server. If you entered the URL manually please "
        "check your spelling and try again.</p>\n"
    )
    return html_response(page, status=404)


def jsonrpc_result(result: Any, request_id: Any = None) -> StubResponse:
    body = json.dumps({"jsonrpc": "2.0", "id": request_id, "result": result}).encode()
    return StubResponse(200, body, [("Content-Type", "application/json")], content_length=True)


def jsonrpc_error(name: str, message: str, request_id: Any = None, code: int = 0) -> StubResponse:
    """JSON-RPC error as Odoo 19 serializes exceptions (``odoo.http.serialize_exception``)."""
    error = {
        "code": code,
        "message": "404: Not Found" if code == 404 else "Odoo Server Error",
        "data": {
            "name": name,
            "debug": f"Traceback (most recent call last):\n  ...\n{name}: {message}\n",
            "message": message,
            "arguments": [message],
            "context": {},
        },
    }
    body = json.dumps({"jsonrpc": "2.0", "id": request_id, "error": error}).encode()
    return StubResponse(200, body, [("Content-Type", "application/json")], content_length=True)


def xmlrpc_result(value: Any) -> StubResponse:
    body = xmlrpc.client.dumps((value,), methodresponse=True, allow_none=True).encode()
    return StubResponse(200, body, [("Content-Type", "text/xml")], content_length=True)


def backup_response(body: bytes | Iterable[bytes], *, filename: str = "backup", **options: Any) -> StubResponse:
    """A successful backup answer as Odoo sends it (no Content-Length by default)."""
    headers = [
        ("Content-Type", "application/octet-stream; charset=binary"),
        ("Content-Disposition", f"attachment; filename*=UTF-8''{urllib.parse.quote(filename)}"),
    ]
    return StubResponse(200, body, headers, **options)


# --------------------------------------------------------------------------
# Odoo database manager error page
# --------------------------------------------------------------------------


def _qweb_escape(text: str) -> str:
    """Escape like QWeb t-out / markupsafe (' becomes &#39;, " becomes &#34;)."""
    return (
        text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace('"', "&#34;").replace("'", "&#39;")
    )


def odoo_error_page(error: str, *, list_db: bool = True, databases: Iterable[str] = ("master",)) -> str:
    """Render the Odoo 19 database manager page as ``Database._render_template(error=...)`` does.

    Structure (head with assets, logo, the list_db alert, the ``alert-danger``
    error block, the database list and the backup modal with its password
    field) follows ``web/static/src/public/database_manager.qweb.html``.
    """
    rows = "".join(
        f"""
                            <div class="list-group-item d-flex align-items-center">
                                <a href="/odoo?db={_qweb_escape(name)}" class="d-block flex-grow-1">{_qweb_escape(name)}</a>
                                <div class="btn-group btn-group-sm float-end">
                                    <button type="button" data-db="{_qweb_escape(name)}" data-bs-target=".o_database_backup" class="o_database_action btn btn-primary">
                                        <i class="fa fa-floppy-o fa-fw"></i> Backup
                                    </button>
                                </div>
                            </div>"""
        for name in (databases if list_db else ())
    )
    disabled = (
        '<div class="alert alert-danger text-center">The database manager has been disabled by the administrator</div>'
        if not list_db
        else ""
    )
    return f"""<!DOCTYPE html>
<html>
<head>
    <meta http-equiv="content-type" content="text/html; charset=utf-8"/>
    <title>Odoo</title>
    <link rel="shortcut icon" href="/web/static/img/favicon.ico" type="image/x-icon"/>
    <link rel="stylesheet" href="/web/static/lib/bootstrap/dist/css/bootstrap.css"/>
    <script src="/web/static/lib/bootstrap/js/dist/modal.js"></script>
    <script src="/web/static/src/public/database_manager.js"></script>
    <style>
        a {{
            text-decoration: none;
        }}
    </style>
</head>
<body>
    <div class="container">
        <!-- Database List -->
        <div class="row">
            <div class="col-lg-6 offset-lg-3 o_database_list">
                <img src="/web/static/img/logo2.png" class="img-fluid d-block mx-auto"/>
                {disabled}
                <div class="alert alert-danger">{_qweb_escape(error)}</div>
                <div class="list-group">{rows}
                </div>
                <div class="d-flex mt-2">
                    <button type="button" data-bs-toggle="modal" data-bs-target=".o_database_create" class="btn btn-primary flex-grow-1">Create Database</button>
                    <button type="button" data-bs-toggle="modal" data-bs-target=".o_database_restore" class="btn btn-primary flex-grow-1 ms-2">Restore Database</button>
                </div>
            </div>
        </div>

        <!-- Backup DB -->
        <div class="modal fade o_database_backup" role="dialog">
            <div class="modal-dialog">
                <div class="modal-content">
                    <div class="modal-header">
                        <h4 class="modal-title">Backup Database</h4>
                        <button type="button" class="btn-close" data-bs-dismiss="modal" aria-label="Close"></button>
                    </div>
                    <form id="form_backup_db" role="form" action="/web/database/backup" method="post">
                        <div class="modal-body">
                            <div class="row mb-3">
                                <label for="master_pwd" class="col-md-4 col-form-label">Master Password</label>
                                <div class="col-md-8">
                                    <input name="master_pwd" class="form-control" required="required" type="password" autocomplete="current-password"/>
                                </div>
                            </div>
                            <div class="row mb-3">
                                <label for="backup_format" class="col-md-4 col-form-label">Backup Format</label>
                                <div class="col-md-8">
                                    <select id="backup_format" name="backup_format" class="form-select" required="required">
                                        <option value="zip">zip</option>
                                        <option value="dump">pg_dump custom format (without filestore)</option>
                                    </select>
                                </div>
                            </div>
                        </div>
                        <div class="modal-footer">
                            <input type="submit" value="Backup" class="btn btn-primary float-end"/>
                        </div>
                    </form>
                </div>
            </div>
        </div>
    </div>
</body>
</html>
"""


# --------------------------------------------------------------------------
# Synthetic Odoo-like backups
# --------------------------------------------------------------------------


def odoo_like_members(
    with_filestore: bool = True, *, seed: int = 0, sql_name: str = "sql.dump"
) -> list[tuple[str, bytes | None]]:
    """Members of a small Odoo backup: ``(name, data)``, ``data=None`` for directories.

    Contains the pg_dump, manifest.json and, with the filestore, directory
    entries, a member larger than 8 KiB and a path longer than 100 characters
    (tarfile writes a PAX header for it).
    """
    rnd = random.Random(seed)
    sql = b"PGDMP" + rnd.randbytes(20_000) if sql_name == "sql.dump" else b"--\n-- PostgreSQL database dump\n--\n" * 400
    manifest = json.dumps(
        {
            "odoo_dump": "1",
            "db_name": "master",
            "version": "19.0",
            "major_version": "19.0",
            "pg_version": "16.4",
            "modules": {"base": "19.0.1.3"},
        },
        indent=4,
    ).encode()
    members: list[tuple[str, bytes | None]] = [(sql_name, sql), ("manifest.json", manifest)]
    if with_filestore:
        members += [
            ("filestore", None),
            ("filestore/ab", None),
            ("filestore/ab/ab" + rnd.randbytes(19).hex(), rnd.randbytes(12_000)),
            ("filestore/cd", None),
            ("filestore/cd/cd" + rnd.randbytes(19).hex(), b"small attachment\n"),
            ("filestore/ef/" + "long-directory-name-" * 5 + "/ef" + rnd.randbytes(19).hex(), rnd.randbytes(3_000)),
        ]
    return members


def build_tar(members: Iterable[tuple[str, bytes | None]], mode: str = "w", **options: Any) -> bytes:
    buffer = io.BytesIO()
    with tarfile.open(fileobj=buffer, mode=mode, **options) as archive:
        for name, data in members:
            info = tarfile.TarInfo(name)
            info.mtime = 1_790_000_000
            if data is None:
                info.type = tarfile.DIRTYPE
                info.mode = 0o755
                archive.addfile(info)
            else:
                info.size = len(data)
                info.mode = 0o644
                archive.addfile(info, io.BytesIO(data))
    return buffer.getvalue()


def build_zip(members: Iterable[tuple[str, bytes | None]]) -> bytes:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as archive:
        for name, data in members:
            if data is None:
                archive.writestr(name.rstrip("/") + "/", b"")
            else:
                archive.writestr(name, data)
    return buffer.getvalue()


def build_backup(fmt: str, with_filestore: bool = True, *, seed: int = 0) -> bytes:
    """A small, valid backup in ``fmt`` like Odoo core (zip, dump) or web_backup (tar*) produces it."""
    if fmt == "dump":
        return b"PGDMP" + random.Random(seed).randbytes(30_000)
    if fmt == "zip":
        return build_zip(odoo_like_members(with_filestore, seed=seed, sql_name="dump.sql"))
    return build_tar(odoo_like_members(with_filestore, seed=seed), TAR_MODES[fmt])


def _str2bool(value: str) -> bool:
    """Odoo's str2bool for the ``filestore`` form field (unknown values count as True here)."""
    return value.strip().lower() not in ("n", "no", "0", "false", "f", "off")


# --------------------------------------------------------------------------
# Server
# --------------------------------------------------------------------------


class _QuietServer(ThreadingHTTPServer):
    daemon_threads = True
    allow_reuse_address = True

    def handle_error(self, request, client_address) -> None:
        if isinstance(sys.exc_info()[1], (ConnectionError, TimeoutError)):
            return  # the client hung up (e.g. verification aborted the download)
        super().handle_error(request, client_address)


class OdooHTTPStub:
    """Threaded HTTP/1.0 server emulating Odoo; use as a context manager."""

    def __init__(
        self,
        *,
        master_password: str = "stub-master-password",
        databases: Iterable[str] = ("master",),
        server_serie: str = "19.0",
        list_db: bool = True,
        honour_filestore: bool = True,
    ) -> None:
        self.master_password = master_password
        self.databases = list(databases)
        self.server_serie = server_serie
        self.list_db = list_db
        self.honour_filestore = honour_filestore  # False emulates Odoo 17 / an old web_backup
        # (fmt, with_filestore) -> backup bytes (or an iterable of chunks)
        self.backup_body: Callable[[str, bool], bytes | Iterable[bytes]] = build_backup
        # extra StubResponse fields for successful backups, e.g. {"content_length": True}
        self.backup_overrides: dict[str, Any] = {}
        self.requests: list[StubRequest] = []
        self.handlers: dict[str, Handler] = {
            BACKUP_PATH: self.handle_backup,
            VERSION_INFO_PATH: self.handle_version_info,
            JSONRPC_PATH: self.handle_jsonrpc,
            DATABASE_LIST_PATH: self.handle_database_list,
            XMLRPC_COMMON_PATH: self.handle_xmlrpc_common,
        }
        self._lock = threading.Lock()
        self._server: _QuietServer | None = None
        self._thread: threading.Thread | None = None

    # lifecycle -------------------------------------------------------------

    def start(self) -> OdooHTTPStub:
        handler_class = type("_BoundHandler", (_StubHandler,), {"stub": self})
        self._server = _QuietServer(("127.0.0.1", 0), handler_class)
        self._thread = threading.Thread(
            target=self._server.serve_forever,
            kwargs={"poll_interval": 0.02},  # stop() waits for one poll interval
            name="odoo-http-stub",
            daemon=True,
        )
        self._thread.start()
        return self

    def stop(self) -> None:
        if self._server is not None:
            self._server.shutdown()
            self._server.server_close()
            self._thread.join(timeout=5)
            self._server = None

    def __enter__(self) -> OdooHTTPStub:
        return self.start()

    def __exit__(self, *exc_info: object) -> None:
        self.stop()

    @property
    def port(self) -> int:
        return self._server.server_address[1]

    @property
    def url(self) -> str:
        return f"http://127.0.0.1:{self.port}"

    # configuration and inspection -----------------------------------------------

    def set_handler(self, path: str, handler: Handler) -> None:
        self.handlers[path] = handler

    def requests_to(self, path: str) -> list[StubRequest]:
        with self._lock:
            return [req for req in self.requests if req.path == path]

    def record(self, req: StubRequest) -> None:
        with self._lock:
            self.requests.append(req)

    def version_info(self) -> dict[str, Any]:
        major = int(self.server_serie.split("~")[-1].split(".")[0])
        return {
            "server_version": self.server_serie,
            "server_version_info": [major, 0, 0, "final", 0, ""],
            "server_serie": self.server_serie,
            "protocol_version": 1,
        }

    # default Odoo behaviour ------------------------------------------------------

    def handle_backup(self, req: StubRequest) -> StubResponse:
        form = req.form
        # Odoo: check_super(master_pwd) first, then http.db_list() (AccessDenied without list_db)
        if form.get("master_pwd") != self.master_password or not self.list_db:
            return html_response(
                odoo_error_page("Database backup error: Access Denied", list_db=self.list_db, databases=self.databases)
            )
        name = form.get("name", "")
        if name not in self.databases:
            return html_response(
                odoo_error_page(f"Database backup error: Database {name!r} is not known", databases=self.databases)
            )
        fmt = form.get("backup_format", "zip")
        with_filestore = _str2bool(form.get("filestore", "true")) if self.honour_filestore else True
        response = backup_response(self.backup_body(fmt, with_filestore), filename=f"{name}_2026-09-27_01-00-00.{fmt}")
        return dataclasses.replace(response, **self.backup_overrides)

    def handle_version_info(self, req: StubRequest) -> StubResponse:
        return jsonrpc_result(self.version_info(), req.json.get("id"))

    def handle_database_list(self, req: StubRequest) -> StubResponse:
        request_id = req.json.get("id")
        if not self.list_db:
            return jsonrpc_error("odoo.exceptions.AccessDenied", "Access Denied", request_id)
        return jsonrpc_result(sorted(self.databases), request_id)

    def handle_jsonrpc(self, req: StubRequest) -> StubResponse:
        payload = req.json
        request_id = payload.get("id")
        params = payload.get("params") or {}
        service, method, args = params.get("service"), params.get("method"), params.get("args") or []
        if service == "common" and method == "version":
            return jsonrpc_result(self.version_info(), request_id)
        if service == "db" and method == "migrate_databases":
            # odoo.service.db.dispatch: check_super(args[0]), then @check_db_management_enabled
            if not args or args[0] != self.master_password or not self.list_db:
                return jsonrpc_error("odoo.exceptions.AccessDenied", "Access Denied", request_id)
            return jsonrpc_result(True, request_id)
        return jsonrpc_error("builtins.KeyError", f"'Method not found: {method}'", request_id)

    def handle_xmlrpc_common(self, req: StubRequest) -> StubResponse:
        _params, method = xmlrpc.client.loads(req.body)
        if method == "version":
            return xmlrpc_result(self.version_info())
        body = xmlrpc.client.dumps(xmlrpc.client.Fault(1, f"Method not found: {method}"), methodresponse=True)
        return StubResponse(200, body.encode(), [("Content-Type", "text/xml")], content_length=True)


class _StubHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.0"  # like Odoo prefork workers: close-delimited bodies
    server_version = "Werkzeug/3.1.3"
    stub: OdooHTTPStub

    def log_message(self, *args: Any) -> None:
        pass  # keep test output clean

    def do_GET(self) -> None:
        self._dispatch("GET")

    def do_POST(self) -> None:
        self._dispatch("POST")

    def _dispatch(self, method: str) -> None:
        length = int(self.headers.get("Content-Length") or 0)
        body = self.rfile.read(length) if length else b""
        split = urllib.parse.urlsplit(self.path)
        req = StubRequest(
            method=method,
            path=split.path,
            query=urllib.parse.parse_qs(split.query),
            headers={key.lower(): value for key, value in self.headers.items()},
            body=body,
        )
        self.stub.record(req)
        handler = self.stub.handlers.get(req.path, not_found)
        try:
            response = handler(req)
        except Exception:  # a broken test handler must be visible, not hang the client
            response = StubResponse(
                500, traceback.format_exc().encode(), [("Content-Type", "text/plain")], content_length=True
            )
        self._send(response)

    def _send(self, response: StubResponse) -> None:
        if response.delay:
            time.sleep(response.delay)
        chunks = [response.body] if isinstance(response.body, (bytes, bytearray)) else response.body
        self.send_response(response.status)
        for name, value in response.headers:
            self.send_header(name, value)
        if response.content_length is True:
            if not isinstance(response.body, (bytes, bytearray)):
                raise TypeError("content_length=True needs a bytes body")
            self.send_header("Content-Length", str(len(response.body)))
        elif response.content_length is not None and response.content_length is not False:
            self.send_header("Content-Length", str(response.content_length))
        self.end_headers()
        sent = 0
        for chunk in chunks:
            data = chunk if response.truncate_after is None else chunk[: max(0, response.truncate_after - sent)]
            if data:
                self.wfile.write(data)
                sent += len(data)
            if response.truncate_after is not None and sent >= response.truncate_after:
                break
        self.wfile.flush()
        self.close_connection = True


__all__ = [
    "BACKUP_PATH",
    "DATABASE_LIST_PATH",
    "JSONRPC_PATH",
    "VERSION_INFO_PATH",
    "XMLRPC_COMMON_PATH",
    "OdooHTTPStub",
    "StubRequest",
    "StubResponse",
    "backup_response",
    "build_backup",
    "build_tar",
    "build_zip",
    "html_response",
    "jsonrpc_error",
    "jsonrpc_result",
    "not_found",
    "odoo_error_page",
    "odoo_like_members",
    "xmlrpc_result",
]
