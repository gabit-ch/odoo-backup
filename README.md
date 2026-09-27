# odoo-backup

[![CI](https://github.com/gabit-ch/odoo-backup/actions/workflows/ci.yml/badge.svg)](https://github.com/gabit-ch/odoo-backup/actions/workflows/ci.yml)
[![CodeQL](https://github.com/gabit-ch/odoo-backup/actions/workflows/codeql.yml/badge.svg)](https://github.com/gabit-ch/odoo-backup/actions/workflows/codeql.yml)

## What is odoo-backup?
odoo-backup is a small Docker service that takes scheduled backups of an Odoo database through
Odoo's database manager, verifies every archive while it downloads, uploads it to any SFTP server
(for example a Hetzner Storage Box) and rotates the old backups with count-based retention.

Main features:

- Daily or every-N-hours schedule in your time zone (DST-safe), runs never overlap.
- Optional database-only backups between the daily full backup (`HOURLY_BACKUP_FILESTORE=false`).
- Every download is verified (compressed stream end, tar structure, `sql.dump` + `manifest.json`);
  an Odoo error page or a truncated download never becomes a "backup".
- Uploads go to `<name>.upload` and are renamed only after the server confirmed every byte.
- SSH host key pinning, password or key authentication, Docker secrets via `*_FILE`.
- Count-based retention (newest N, daily, monthly, yearly) that never deletes the last good
  backups during an outage.
- `--check` for post-deployment verification, a Docker `HEALTHCHECK`, and an optional heartbeat
  ping (e.g. healthchecks.io).

Quick reference:

- Contributed by: GABIT Marco Gantenbein
- Docker image: [gabitch/odoo-backup](https://hub.docker.com/r/gabitch/odoo-backup)
- License: GPLv3

## Quick start

```yaml
# docker-compose.yml
services:
  odoo-backup:
    image: gabitch/odoo-backup:2.0.0
    container_name: odoo-backup
    restart: always
    environment:
      ODOO_URL: http://odoo:8069
      ODOO_MASTER_PWD_FILE: /run/secrets/odoo_master_pwd
      ODOO_DB_NAME: master
      ODOO_BACKUP_FORMAT: tar.zst          # needs web_backup, see "Backup formats"
      TZ: Europe/Zurich
      BACKUP_TIME: "01:00"                 # quote times in YAML
      BACKUP_EVERY_HOUR: "2"
      HOURLY_BACKUP_FILESTORE: "false"     # 01:00 full, the other slots database-only
      HOURLY_BACKUP_KEEP: "12"
      DAILY_BACKUP_KEEP: "30"
      MONTHLY_BACKUP_KEEP: "12"
      YEARLY_BACKUP_KEEP: "-1"
      SFTP_HOST: u123456.your-storagebox.de
      SFTP_PORT: "23"
      SFTP_USER: u123456-sub1
      SFTP_PRIVATE_KEY_FILE: /run/secrets/sftp_key
      SFTP_HOST_KEY: "SHA256:XqONwb1S0zuj5A1CDxpOSuD2hnAArV1A3wKY7Z3sdgM"
      SFTP_PATH: /backups/odoo
      BACKUP_TMP_DIR: /var/lib/odoo-backup/tmp
      BACKUP_STATE_DIR: /var/lib/odoo-backup/state
      HEARTBEAT_URL_FILE: /run/secrets/heartbeat_url
    volumes:
      - odoo-backup-data:/var/lib/odoo-backup   # the image creates this directory for UID 10001
    secrets: [odoo_master_pwd, sftp_key, heartbeat_url]

volumes:
  odoo-backup-data:

secrets:
  odoo_master_pwd:
    file: ./secrets/odoo_master_pwd
  sftp_key:
    file: ./secrets/sftp_key
  heartbeat_url:
    file: ./secrets/heartbeat_url
```

The container runs as UID/GID 10001: secret and key files must be readable for that user.
After `docker compose up -d`, verify the deployment:

```sh
docker exec odoo-backup python backup.py --check
```

Before the first scheduled run of a new installation or an upgrade from 1.x, look at what the
retention would delete (see "Upgrading to 2.0.0"):

```sh
docker exec odoo-backup python backup.py --retention-plan
```

## Environment variables

An empty value counts as unset. Invalid values stop the service at start with exit code 2 and a
list of every problem; invalid retention values are the exception (see "Retention"). Secrets can
be passed directly or as a file (`NAME_FILE`, e.g. a Docker secret; one trailing line break is
removed); setting both is an error.

### Odoo

| Variable | Default | Description |
|---|---|---|
| `ODOO_URL` | required | Odoo base URL, e.g. `http://odoo:8069`. Only scheme, host and port are used. Use the internal address: redirects are not followed. |
| `ODOO_MASTER_PWD` / `ODOO_MASTER_PWD_FILE` | required | Odoo master password (`admin_passwd`). Odoo's database manager must be enabled (`list_db = True`). |
| `ODOO_DB_NAME` | required | Database to back up; must match Odoo's pattern `^[a-zA-Z0-9][a-zA-Z0-9_.-]+$`. |
| `ODOO_BACKUP_FORMAT` | `zip` | `zip`, `dump`, `tar`, `tar.gz`, `tar.bz2`, `tar.xz` or `tar.zst` (case-insensitive), see "Backup formats". |
| `ODOO_TIMEOUT` | `30` | Seconds for connecting to Odoo and for every JSON-RPC/XML-RPC call. |
| `ODOO_READ_TIMEOUT` | `14400` | Seconds Odoo may stay silent during the backup download (Odoo builds the dump before it sends the first byte). |

### Schedule

| Variable | Default | Description |
|---|---|---|
| `TZ` | `UTC` | IANA time zone of the schedule, the file names and the log. An unknown zone is a configuration error. |
| `BACKUP_TIME` | `02:00` | `H:MM`, `HH:MM` or `HH:MM:SS`. Daily mode: the backup time. Hourly mode: the first slot of the day, always a full backup, and the preferred time of the daily retention representative. |
| `BACKUP_EVERY_HOUR` | unset | Unset, empty or `0`: one backup per day. `1`..`24`: a backup every N hours from `BACKUP_TIME` (`24 // N` slots per day; if N does not divide 24 the last gap of the day is longer and a warning is logged). |
| `HOURLY_BACKUP_FILESTORE` | `true` | Hourly mode only. `false`: only the `BACKUP_TIME` slot is a full backup (database + filestore); the other slots are database-only backups in `DB_ONLY_BACKUP_PATH`. |
| `BACKUP_MAX_RUNTIME_MINUTES` | `480` | At least 10. A run that takes longer is considered hung: it is recorded as failed in the state file, the process exits with code 3 and Docker's restart policy starts a fresh service. |
| `TEST_MODE` | `false` | `true`: run one full backup at start and exit (same as `--once`; strict boolean). |

### Retention

| Variable | Default | Description |
|---|---|---|
| `HOURLY_BACKUP_KEEP` | `4` | Hourly mode only: keep the N newest backups; also the number of database-only backups kept (at least the newest one, also with `0`). |
| `DAILY_BACKUP_KEEP` | `30` | Keep one backup per day for the N newest days that have backups. |
| `MONTHLY_BACKUP_KEEP` | `12` | Keep one backup per month for the N newest months that have backups. |
| `YEARLY_BACKUP_KEEP` | `-1` | Keep one backup per year for the N newest years; `-1` keeps every year, `0` none. |
| `RETENTION_DRY_RUN` | `false` | `true`: only log what would be deleted (old backups and stale `.upload` partials). |

### SFTP

| Variable | Default | Description |
|---|---|---|
| `SFTP_HOST` | required | Host name or IP address. The 1.x form `host:port` still works but is deprecated. |
| `SFTP_PORT` | `22` | TCP port (`1`..`65535`). |
| `SFTP_USER` | required | User name. |
| `SFTP_PASSWORD` / `SFTP_PASSWORD_FILE` | unset | Password. Required unless `SFTP_PRIVATE_KEY_FILE` is set; with both, the key is tried first. |
| `SFTP_PRIVATE_KEY_FILE` | unset | OpenSSH private key (Ed25519, ECDSA or RSA) for public key authentication. |
| `SFTP_PRIVATE_KEY_PASSPHRASE` / `SFTP_PRIVATE_KEY_PASSPHRASE_FILE` | unset | Passphrase of an encrypted private key. |
| `SFTP_HOST_KEY` | unset | Pinned server host keys, separated by commas or new lines: `SHA256:...` fingerprints or public keys (`ssh-ed25519 AAAA...`, `known_hosts`/`ssh-keyscan` lines accepted). Set: a mismatch aborts before any authentication; the key types of pinned public keys are negotiated first. Unset: every connection logs a WARNING with the observed fingerprint. |
| `SFTP_PATH` | `/` | Directory of the full backups (created if missing, also by `--check`). Relative paths start in the login directory. |
| `DB_ONLY_BACKUP_PATH` | `<SFTP_PATH>/db-only` | Directory of the database-only backups; must differ from `SFTP_PATH`, also on the server: runs, `--check` and `--retention-plan` fail if the server resolves both to the same directory (e.g. a relative and an absolute spelling, or a symlink). |
| `SFTP_CIPHERS` | `aes128-gcm@openssh.com,aes256-gcm@openssh.com,aes128-ctr,aes256-ctr,aes192-ctr` | Preferred cipher order; names paramiko does not support are ignored with a warning, paramiko's other ciphers follow. |
| `SFTP_TIMEOUT` | `60` | Seconds for connecting, the SSH handshake, the authentication, opening the SFTP session and every SFTP request. |
| `SFTP_UPLOAD_ATTEMPTS` | `3` | `1`..`10` upload attempts; each retry starts from byte 0 after `5 s x attempt`. |
| `SFTP_MAX_REQUEST_SIZE` | auto | `4096`..`261120` bytes per SFTP write request. Auto: the server's `limits@openssh.com` value (at most 261120), else 32768. |

### Local directories, health and logging

| Variable | Default | Description |
|---|---|---|
| `BACKUP_TMP_DIR` | `/tmp/odoo-backup` | Local spool for the download (absolute path, mode 0700). Its contents are deleted when the service starts. Needs free space of 110 % of the newest backup. |
| `BACKUP_STATE_DIR` | `/tmp/odoo-backup-state` | Holds `state.json` (last success/failure) and the run lock. Must not be inside `BACKUP_TMP_DIR`. |
| `HEARTBEAT_URL` / `HEARTBEAT_URL_FILE` | unset | `http(s)://` URL requested with GET after every successful run. Treated as a secret. |
| `HEALTHCHECK_MAX_AGE_HOURS` | interval + max(2, interval // 2) (whole hours) | Age of the last successful backup after which `--health` fails: 36 h in daily mode, 4 h with `BACKUP_EVERY_HOUR=2`. With `HOURLY_BACKUP_FILESTORE=false` the last full backup must also be younger than 36 h (or this value, if larger). |
| `LOG_LEVEL` | `INFO` | `DEBUG`, `INFO`, `WARNING` or `ERROR`. paramiko and urllib3 always log at WARNING or above. |

## Backup formats

| Format | Produced by | Filestore | Verified while downloading |
|---|---|---|---|
| `zip` | Odoo core | yes (Odoo 19 honours database-only) | ZIP magic; after the download the central directory, `dump.sql` and `manifest.json` |
| `dump` | Odoo core | never | `PGDMP` magic only; a truncated dump **cannot** be detected |
| `tar` | web_backup | yes / database-only | every tar header checksum, member sizes, end-of-archive marker, `sql.dump` + `manifest.json` |
| `tar.gz`, `tar.bz2`, `tar.xz`, `tar.zst` | web_backup | yes / database-only | the compressed stream must end properly (gzip/bz2/xz CRCs), then the tar checks above |

The `tar*` formats need the Tradingzone module `web_backup` loaded server-wide
(`server_wide_modules = base,web,web_backup`). Without it Odoo answers with a raw pg_dump, which
the verification rejects with a hint. `tar.zst` is the fastest choice for large databases.

Odoo answers every error (wrong master password, unknown database, disabled database manager,
pg_dump failure) with an HTML page. Such an answer is always a failed backup: nothing is uploaded,
the retention does not run, and Odoo's error text is logged, e.g.
`Database backup error: Access Denied (check ODOO_MASTER_PWD ...)`.

## Schedule and database-only backups

Backups run synchronously at fixed local wall-clock times. A run that takes longer than the gap to
the next slot makes the scheduler skip the missed slots (logged as a WARNING) instead of starting
them late or in parallel. Daylight saving time: a slot inside the spring-forward gap runs after the
jump (02:30 runs at 03:30), a slot inside the autumn fold runs once.

With `BACKUP_EVERY_HOUR=2`, `BACKUP_TIME=01:00` and `HOURLY_BACKUP_FILESTORE=false`, the 01:00
backup contains database and filestore and goes to `SFTP_PATH`; the eleven other backups of the
day contain only the database (Odoo form field `filestore=false`) and go to `DB_ONLY_BACKUP_PATH`.
Restore tooling that picks "the newest archive in `SFTP_PATH`" therefore always gets a complete
backup. To restore a database-only backup, restore it and copy the filestore from the latest full
backup. Odoo 19 and a current web_backup honour `filestore=false`; Odoo 17 ignores it (the
backup then contains the filestore and a WARNING is logged).

## Retention

After every successful upload the retention plans both backup directories. Backups are matched by
their file name `odoo{server_serie}-{ODOO_DB_NAME}-{YYYYmmdd-HHMMSS}.{format}` (local time in
`TZ`); all series (e.g. 17.0 and 19.0) and formats of the database form one timeline, every other
file is ignored and never deleted. A backup is kept if any rule keeps it:

- the backup just uploaded and the newest backup (always);
- `HOURLY_BACKUP_KEEP` newest backups (hourly mode only);
- per day, the first backup at or after `BACKUP_TIME` (if there is none, the earliest one of the
  day), for the `DAILY_BACKUP_KEEP` newest days that have backups;
- per month, the daily representative of the earliest day, for the `MONTHLY_BACKUP_KEEP` newest months;
- per year, the monthly representative of the earliest month, for the `YEARLY_BACKUP_KEEP` newest
  years (`-1` = all).

The rules count buckets that contain backups, not calendar time: an outage of weeks never deletes
the last good backups. Backups dated more than a day in the future are kept (WARNING). Leftover
`.upload` files of interrupted runs are deleted at the start of the next run (with
`RETENTION_DRY_RUN=true` they are only logged).

Database-only backups keep only the newest `HOURLY_BACKUP_KEEP`, at least the newest one: with
`HOURLY_BACKUP_KEEP=0` only the newest database-only backup is kept, so they never pile up until
the SFTP server is full. In daily mode database-only backups (only from `--once --database-only`)
are never deleted.

Example, the production policy `BACKUP_EVERY_HOUR=2`, `BACKUP_TIME=01:00`,
`HOURLY_BACKUP_KEEP=12`, `DAILY_BACKUP_KEEP=30`, `MONTHLY_BACKUP_KEEP=12`, `YEARLY_BACKUP_KEEP=-1`,
after 16 months:

- full backups every 2 h: 53 backups, the last 24 hours (12), 01:00 of the last 30 days, the
  first 01:00 backup of each of the last 12 months, and one per year;
- with `HOURLY_BACKUP_FILESTORE=false`: 42 full backups in `SFTP_PATH` (the 12 newest are the last
  12 days) and the 12 newest database-only backups (about one day) in `DB_ONLY_BACKUP_PATH`.

The run logs one summary line per directory, e.g.
`keep 53 [hourly 12, daily 30, monthly 12, yearly 2], delete 1, ignored 0, future 0`.
`RETENTION_DRY_RUN=true` only logs the names that would be deleted; `--retention-plan` prints the
plan of both directories and deletes nothing.

If a retention variable is invalid, backups continue but nothing is deleted and every run is
reported as failed (the health check turns unhealthy) until the value is fixed.

## SFTP host key pinning

Pin the server's host key with `SFTP_HOST_KEY`; the connection is then refused before any
password or key signature is sent if the server presents another key. Get the fingerprint with:

```sh
ssh-keyscan -p 23 u123456.your-storagebox.de | ssh-keygen -lf -
```

or copy the `ssh-keyscan` lines into `SFTP_HOST_KEY`. A server with several host keys (OpenSSH,
e.g. port 23) prints one line per key type: pin all of them, or at least one public key line (the
service then negotiates that key type), or the fingerprint of the key the service is offered,
which is the Ed25519 key when the server has one (shown by the WARNING below and by `--check`). A
lone fingerprint of the server's RSA key fails as a mismatch on such a server. Several entries (key
rotation) are separated by commas or new lines. Without `SFTP_HOST_KEY`, every connection logs a
WARNING with the observed fingerprint, ready to be copied.

Hetzner Storage Box host keys as observed for this release (compare them with Hetzner's
documentation or `ssh-keyscan` before you pin them):

| Port | Server | Key | Fingerprint |
|---|---|---|---|
| 22 | ProFTPD mod_sftp | RSA | `SHA256:EMlfI8GsRIfpVkoW1H2u0zYVpFGKkIMKHFZIRkf2ioI` |
| 23 | OpenSSH | Ed25519 | `SHA256:XqONwb1S0zuj5A1CDxpOSuD2hnAArV1A3wKY7Z3sdgM` |

## Post-deployment check

```sh
docker exec odoo-backup python backup.py --check
```

prints one line per check and exits 0 only if every check passed, else 1:

```
OK config: database 'master', format tar.zst, every 2 h (database-only between full backups) from 01:00:00 (Europe/Zurich)
OK retention: last 12, daily 30, monthly 12, yearly all (anchor 01:00:00)
OK odoo: Odoo 19.0 at http://odoo:8069
OK master-password: accepted by Odoo
OK database: database 'master' exists
OK sftp: u123456-sub1@u123456.your-storagebox.de:23, host key ssh-ed25519 SHA256:XqONwb1S0zuj5A1CDxpOSuD2hnAArV1A3wKY7Z3sdgM pinned, cipher aes128-gcm@openssh.com
OK target-dir: /backups/odoo exists and is writable
OK db-only-dir: /backups/odoo/db-only exists and is writable
```

The check takes no backup: the master password is verified with `db.migrate_databases` on an
empty list (no side effect; the JSON-RPC route is deprecated in Odoo 19, so the service falls back
to a backup request for a non-existent database when it disappears), the directories with a
small probe file that is deleted again. Missing directories are created, as the first backup would
do (the line then says `created and is writable`; check the path if you did not expect that). The
check fails if `SFTP_PATH` and `DB_ONLY_BACKUP_PATH` are the same directory on the server. It never
prints a secret and finishes within two minutes even if every connection hangs. The service runs
the same checks once at start and logs them; it starts the schedule after two minutes at the
latest, even if a check hangs.

## Health check and heartbeat

The image declares `HEALTHCHECK --interval=5m --timeout=30s --start-period=10m CMD ["python",
"backup.py", "--health"]`. `--health` reads `BACKUP_STATE_DIR/state.json` and is healthy while the
last successful backup is younger than `HEALTHCHECK_MAX_AGE_HOURS`; before the first success it
is healthy for that long after the first start of the service (restarts, e.g. after the watchdog
ended a hung run, do not extend this grace period). The state file (mode 0600) holds only
timestamps, the last file names and a short error text.

With `HOURLY_BACKUP_FILESTORE=false` the database-only backups alone do not keep the service
healthy: the last full backup (database + filestore, the one restore tooling uses) must also be
younger than 36 h (or `HEALTHCHECK_MAX_AGE_HOURS`, if larger).

`HEARTBEAT_URL` receives a GET after every successful run (not after failures), which suits
dead-man's-switch services such as healthchecks.io. With `HOURLY_BACKUP_FILESTORE=false` the
heartbeat is also withheld (WARNING `Heartbeat ping not sent: ...`) while the full backups are
overdue as described above, so the dead-man's switch fires. The URL is never logged.

## Command line

| Command | Purpose | Exit codes |
|---|---|---|
| `python backup.py` | the service (Docker `CMD`) | 0 after SIGTERM/SIGINT, 2 invalid configuration, 3 a run exceeded `BACKUP_MAX_RUNTIME_MINUTES` |
| `python backup.py --once [--database-only]` | one backup now | 0 success, 1 failure, 2 invalid configuration, 3 the run exceeded `BACKUP_MAX_RUNTIME_MINUTES` |
| `python backup.py --check` | post-deployment verification | 0 all OK, 1 otherwise |
| `python backup.py --retention-plan` | print the retention plan, delete nothing | 0, 1 SFTP error or retention disabled, 2 invalid configuration |
| `python backup.py --health` | Docker health check | 0 healthy, 1 unhealthy |

SIGTERM (e.g. `docker stop`) while idle stops the service at once; during a run it interrupts
the run, removes the local temporary file, records the run as failed ("interrupted") and exits 0
(`--once` exits 1).
A second run cannot start while one is active (run lock in `BACKUP_STATE_DIR`), which also
protects a scheduled run from a manual `--once`.

## Security notes

- Pass secrets as Docker secrets through the `*_FILE` variables. The master password, the SFTP
  password, the key passphrase and the heartbeat URL are never logged or written to the state file.
- Use a Storage Box sub-account that only sees the backup directory, authenticate with an SSH key
  (`SFTP_PRIVATE_KEY_FILE`) and pin `SFTP_HOST_KEY`.
- The service deletes old backups, so whoever controls it can delete backups. Enable Storage Box
  snapshots (or another copy the service cannot reach) to survive a compromised or misconfigured
  service.
- The master password gives full access to Odoo's database manager. Keep `/web/database/*`
  unreachable from the internet at your reverse proxy; the service talks to Odoo on the internal
  network. If Odoo's master password is still the default `admin`, Odoo replaces it with
  `ODOO_MASTER_PWD` on the first backup request.
- The container runs as the unprivileged UID/GID 10001 without any build tools.

## Performance notes

- The Odoo side dominates: Odoo runs pg_dump and compresses the archive before it sends the first
  byte. `tar.zst` (multi-threaded zstd) is the fastest format; web_backup writes `tar.gz` with gzip
  level 1 and Odoo core `zip` with deflate.
- Download, verification and upload stream at network speed; the verification processes more than
  300 MB/s.
- Hetzner Storage Box: port 23 (OpenSSH) offers AES-GCM and announces large write requests;
  port 22 (ProFTPD) offers only AES-CTR with HMAC and most likely no `limits@openssh.com`, which
  means 32 KiB write requests. Prefer port 23.
- Keep `BACKUP_TMP_DIR` on a volume with room for 110 % of the largest backup; the backup is
  downloaded and verified completely before the upload starts.

## Upgrading to 2.0.0

2.0.0 is a rewrite with breaking changes:

- **The retention semantics changed, and the first run deletes every backup outside the new
  rules.** 1.x never deleted anything in hourly mode, so a long-running hourly installation may
  lose hundreds of files on the first 2.0.0 run. If unsure, start with `RETENTION_DRY_RUN=true`
  and look at `python backup.py --retention-plan` before you enable deletion.
- **Deployments that pull the `latest` image upgrade to 2.0.0 as soon as it is released**, without
  a dry run. Pin the image tag (e.g. `gabitch/odoo-backup:1.0.8`) before the release and upgrade
  deliberately.
- The configuration is validated at start: invalid or missing values stop the service with exit
  code 2 and a list of all problems (1.x silently used fallbacks, e.g. UTC for an unknown `TZ`).
  `TEST_MODE` is a strict boolean now (in 1.x any non-empty value, even `False`, enabled it).
- `SFTP_PORT` is honoured (1.x always connected to port 22). Check the value before upgrading.
- Odoo HTML error answers are failed backups now; 1.x uploaded such pages as backups.
- Python 3.14 and paramiko 5.0.0; `schedule`, `pytz` and `python-dateutil` are gone. The image
  runs as UID/GID 10001; make mounted secrets, keys and volumes readable for that user.
- The variables `BACKUP_CHUNK_SIZE_MB`, `BACKUP_QUEUE_MAX_SIZE`, `SFTP_SSH_CIPHERS`,
  `SFTP_SSH_COMPRESSION`, `SFTP_REKEY_BYTES`, `SFTP_REKEY_PACKETS`, `SFTP_TCP_NO_DELAY` and
  `SFTP_SOCK_BUF_KB` from the 1.x README were never read by the code and are removed from the
  documentation; use `SFTP_CIPHERS` for the cipher order.
- The service no longer empties the whole system temp directory at start, only `BACKUP_TMP_DIR`.
- Interrupted uploads are no longer resumed (a resumed file could combine two different uploads);
  every attempt starts from byte 0.
- The file name contract `odoo{server_serie}-{db}-{YYYYmmdd-HHMMSS}.{format}` is unchanged.

## Development and CI

Set up a virtual environment with the pinned runtime and development dependencies
(`requirements-dev.txt` includes `requirements.txt`; both pin their complete closure):

```sh
python3.14 -m venv .venv
.venv/bin/pip install --only-binary=:all: --no-deps -r requirements-dev.txt && .venv/bin/pip check
```

Unit tests, with branch coverage (the threshold `fail_under` is in `pyproject.toml`):

```sh
.venv/bin/python -X dev -W error::DeprecationWarning -m coverage run -m unittest discover -s tests -t . -v
.venv/bin/python -m coverage report
```

The unit tests use only the standard library plus local stubs (an Odoo HTTP stub and an
in-process paramiko SFTP server on 127.0.0.1); they need no network.

Lint, formatting and the dependency audit (`pyproject.toml` holds the ruff configuration):

```sh
.venv/bin/ruff check . && .venv/bin/ruff format --check .
.venv/bin/pip-audit -r requirements.txt -r requirements-dev.txt --no-deps --disable-pip --strict
```

End-to-end test against real Odoo 19, PostgreSQL 18 and an OpenSSH SFTP server (needs Docker with
the compose plugin, `ssh-keygen` and Python 3.12 or newer; it downloads the images pinned in
`tests/e2e/docker-compose.yml` and takes a few minutes):

```sh
docker build -t odoo-backup:e2e .
python3 tests/e2e/run_e2e.py --image odoo-backup:e2e
```

The driver generates every password and SSH host key it uses, creates a database with a binary
attachment, runs the image with `--check`, `--once`, `--once --database-only`, `--retention-plan`
and `--health`, checks the failures with a wrong master password and a wrong host key, the
retention on a seeded directory (dry run and real run) and restores the backup through Odoo's
database manager. `--keep` leaves the containers running, the logs (secrets redacted) are in
`<work dir>/logs`. The SFTP test image (`atmoz/sftp:debian`) is amd64-only and runs emulated on
arm64 machines.

GitHub Actions (`.github/workflows/ci.yml`) runs on every push, pull request and release tag:

| Job | What it checks |
|---|---|
| `lint` | `ruff check`, `ruff format --check`, actionlint (with shellcheck for the run steps), hadolint, shellcheck |
| `test` | the unit tests under coverage on Python 3.14 (`-X dev`, deprecation warnings are errors), coverage summary and `coverage.xml` |
| `audit` | `pip-audit` of `requirements.txt` and `requirements-dev.txt`; any known vulnerability fails |
| `image` | builds the linux/amd64 image and runs the unit tests inside it with `--network none` |
| `scan` | Grype scan of that image; fixable High or Critical vulnerabilities fail, the SARIF report goes to code scanning |
| `e2e` | `tests/e2e/run_e2e.py` against that image |
| `publish` | release tags `X.Y.Z` (never `0.x`) only, after all other jobs: multi-arch image (linux/amd64, linux/arm64) with provenance and SBOM to Docker Hub as `X.Y.Z`, `X.Y` and `latest` |

`.github/workflows/codeql.yml` runs CodeQL on pushes to `main`, pull requests into `main` and
weekly. Dependabot updates the Python pins, the base image digest, the end-to-end images and the
actions; the checksums of the tool binaries in `ci.yml` (actionlint, hadolint, shellcheck, Grype)
are updated by hand.

## Releases

### 2.0.0
* Rewrite as the `odoo_backup` package; `backup.py` is a thin entry point
* Count-based retention (newest, daily, monthly, yearly) that works in hourly mode, keeps the newest and just uploaded backup, and never deletes foreign files; `RETENTION_DRY_RUN` and `--retention-plan`
* Database-only backups between the daily full backup (`HOURLY_BACKUP_FILESTORE=false`, `DB_ONLY_BACKUP_PATH`)
* Streaming verification of every download; Odoo HTML error pages and truncated downloads are failed backups
* Verified SFTP uploads (all pipelined writes confirmed, size check, atomic rename), `SFTP_PORT` honoured, SSH host key pinning (`SFTP_HOST_KEY`), key authentication, configurable ciphers, retries
* DST-safe synchronous scheduler; skipped slots are logged; watchdog `BACKUP_MAX_RUNTIME_MINUTES`
* `--check`, `--once`, `--health` (Docker HEALTHCHECK), heartbeat URL, state file, run lock; with database-only backups, health and heartbeat also watch the daily full backup
* Fail-fast configuration, Docker secrets via `*_FILE`, secrets never logged
* Python 3.14, paramiko 5.0.0, requests 2.34.2; image pinned by digest, non-root UID 10001, no build tools; tests and CI
* CI: ruff, coverage gate, pip-audit, image vulnerability scan, CodeQL and an end-to-end test against Odoo 19, PostgreSQL 18 and OpenSSH before every release

### 1.0.8
* Increase timeout to 14400

### 1.0.7
* Fix remove timeout for backup requests

### 1.0.6
* Add additional supported backup formats
* Replace pysftp with paramiko
* Optimize SFTPHandler
* Add additional logging
* Update to python 3.13
* Update python packages to newest versions

### 1.0.5
* Fix remove local backup in the container after upload and on start of container

### 1.0.4
* Add Multithreading support for backup durations of more than 1 hour

### 1.0.3
* Fix "No hostkey" error for reconnecting

### 1.0.2
* Fix "SSH session not active" error for huge backups. Add reconnect method with 3 retries

### 1.0.1
* Add HOURLY_BACKUP_KEEP as a new environment variable
* Add docstrings to script, class and methods
* Fix cleanup process with remaining backups

### 1.0.0
First release with all base functions and environment variables

## Known issues

- `dump` format: a raw pg_dump stream has no end marker the service could check, so a truncated
  dump cannot be detected. Prefer a `tar*` format.
- `tar.zst` archives written by web_backup carry no zstd content checksum: truncation is detected,
  but a corrupted byte inside member data mostly is not. The same holds for member data of plain
  `tar` archives. `tar.gz`, `tar.bz2` and `tar.xz` carry CRCs.
- `zip` archives are verified only after the download is complete (the central directory is at
  the end of the file).
- Odoo 17 ignores `filestore=false`; database-only backups then contain the filestore.
