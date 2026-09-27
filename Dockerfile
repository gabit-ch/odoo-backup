# python:3.14-alpine3.24, multi-arch index digest resolved from Docker Hub on 2026-09-27
# (Python 3.14.7; the base image already contains tzdata and ca-certificates).
# Dependabot updates the digest (see .github/dependabot.yml).
FROM python:3.14-alpine3.24@sha256:9e9fde4d32eedce0b661d9ab91e826b62dddf28e928c230ec55f1866cac66b01

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /usr/app/src

# Wheels only: no compiler, no -dev packages, nothing to remove afterwards.
COPY requirements.txt .
RUN pip install --root-user-action=ignore --only-binary=:all: --no-deps -r requirements.txt \
 && pip check

COPY backup.py .
COPY odoo_backup/ odoo_backup/

# Unprivileged user with a fixed UID/GID (BusyBox addgroup/adduser: -S system account without
# password or login shell, -H no home directory). /var/lib/odoo-backup is the suggested mount
# point for BACKUP_TMP_DIR and BACKUP_STATE_DIR; a new named volume inherits its owner.
RUN addgroup -S -g 10001 appuser \
 && adduser -S -D -H -u 10001 -G appuser appuser \
 && mkdir -p /var/lib/odoo-backup \
 && chown 10001:10001 /var/lib/odoo-backup \
 && chmod 0700 /var/lib/odoo-backup
USER 10001:10001

HEALTHCHECK --interval=5m --timeout=30s --start-period=10m \
    CMD ["python", "backup.py", "--health"]

CMD ["python", "backup.py"]
