FROM python:3.13-alpine

# Environment variables
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

WORKDIR /usr/app/src

# Copy requirements first (better layer caching)
COPY requirements.txt .

# Install runtime dependencies (minimal)
RUN apk add --no-cache \
    libffi \
    openssh-client \
    openssl \
    tzdata

# Install temporary build dependencies, then remove them after pip install
RUN apk add --no-cache --virtual .build-deps \
    build-base \
    libffi-dev \
    openssl-dev \
 && pip install --no-cache-dir --upgrade pip \
 && pip install --no-cache-dir -r requirements.txt \
 && apk del .build-deps

# Copy the rest of the files
COPY backup.py .

# Optional: non-root user for security
RUN adduser -D appuser
USER appuser

CMD ["python", "backup.py"]
