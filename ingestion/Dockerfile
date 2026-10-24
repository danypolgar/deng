# syntax=docker/dockerfile:1.7
#
# Pipeline image: deng-pipeline:latest. Kestra starts one container from it for every task.
#
#   runtime (default) : minimal image used by Kestra and `docker compose run pipeline`
#   test              : runtime + pytest + tests, used by `docker compose run --rm test`
#
# Base image pinned by digest (multi-arch index: works on arm64 Macs and amd64 CI).
# Update: docker buildx imagetools inspect python:3.12-slim

ARG PYTHON_IMAGE=python:3.12-slim@sha256:a0f57064cbdcd6285fa45f51237ab0b35ea8ebaed06a1f5973e89e19dfb86704

# ---------------------------------------------------------------------------- build ----
# Install the dependencies into a virtualenv. Only the venv is copied to the runtime stage,
# so pip caches and build files never reach the final image.
FROM ${PYTHON_IMAGE} AS build

ENV PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_NO_CACHE_DIR=1

RUN python -m venv --without-pip /opt/venv

COPY ingestion/requirements.lock /tmp/requirements.lock
# The system pip installs into the venv; the venv itself gets no pip (smaller, less to patch).
RUN /usr/local/bin/python -m pip --python /opt/venv/bin/python install --no-deps -r /tmp/requirements.lock \
 && /usr/local/bin/python -m pip --python /opt/venv/bin/python check

# -------------------------------------------------------------------------- runtime ----
FROM ${PYTHON_IMAGE} AS runtime

LABEL org.opencontainers.image.title="deng-pipeline" \
      org.opencontainers.image.description="Polymarket calibration pipeline (DENG HS26, Team 04)" \
      org.opencontainers.image.source="https://github.com/danypolgar/deng" \
      org.opencontainers.image.licenses="MIT"

ENV PATH=/opt/venv/bin:$PATH \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONPATH=/app \
    SQL_DIR=/app/sql

# Fixed numeric uid/gid: no root at runtime, and the id resolves the same on every host.
RUN groupadd --system --gid 10001 pipeline \
 && useradd --system --uid 10001 --gid 10001 --no-create-home --shell /usr/sbin/nologin pipeline

WORKDIR /app
COPY --from=build /opt/venv /opt/venv
# Code and SQL stay owned by root and are read-only for the pipeline user.
COPY ingestion/polymarket_pipeline ./polymarket_pipeline
COPY transformations/sql ./sql

USER 10001:10001

# One-shot CLI container (each Kestra task runs one command and exits), so no HEALTHCHECK.
CMD ["python", "-m", "polymarket_pipeline", "--help"]

# ----------------------------------------------------------------------------- test ----
FROM runtime AS test

USER 0
COPY ingestion/requirements-dev.txt /tmp/
# requirements-dev.txt = runtime requirements + pytest. Runtime deps are already in the venv.
RUN /usr/local/bin/python -m pip --python /opt/venv/bin/python install \
      --no-cache-dir --disable-pip-version-check \
      "$(grep -E '^pytest==' /tmp/requirements-dev.txt)"
COPY ingestion/tests ./tests
USER 10001:10001

CMD ["python", "-m", "pytest", "-q", "-p", "no:cacheprovider", "tests"]
