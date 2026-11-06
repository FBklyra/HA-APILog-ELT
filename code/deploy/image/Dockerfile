# syntax=docker/dockerfile:1
# ALELT product image: console + hub API + worker + migrations + C Db2 decoder, one OCI image (multi-arch).
#   docker build -f deploy/image/Dockerfile -t alelt:local .                          (build context = repo root)
#   deploy/build-image.ps1 -Platform linux/amd64,linux/arm64 -Tag <registry>/alelt:<ver> -Push | -Output tar
#   verification stage (decoder C tests + golden corpus with the native decoder, on the target arch):
#   docker buildx build --platform linux/amd64 --target verify -f deploy/image/Dockerfile .
# Run: alelt <api|worker|migrate|all|version>  (default: all)
# Base images pinned by digest (DEP-05; bump via Dependabot or `docker buildx imagetools inspect <tag>`).
ARG PYTHON_IMAGE=python:3.12-slim-bookworm@sha256:e697c81897302a4c4333bade000d98038fbb21eb064f8ee1fd42d02ebcd9d337
ARG NODE_IMAGE=node:22-bookworm-slim@sha256:4b150b48b4116e2a22f2c39cc27025fbf4f153e2078fd3b14d159fdb5c7279da
ARG DEBIAN_IMAGE=debian:bookworm-slim@sha256:99c0b25c46d0214d5a2091092e7908907dcaecd6a962cde0ac700acee222b6f3

# ---- console (static SPA, architecture-independent: built once on the build platform) -----------------------
FROM --platform=$BUILDPLATFORM ${NODE_IMAGE} AS console
WORKDIR /console
COPY console/package.json console/package-lock.json ./
RUN --mount=type=cache,target=/root/.npm npm ci --no-audit --no-fund --fetch-retries=5 --fetch-timeout=120000
COPY console/ ./
# Runtime /config.js (served by the hub) decides dev auth / OIDC; build defaults: real hub, OIDC.
ENV VITE_ALELT_MOCK=0 VITE_ALELT_DEV_AUTH=0 VITE_ALELT_API_BASE=/api/v1
RUN npm run build

# ---- libaleltdecode: cross-compiled on the build platform for $TARGETARCH (no emulation) ------------------------
FROM --platform=$BUILDPLATFORM ${DEBIAN_IMAGE} AS decoder
ARG TARGETARCH
ARG BUILDARCH
RUN set -eux; \
    case "$TARGETARCH" in amd64) triple=x86_64-linux-gnu ;; arm64) triple=aarch64-linux-gnu ;; \
      *) echo "unsupported TARGETARCH $TARGETARCH" >&2; exit 1 ;; esac; \
    if [ "$TARGETARCH" = "$BUILDARCH" ]; then pkgs="gcc libc6-dev"; cc=gcc; \
    else pkgs="gcc-$(echo $triple | tr _ -) libc6-dev-${TARGETARCH}-cross"; cc="$triple-gcc"; fi; \
    apt-get update && apt-get install -y --no-install-recommends cmake make $pkgs && rm -rf /var/lib/apt/lists/*; \
    printf '%s %s\n' "$triple" "$cc" > /tmp/toolchain
COPY connectors/sources/db2/decoder/ /src/decoder/
RUN set -eux; read triple cc < /tmp/toolchain; \
    cmake -S /src/decoder -B /build -DCMAKE_BUILD_TYPE=Release -DCMAKE_SYSTEM_NAME=Linux \
          -DCMAKE_SYSTEM_PROCESSOR="${triple%%-*}" -DCMAKE_C_COMPILER="$cc" -DALELTDECODE_TESTS=ON; \
    cmake --build /build -j"$(nproc)"; \
    mkdir -p /out/lib /out/test; cp /build/libaleltdecode.so /out/lib/; cp /build/test_aleltdecode /out/test/; \
    if [ "$TARGETARCH" = "$BUILDARCH" ]; then /out/test/test_aleltdecode; fi

# ---- python dependencies (target platform; binary wheels only) -----------------------------------------------
FROM ${PYTHON_IMAGE} AS deps
ENV PIP_DISABLE_PIP_VERSION_CHECK=1
# hashed lock (DEP-04): every artifact verified by sha256, no resolver at build time (regenerate: deploy/image/lock.sh)
COPY deploy/image/requirements.lock /tmp/requirements.lock
RUN --mount=type=cache,target=/root/.cache/pip \
    python -m venv /opt/alelt \
 && /opt/alelt/bin/pip install --retries 10 --timeout 120 --only-binary=:all: --require-hashes --no-deps -r /tmp/requirements.lock \
 && /opt/alelt/bin/pip check

# ---- runtime -------------------------------------------------------------------------------------------------
FROM ${PYTHON_IMAGE} AS runtime
ARG ALELT_BUILD_VERSION=0.1.0
ARG ALELT_BUILD_REVISION=dev
ARG ALELT_BUILD_DATE=unknown
LABEL org.opencontainers.image.title="ALELT" \
      org.opencontainers.image.description="Engine-agnostic log-based CDC replication platform (hub, worker, console)" \
      org.opencontainers.image.vendor="Freddy Beltran" \
      org.opencontainers.image.authors="Freddy Beltran" \
      org.opencontainers.image.licenses="Apache-2.0" \
      org.opencontainers.image.version="${ALELT_BUILD_VERSION}" \
      org.opencontainers.image.revision="${ALELT_BUILD_REVISION}"
ENV PATH=/opt/alelt/bin:$PATH PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1 \
    PYTHONPATH=/app/hub:/app/connectors/sources/db2:/app/connectors/sources/postgres:/app/connectors/sources/mysql:/app/connectors/sources/mariadb:/app/connectors/sources/oracle:/app/connectors/sources/sqlserver:/app/connectors/destinations/_spi:/app/connectors/destinations/postgres:/app/connectors/destinations/snowflake:/app/connectors/destinations/sql:/app/connectors/destinations/iceberg:/app/connectors/destinations/delta:/app/connectors/destinations/kafka:/app/connectors/destinations/clickhouse:/app/connectors/destinations/bigquery:/app/connectors/destinations/databricks:/app/connectors/destinations/redshift:/app/connectors/destinations/fabric \
    ALELT_BUILD_VERSION=${ALELT_BUILD_VERSION} ALELT_BUILD_REVISION=${ALELT_BUILD_REVISION} ALELT_BUILD_DATE=${ALELT_BUILD_DATE} \
    ALELT_CONSOLE_DIR=/app/console/dist ALELT_SPOOL_DIR=/var/lib/alelt/spool ALELT_LOG_DIR=/var/lib/alelt/logs \
    ALELT_DB2_DECODER=native ALELTDECODE_LIB=/opt/alelt/lib/libaleltdecode.so \
    ALELT_PORT=8080 ALELT_WORKER_ALIVE_FILE=/tmp/alelt_worker_alive
RUN useradd --system --uid 10001 --home-dir /app --shell /usr/sbin/nologin alelt \
 && mkdir -p /var/lib/alelt/spool /var/lib/alelt/logs /data \
 && chown -R alelt /var/lib/alelt /data
COPY --from=deps /opt/alelt /opt/alelt
COPY --from=decoder /out/lib/libaleltdecode.so /opt/alelt/lib/libaleltdecode.so
COPY hub/ /app/hub/
COPY connectors/ /app/connectors/
COPY config/ /app/config/
COPY tests/ladder/ /app/tests/ladder/
COPY --from=console /console/dist /app/console/dist
COPY deploy/image/alelt /usr/local/bin/alelt
RUN sed -i 's/\r$//' /usr/local/bin/alelt && chmod 0755 /usr/local/bin/alelt \
 && ALELT_DB2_DECODER=require-native python -c "from alelt_src_db2 import native; assert native.enable(); print('libaleltdecode loaded')"
WORKDIR /app
USER alelt
EXPOSE 8080 9100
VOLUME ["/var/lib/alelt"]
HEALTHCHECK --interval=15s --timeout=5s --start-period=40s --retries=4 \
  CMD python -c "import os,sys,time,urllib.request; r=os.environ.get('ALELT_ROLE','all'); p=os.environ.get('ALELT_WORKER_ALIVE_FILE','/tmp/alelt_worker_alive'); sys.exit((0 if os.path.exists(p) and time.time()-os.path.getmtime(p)<120 else 1) if r=='worker' else (0 if urllib.request.urlopen('http://127.0.0.1:'+os.environ.get('ALELT_PORT','8080')+'/readyz', timeout=4).status==200 else 1))"
ENTRYPOINT ["alelt"]
CMD ["all"]

# ---- verify (not shipped): decoder C tests + golden corpus with the native decoder, on the target arch --------
FROM runtime AS verify
USER root
COPY --from=decoder /out/test/test_aleltdecode /opt/alelt/test/test_aleltdecode
COPY tests/golden/ /app/tests/golden/
COPY deploy/image/requirements-ci.lock /tmp/requirements-ci.lock
RUN --mount=type=cache,target=/root/.cache/pip /opt/alelt/bin/pip install --retries 10 --timeout 120 --require-hashes --no-deps -r /tmp/requirements-ci.lock \
 && /opt/alelt/test/test_aleltdecode \
 && ALELT_DB2_DECODER=require-native python -m pytest -q -p no:cacheprovider /app/tests/golden/db2 \
 && python -c "import json; from alelt_hub.product import decoder_info; d=decoder_info(); print(json.dumps(d)); assert d['db2']['backend'] == 'native'" \
 && echo "verify $(uname -m): OK" | tee /verify.txt
USER alelt

# ---- default target = shipped runtime ------------------------------------------------------------------------
FROM runtime
