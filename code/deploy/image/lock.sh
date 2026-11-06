#!/bin/sh
# Regenerate the hashed Python locks (DEP-04): image (hub + connectors), CI toolchain, Linux agent. Run from the repo root after editing deploy/image/requirements*.{txt,in}:
#   sh deploy/image/lock.sh
# Resolves inside the same base image the product uses (Python 3.12, Linux); pip-compile records the sha256 of every
# published artifact of each pinned version (amd64 + arm64 wheels alike), so one lock serves both architectures.
# Consumers install with `pip install --require-hashes --no-deps -r <lock>` (Dockerfile, CI, deploy/linux/install.sh).
set -eu
IMG="python:3.12-slim-bookworm@sha256:e697c81897302a4c4333bade000d98038fbb21eb064f8ee1fd42d02ebcd9d337"
docker run --rm -v "$(pwd)/deploy:/d" -w /d/image "$IMG" sh -c '
  pip install -q --disable-pip-version-check "pip-tools==7.4.1" &&
  pip-compile -q --allow-unsafe --generate-hashes --strip-extras --no-header -o requirements.lock requirements.txt &&
  pip-compile -q --allow-unsafe --generate-hashes --strip-extras --no-header -o requirements-ci.lock requirements-ci.in &&
  pip-compile -q --allow-unsafe --generate-hashes --strip-extras --no-header -o ../linux/requirements-agent.lock ../linux/requirements-agent.in'
