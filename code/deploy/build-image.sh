#!/bin/sh
# Build the ALELT product image with docker buildx (multi-arch). Same options as build-image.ps1:
#   deploy/build-image.sh [--platform linux/amd64,linux/arm64] [--tag alelt:local] [--push | --output load|tar]
#                         [--verify] [--version 0.1.0] [--builder aleltbuilder] [--out-dir dist]
# Offline install of a tar: docker load -i dist/alelt-0.1.0-amd64.tar
set -eu
REPO=$(cd "$(dirname "$0")/.." && pwd)
PLATFORM=""; TAG="alelt:local"; PUSH=0; OUTPUT=""; VERIFY=0; VERSION="0.1.0"; BUILDER="aleltbuilder"; OUTDIR="$REPO/dist"
while [ $# -gt 0 ]; do
  case "$1" in
    --platform) PLATFORM=$2; shift 2 ;;
    --tag) TAG=$2; shift 2 ;;
    --push) PUSH=1; shift ;;
    --output) OUTPUT=$2; shift 2 ;;
    --verify) VERIFY=1; shift ;;
    --version) VERSION=$2; shift 2 ;;
    --builder) BUILDER=$2; shift 2 ;;
    --out-dir) OUTDIR=$2; shift 2 ;;
    *) echo "unknown option $1" >&2; exit 2 ;;
  esac
done
if [ -z "$PLATFORM" ]; then
  case "$(docker version --format '{{.Server.Arch}}')" in arm64|aarch64) PLATFORM=linux/arm64 ;; *) PLATFORM=linux/amd64 ;; esac
fi
n=$(echo "$PLATFORM" | tr ',' '\n' | grep -c .)
if [ -z "$OUTPUT" ] && [ "$PUSH" = 0 ]; then if [ "$n" = 1 ]; then OUTPUT=load; else OUTPUT=tar; fi; fi
case "$(docker version --format '{{.Server.Arch}}')" in arm64|aarch64) HOSTP=linux/arm64 ;; *) HOSTP=linux/amd64 ;; esac
# host arch + load: current builder (local cache); otherwise a docker-container builder (QEMU for foreign archs)
if [ "$BUILDER" = "aleltbuilder" ] && [ "$PLATFORM" = "$HOSTP" ] && [ "$OUTPUT" = load ]; then BUILDER=default; fi
if [ "$BUILDER" = default ]; then BARGS=""; else
  docker buildx inspect "$BUILDER" >/dev/null 2>&1 || docker buildx create --name "$BUILDER" --driver docker-container --bootstrap >/dev/null
  BARGS="--builder $BUILDER"
fi
REV=$(git -C "$REPO" rev-parse --short HEAD 2>/dev/null || echo local)
# shellcheck disable=SC2086
set -- buildx build $BARGS -f "$REPO/deploy/image/Dockerfile" \
  --build-arg "ALELT_BUILD_VERSION=$VERSION" --build-arg "ALELT_BUILD_REVISION=$REV" \
  --build-arg "ALELT_BUILD_DATE=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
if [ "$VERIFY" = 1 ]; then
  for p in $(echo "$PLATFORM" | tr ',' ' '); do docker "$@" --platform "$p" --target verify --progress plain "$REPO"; done
fi
if [ "$PUSH" = 1 ]; then
  docker "$@" --platform "$PLATFORM" -t "$TAG" --push "$REPO"
elif [ "$OUTPUT" = load ]; then
  [ "$n" = 1 ] || { echo "--output load needs exactly one platform" >&2; exit 2; }
  docker "$@" --platform "$PLATFORM" -t "$TAG" --load "$REPO"
else
  mkdir -p "$OUTDIR"
  for p in $(echo "$PLATFORM" | tr ',' ' '); do
    arch=${p##*/}; name=$(echo "$TAG" | tr '/:' '--')-$arch.tar
    docker "$@" --platform "$p" -t "$TAG" --output "type=docker,dest=$OUTDIR/$name" "$REPO"
    echo "wrote $OUTDIR/$name  (install: docker load -i $name)"
  done
fi
