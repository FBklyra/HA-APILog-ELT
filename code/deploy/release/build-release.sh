#!/bin/sh
# Assemble the downloadable ALELT release in dist/release/<version>/ (public release = linux/amd64 + x64 only).
# Same assets as build-release.ps1 (Windows MSI is built only by the .ps1 / CI Windows job).
#   deploy/release/build-release.sh [--version 0.1.0] [--image-name alelt] [--skip-images] [--dry-run]
#                                   [--agent-zip path.zip]   (Windows agent zip produced by the CI Windows job)
# SPDX-License-Identifier: Apache-2.0
set -eu
REPO=$(cd "$(dirname "$0")/../.." && pwd)
VERSION=0.1.0; IMAGE=alelt; SKIP_IMAGES=0; DRY=0; AGENT_ZIP=""; HELM_IMAGE=alpine/helm:3.16.2
OUTROOT="$REPO/dist/release"
while [ $# -gt 0 ]; do
  case "$1" in
    --version) VERSION=$2; shift 2 ;;
    --image-name) IMAGE=$2; shift 2 ;;
    --skip-images) SKIP_IMAGES=1; shift ;;
    --dry-run) DRY=1; shift ;;
    --agent-zip) AGENT_ZIP=$2; shift 2 ;;
    --out-root) OUTROOT=$2; shift 2 ;;
    *) echo "unknown option $1" >&2; exit 2 ;;
  esac
done
echo "$VERSION" | grep -Eq '^[0-9]+\.[0-9]+\.[0-9]+([-+][0-9A-Za-z.-]+)?$' || { echo "Version must be SemVer" >&2; exit 2; }
OUT="$OUTROOT/$VERSION"
mkdir -p "$OUTROOT"
STAGE=$(mktemp -d "$OUTROOT/.stage-XXXXXX")   # under OUTROOT so a containerised helm can mount it
hostpath() { (cd "$1" && pwd -W 2>/dev/null || pwd); }   # Git Bash on Windows: C:/... for docker -v
trap 'rm -rf "$STAGE"' EXIT
WARN=""
step() { echo "==> $*"; }
warn() { echo "WARN: $*" >&2; WARN="$WARN
  - $*"; }
plan() { [ "$DRY" = 1 ] && echo "    [dry-run] $*"; return 0; }
legal() { for f in LICENSE NOTICE DISCLAIMER.md; do [ -f "$REPO/$f" ] && cp "$REPO/$f" "$1/"; done; return 0; }
# DEP-28: release trees never carry developer-machine artefacts or secrets. scrub() removes them, deny_check() fails the
# build if anything matching the deny list is still present (defence in depth; CI builds from a clean checkout).
deny_find() {  # deny_find DIR [find actions...]: every deny-listed path under DIR (examples / READMEs / docs exempt)
  d=$1; shift
  find "$d" -depth \( -name .env -o -name '.env.*' -o -name '*.key' -o -name '*.pem' -o -name '*.p8' -o -name '*.pfx' \
    -o -name '*.p12' -o -name '*.crt' -o -name '*.jks' -o -name '*.db' -o -name '*.sqlite' -o -path '*/secrets/*' \
    -o -path '*/.alelt/*' -o -path '*/company-config/*' -o -name __pycache__ -o -name .pytest_cache \) \
    ! -name '*.example' ! -name 'README*' ! -name '*.md' "$@"
}
scrub() { deny_find "$1" -exec rm -rf {} + 2>/dev/null || true; }
deny_check() {
  left=$(deny_find "$1" -print 2>/dev/null | head -20)
  [ -z "$left" ] || { echo "ERROR: deny-listed files in release tree $1:" >&2; echo "$left" >&2; exit 1; }
}

step "release $VERSION -> $OUT"
[ "$DRY" = 1 ] || { rm -rf "$OUT"; mkdir -p "$OUT"; }

# 1. hub image (amd64 only)
IMG_TAR="alelt-$VERSION-linux-amd64.image.tar"
if [ "$SKIP_IMAGES" = 1 ]; then step "image: skipped (--skip-images)"; else
  step "image: build linux/amd64 $IMAGE:$VERSION -> $IMG_TAR"
  plan "deploy/build-image.sh --platform linux/amd64 --tag $IMAGE:$VERSION --output tar --out-dir <stage>"
  if [ "$DRY" = 0 ]; then
    sh "$REPO/deploy/build-image.sh" --platform linux/amd64 --tag "$IMAGE:$VERSION" --version "$VERSION" --output tar --out-dir "$STAGE/img"
    mv "$STAGE"/img/*-amd64.tar "$OUT/$IMG_TAR"
  fi
fi

# 2. Windows agent zip (built on Windows; passed in)
if [ -n "$AGENT_ZIP" ]; then
  step "agent: alelt-agent-$VERSION-windows-x64.zip (from $AGENT_ZIP)"
  [ "$DRY" = 1 ] || cp "$AGENT_ZIP" "$OUT/alelt-agent-$VERSION-windows-x64.zip"
else warn "agent: no --agent-zip given (Windows build runs in build-release.ps1 / CI windows job)"; fi

# 3. Linux edge daemon (x64): deploy/linux/install.sh installs from a source tree (--src, default ../..),
#    so the tarball keeps the repo layout
if [ ! -d "$REPO/edge-agent/linux" ]; then warn "linux daemon: edge-agent/linux not found - skipped"; else
  TGZ="alelt-agentd-$VERSION-linux-x64.tar.gz"
  step "linux daemon: $TGZ (source tree for deploy/linux/install.sh)"
  [ -f "$REPO/deploy/linux/install.sh" ] || warn "linux daemon: deploy/linux/install.sh not found"
  PARTS=""; for p in edge-agent/linux hub connectors config deploy/linux deploy/image/requirements.txt deploy/image/requirements.lock; do [ -e "$REPO/$p" ] && PARTS="$PARTS $p"; done
  plan "tar.gz$PARTS (no tests, caches)"
  if [ "$DRY" = 0 ]; then
    D="$STAGE/agentd/alelt-agentd-$VERSION"; mkdir -p "$D"
    # shellcheck disable=SC2086
    (cd "$REPO" && tar -cf - --exclude=__pycache__ --exclude=.pytest_cache --exclude=tests $PARTS) | (cd "$D" && tar -xf -)
    scrub "$D"; deny_check "$D"
    legal "$D"
    tar -czf "$OUT/$TGZ" -C "$STAGE/agentd" "alelt-agentd-$VERSION"
  fi
fi

# 4. compose bundle
CB="alelt-compose-$VERSION.tar.gz"
step "compose bundle: $CB"
plan "tar.gz deploy/local deploy/ha deploy/hub (exclude .env and secret values)"
if [ "$DRY" = 0 ]; then
  C="$STAGE/compose/alelt-compose-$VERSION"; mkdir -p "$C"
  for d in local ha hub; do [ -d "$REPO/deploy/$d" ] && cp -R "$REPO/deploy/$d" "$C/$d"; done
  scrub "$C"; deny_check "$C"
  for f in deploy/HA.md deploy/INSTALL-IN-A-MORNING.md; do [ -f "$REPO/$f" ] && cp "$REPO/$f" "$C/"; done
  legal "$C"
  tar -czf "$OUT/$CB" -C "$STAGE/compose" "alelt-compose-$VERSION"
fi

# 5. helm chart
CHART=""
for c in "$REPO"/deploy/helm/*/Chart.yaml; do [ -f "$c" ] && { CHART=$(dirname "$c"); break; }; done
if [ -z "$CHART" ]; then warn "helm: no chart under deploy/helm - skipped"; else
  HT="alelt-helm-$VERSION.tgz"; step "helm: $HT (chart $(basename "$CHART"))"
  plan "helm package $(basename "$CHART") --version $VERSION --app-version $VERSION"
  if [ "$DRY" = 0 ]; then
    mkdir -p "$STAGE/helm"
    if command -v helm >/dev/null 2>&1; then helm package "$CHART" -d "$STAGE/helm" --version "$VERSION" --app-version "$VERSION"
    else docker run --rm -v "$(hostpath "$(dirname "$CHART")"):/charts:ro" -v "$(hostpath "$STAGE/helm"):/out" "$HELM_IMAGE" \
        package "/charts/$(basename "$CHART")" -d /out --version "$VERSION" --app-version "$VERSION"; fi
    mv "$STAGE"/helm/*.tgz "$OUT/$HT"
  fi
fi

# 6. release notes + checksums
step "RELEASE-NOTES.md + SHA256SUMS"
if [ "$DRY" = 0 ]; then
  LIST=$(cd "$OUT" && for f in *; do printf '| `%s` | %s KB |\\n' "$f" "$(( $(wc -c < "$f") / 1024 ))"; done)
  sed -e "s/{{VERSION}}/$VERSION/g" -e "s/{{DATE}}/$(date -u +%Y-%m-%d)/g" -e "s#{{ASSETS}}#$LIST#" \
    "$REPO/deploy/release/RELEASE-NOTES.template.md" > "$OUT/RELEASE-NOTES.md"
  (cd "$OUT" && find . -maxdepth 1 -type f ! -name SHA256SUMS | sed 's#^\./##' | sort | xargs sha256sum > SHA256SUMS)
  cat "$OUT/SHA256SUMS"
fi
[ -n "$WARN" ] && echo "Warnings:$WARN"
[ "$DRY" = 1 ] && echo "Done (dry-run, nothing written)" || echo "Done: $OUT"
