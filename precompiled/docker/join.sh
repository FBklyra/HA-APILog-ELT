#!/bin/sh
# Re-assemble the image tarball from its parts (GitHub file-size limit), verify it, and load it.
set -eu
cd "$(dirname "$0")"
F=ha-apilog-elt-1.0.0-linux-amd64.tar.gz
cat "$F".part-* > "$F"
grep " docker/$F\$" ../SHA256SUMS | sed "s# docker/# #" | sha256sum -c -
echo "load with: docker load -i $F"
