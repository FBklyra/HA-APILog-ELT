# Docker image tarball (split)

GitHub refuses files over 100 MB, so `ha-apilog-elt-1.0.0-linux-amd64.tar.gz` (container image
`ghcr.io/fbklyra/ha-apilog-elt:1.0.0`, linux/amd64) is stored as 90 MB parts `*.part-00`, `*.part-01`, ...

- Linux / macOS: `sh join.sh` — or by hand: `cat ha-apilog-elt-1.0.0-linux-amd64.tar.gz.part-* > ha-apilog-elt-1.0.0-linux-amd64.tar.gz`
- Windows PowerShell: `.\join.ps1`

Both scripts verify the joined file against `../SHA256SUMS`. Then: `docker load -i ha-apilog-elt-1.0.0-linux-amd64.tar.gz`.
Easier: `docker pull ghcr.io/fbklyra/ha-apilog-elt:1.0.0`, or download the single file from the GitHub release.
