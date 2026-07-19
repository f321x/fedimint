#!/usr/bin/env bash
# Build a fedimintd container image from this working tree and load it into podman.
# See docs/low-io-patch.md
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

die() { echo "error: $*" >&2; exit 1; }

command -v nix >/dev/null || die "nix not found"
command -v podman >/dev/null || die "podman not found"

# Nix flakes only build git-tracked content. An uncommitted patch would silently
# produce an image built from unpatched source, which is the worst outcome here.
if [[ -n "$(git status --porcelain -- ':!result')" ]]; then
    git status --short -- ':!result' >&2
    die "working tree is dirty; commit the patch first (nix only sees tracked files)"
fi

if ! git grep -q FM_ROCKSDB_WAL_SYNC_INTERVAL_MS -- fedimint-rocksdb/src; then
    die "the low-IO patch is not applied on this commit ($(git rev-parse --short HEAD))"
fi

echo "==> building from $(git rev-parse --short HEAD) ($(git describe --tags --always))"
nix build -L .#container.fedimintd

echo "==> loading into podman"
# `podman load` prints e.g. "Loaded image: localhost/fedimintd:<tag>"
load_output="$(podman load --quiet < result 2>&1)"
echo "$load_output"
loaded="$(sed -n 's/^.*[Ll]oaded image[^:]*: //p' <<<"$load_output" | tail -1)"
[[ -n "$loaded" ]] || die "could not parse image name from: $load_output"

tag="fedimintd:lowio-$(git describe --tags --always)"
podman tag "$loaded" "$tag"

cat <<EOF

==> done

Image: $tag

Point your quadlet at it and enable the patch:

    [Container]
    Image=localhost/$tag
    Environment=FM_ROCKSDB_WAL_SYNC_INTERVAL_MS=5000

Then:

    systemctl --user daemon-reload
    systemctl --user restart <your-fedimintd>.service

Verify the write rate afterwards:

    pidstat -d -C fedimintd 60 5
EOF
