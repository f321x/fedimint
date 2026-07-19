#!/usr/bin/env bash
# Build a patched fedimintd container image and load it into podman.
# See docs/low-io-patch.md
#
# Default: builds with podman itself (Containerfile.lowio), so nothing beyond
# podman needs to be installed on the host.
# --nix:   reproduces the official upstream image instead. Requires nix.
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

die() { echo "error: $*" >&2; exit 1; }

USE_NIX=0
[[ "${1:-}" == "--nix" ]] && USE_NIX=1

command -v podman >/dev/null || die "podman not found"

if ! git grep -q FM_ROCKSDB_WAL_SYNC_INTERVAL_MS -- fedimint-rocksdb/src; then
    die "the low-IO patch is not applied on this commit ($(git rev-parse --short HEAD))"
fi

commit="$(git rev-parse HEAD)"
describe="$(git describe --tags --always)"
tag="fedimintd:lowio-${describe}"

if [[ -n "$(git status --porcelain -- ':!result')" ]]; then
    if (( USE_NIX )); then
        git status --short -- ':!result' >&2
        die "working tree is dirty; commit first (nix only builds git-tracked files)"
    fi
    # podman builds the working tree, so uncommitted changes DO end up in the
    # image - but the version stamped into the binary is the commit, which
    # would then misrepresent what is running.
    echo "warning: working tree is dirty; uncommitted changes will be built" >&2
    echo "         but the binary will report version ${describe}" >&2
fi

echo "==> building from ${describe} (${commit:0:11})"

if (( USE_NIX )); then
    command -v nix >/dev/null || die "nix not found (omit --nix to build with podman)"
    nix build -L .#container.fedimintd
    echo "==> loading into podman"
    load_output="$(podman load --quiet < result 2>&1)"
    echo "$load_output"
    loaded="$(sed -n 's/^.*[Ll]oaded image[^:]*: //p' <<<"$load_output" | tail -1)"
    [[ -n "$loaded" ]] || die "could not parse image name from: $load_output"
    podman tag "$loaded" "$tag"
else
    # Builds straight into podman's local storage; no load step needed.
    podman build \
        --file Containerfile.lowio \
        --build-arg "FEDIMINT_BUILD_FORCE_GIT_HASH=${commit}" \
        --tag "$tag" \
        .
fi

# Stable alias so the quadlet does not need editing after every rebuild.
podman tag "$tag" fedimintd:lowio

cat <<EOF

==> done

Image: $tag
       fedimintd:lowio   (stable alias, same image)

Quadlet:

    [Container]
    Image=localhost/fedimintd:lowio
    Pull=never
    Environment=FM_ROCKSDB_WAL_SYNC_INTERVAL_MS=5000

    systemctl --user daemon-reload
    systemctl --user restart <your-fedimintd>.service

Confirm the patch is active (this line appears only when it is):

    journalctl --user -u <your-fedimintd>.service | grep -i 'Durability relaxed'

Then measure:

    pidstat -d -C fedimintd 60 5
EOF
