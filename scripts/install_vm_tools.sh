#!/usr/bin/env bash
# QEMU needs a generic kernel and its modules, not desktop GPU firmware.
set -euo pipefail
apt-get -o Acquire::Retries=3 -o Acquire::http::Timeout=30 -o Acquire::https::Timeout=30 update
package=$(apt-cache search --names-only '^linux-image-[0-9].*-generic$' |
    awk '{print $1}' | sort -V | tail -1)
if [[ -z "$package" ]]; then
    echo 'No versioned generic kernel image is available' >&2
    exit 1
fi
apt-get -o Acquire::Retries=3 -o Acquire::http::Timeout=30 -o Acquire::https::Timeout=30 install --no-install-recommends -y \
    "$package" busybox-static qemu-system-x86 e2fsprogs kmod zstd
