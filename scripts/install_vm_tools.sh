#!/usr/bin/env bash
# QEMU needs a generic kernel and its modules, not desktop GPU firmware.
set -euo pipefail
apt-get -o Acquire::Retries=3 -o Acquire::http::Timeout=30 -o Acquire::https::Timeout=30 update
# Resolve the distribution's maintained generic track. Sorting every available
# image can select a large HWE kernel unrelated to this disposable guest.
# Install only its image dependency, not the firmware/modules-extra metapackage.
package=$(apt-cache depends linux-image-generic |
    awk '$1 == "Depends:" && $2 ~ /^linux-image-[0-9].*-generic$/ {print $2}')
if [[ -z "$package" || "$package" == *$'\n'* ]]; then
    echo 'Expected one versioned image dependency of linux-image-generic' >&2
    exit 1
fi
apt-get -o Acquire::Retries=3 -o Acquire::http::Timeout=30 -o Acquire::https::Timeout=30 install --no-install-recommends -y \
    "$package" busybox-static qemu-system-x86 e2fsprogs kmod zstd
