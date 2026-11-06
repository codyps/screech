#!/usr/bin/env bash
# Meta packages can reference a kernel-tools build not yet present on a mirror.
# Select a concrete tools package from the downloaded package descriptions.
set -euo pipefail
apt-get -o Acquire::Retries=3 -o Acquire::http::Timeout=30 -o Acquire::https::Timeout=30 update
package=$(apt-cache search --names-only '^linux-tools-[0-9].*-generic$' |
    awk '{print $1}' | sort -V | tail -1)
if [[ -z "$package" ]]; then
    echo 'No versioned generic kernel tools package is available' >&2
    exit 1
fi
apt-get -o Acquire::Retries=3 -o Acquire::http::Timeout=30 -o Acquire::https::Timeout=30 install -y "$package" strace
