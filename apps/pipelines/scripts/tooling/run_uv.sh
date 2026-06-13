#!/usr/bin/env bash
set -euo pipefail

if command -v uv >/dev/null 2>&1; then
    UV_BIN="$(command -v uv)"
elif [ -n "${HOME:-}" ] && [ -x "${HOME}/.local/bin/uv" ]; then
    UV_BIN="${HOME}/.local/bin/uv"
elif [ -n "${HOME:-}" ] && [ -x "${HOME}/.cargo/bin/uv" ]; then
    UV_BIN="${HOME}/.cargo/bin/uv"
elif [ -x /opt/homebrew/bin/uv ]; then
    UV_BIN=/opt/homebrew/bin/uv
elif [ -x /usr/local/bin/uv ]; then
    UV_BIN=/usr/local/bin/uv
else
    echo "Executable uv not found. Install uv or add it to PATH for Git/pre-commit." >&2
    exit 127
fi

REPO_ROOT="$(git rev-parse --show-toplevel)"
export UV_CACHE_DIR="${UV_CACHE_DIR:-${REPO_ROOT}/apps/pipelines/.uv-cache}"

exec "${UV_BIN}" run --project "${REPO_ROOT}/apps/pipelines" "$@"
