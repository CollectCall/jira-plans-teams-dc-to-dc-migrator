#!/usr/bin/env bash

set -euo pipefail

if [[ "${OSTYPE:-}" != linux* ]]; then
  echo "This installer is intended for Linux." >&2
  echo "Use 'go build -o bin/teams-migrator ./cmd/teams-migrator' on other platforms." >&2
  exit 1
fi

if ! command -v go >/dev/null 2>&1; then
  echo "Go is required but was not found in PATH." >&2
  echo "Install Go 1.26 or newer, then rerun this script." >&2
  exit 1
fi

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
install_dir="${INSTALL_DIR:-$HOME/.local/bin}"
binary_path="$install_dir/teams-migrator"
build_cache_root="$(mktemp -d)"

cleanup() {
  rm -rf "$build_cache_root"
}

trap cleanup EXIT

mkdir -p "$install_dir"

echo "Building teams-migrator..."
(
  cd "$repo_root"
  GOCACHE="$build_cache_root/gocache" \
    GOPATH="$build_cache_root/gopath" \
    GOMODCACHE="$build_cache_root/gomodcache" \
    CGO_ENABLED=0 \
    go build -o "$binary_path" ./cmd/teams-migrator
)

chmod +x "$binary_path"

echo
echo "Installed: $binary_path"

case ":${PATH:-}:" in
  *":$install_dir:"*)
    echo "PATH already includes $install_dir"
    ;;
  *)
    echo "Add $install_dir to PATH to run 'teams-migrator' directly:"
    echo "  echo 'export PATH=\"$install_dir:\$PATH\"' >> ~/.bashrc"
    echo "  source ~/.bashrc"
    ;;
esac

echo
echo "First run:"
echo "  teams-migrator config init"
