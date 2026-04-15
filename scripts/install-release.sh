#!/usr/bin/env sh

set -eu

repo="CollectCall/jira-plans-teams-dc-to-dc-migrator"
bin_name="teams-migrator"
version="${VERSION:-latest}"
install_dir="${INSTALL_DIR:-$HOME/.local/bin}"

case "$(uname -s)" in
  Linux) os="linux" ;;
  Darwin) os="darwin" ;;
  *)
    echo "Unsupported operating system: $(uname -s)" >&2
    exit 1
    ;;
esac

case "$(uname -m)" in
  x86_64|amd64) arch="amd64" ;;
  aarch64|arm64) arch="arm64" ;;
  *)
    echo "Unsupported architecture: $(uname -m)" >&2
    exit 1
    ;;
esac

archive="teams-migrator_${version}_${os}_${arch}.tar.gz"
if [ "$version" = "latest" ]; then
  url="https://github.com/${repo}/releases/latest/download/${archive}"
else
  url="https://github.com/${repo}/releases/download/${version}/${archive}"
fi

tmpdir="$(mktemp -d)"
trap 'rm -rf "$tmpdir"' EXIT INT TERM

download_to="$tmpdir/$archive"

if command -v curl >/dev/null 2>&1; then
  curl -fsSL "$url" -o "$download_to"
elif command -v wget >/dev/null 2>&1; then
  wget -qO "$download_to" "$url"
else
  echo "curl or wget is required to download releases." >&2
  exit 1
fi

mkdir -p "$install_dir"
tar -xzf "$download_to" -C "$tmpdir"
install "$tmpdir/$bin_name" "$install_dir/$bin_name"

echo "Installed $bin_name to $install_dir/$bin_name"
case ":${PATH:-}:" in
  *":$install_dir:"*) ;;
  *)
    echo "Add $install_dir to PATH if needed:"
    echo "  export PATH=\"$install_dir:\$PATH\""
    ;;
esac
