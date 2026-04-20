#!/usr/bin/env sh

set -eu

repo="CollectCall/jira-advanced-roadmaps-teams-dc-to-dc-migrator"
bin_name="teams-migrator"
version="${VERSION:-latest}"
install_dir="${INSTALL_DIR:-$HOME/.local/bin}"
api_url="https://api.github.com/repos/${repo}/releases/latest"

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

tmpdir="$(mktemp -d)"
trap 'rm -rf "$tmpdir"' EXIT INT TERM

if command -v curl >/dev/null 2>&1; then
  downloader="curl"
elif command -v wget >/dev/null 2>&1; then
  downloader="wget"
else
  echo "curl or wget is required to download releases." >&2
  exit 1
fi

resolve_version() {
  if [ "$version" != "latest" ]; then
    printf '%s\n' "$version"
    return 0
  fi

  if [ "$downloader" = "curl" ]; then
    curl -fsSL "$api_url" | sed -n 's/.*"tag_name"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/p' | head -n 1
  else
    wget -qO- "$api_url" | sed -n 's/.*"tag_name"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/p' | head -n 1
  fi
}

choose_install_dir() {
  if [ -n "${INSTALL_DIR:-}" ]; then
    printf '%s\n' "$INSTALL_DIR"
    return 0
  fi

  old_ifs=$IFS
  IFS=:
  for dir in ${PATH:-}; do
    [ -n "$dir" ] || continue
    [ -d "$dir" ] || continue
    [ -w "$dir" ] || continue
    printf '%s\n' "$dir"
    IFS=$old_ifs
    return 0
  done
  IFS=$old_ifs

  printf '%s\n' "$install_dir"
}

resolved_version="$(resolve_version)"
if [ -z "$resolved_version" ]; then
  echo "Failed to determine the latest release version." >&2
  exit 1
fi

install_dir="$(choose_install_dir)"

archive="teams-migrator_${resolved_version}_${os}_${arch}.tar.gz"
download_to="$tmpdir/$archive"
url="https://github.com/${repo}/releases/download/${resolved_version}/${archive}"

if [ "$downloader" = "curl" ]; then
  curl -fsSL "$url" -o "$download_to"
else
  wget -qO "$download_to" "$url"
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
