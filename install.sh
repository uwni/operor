#!/bin/sh
# Operor installer — downloads a prebuilt binary from GitHub Releases.
#
# Usage:
#   curl -fsSL https://raw.githubusercontent.com/uwni/operor/main/install.sh | sh
#   curl -fsSL https://raw.githubusercontent.com/uwni/operor/main/install.sh | sh -s -- --to ~/.local/bin
#   curl -fsSL https://raw.githubusercontent.com/uwni/operor/main/install.sh | sh -s -- --version v0.1.0

set -eu

REPO="uwni/operor"
BIN_NAME="operor"
INSTALL_DIR=""
VERSION=""

usage() {
    cat <<EOF
Operor installer

Usage:
    install.sh [OPTIONS]

Options:
    --to <dir>        Install directory (default: ~/.local/bin or /usr/local/bin)
    --version <tag>   Install a specific release tag (default: latest)
    -h, --help        Show this help
EOF
}

err() {
    printf "error: %s\n" "$1" >&2
    exit 1
}

# ---------------------------------------------------------------------------
# Parse arguments
# ---------------------------------------------------------------------------
while [ $# -gt 0 ]; do
    case "$1" in
        --to)
            shift
            [ $# -eq 0 ] && err "--to requires a directory argument"
            INSTALL_DIR="$1"
            ;;
        --version)
            shift
            [ $# -eq 0 ] && err "--version requires a tag argument"
            VERSION="$1"
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        *)
            err "unknown option: $1"
            ;;
    esac
    shift
done

# ---------------------------------------------------------------------------
# Detect platform
# ---------------------------------------------------------------------------
detect_target() {
    os="$(uname -s)"
    arch="$(uname -m)"

    case "$os" in
        Linux*)  os_part="linux-musl" ;;
        Darwin*) os_part="macos" ;;
        *)       err "unsupported OS: $os (use Linux or macOS)" ;;
    esac

    case "$arch" in
        x86_64|amd64)   arch_part="x86_64" ;;
        aarch64|arm64)   arch_part="aarch64" ;;
        *)               err "unsupported architecture: $arch" ;;
    esac

    echo "${arch_part}-${os_part}"
}

# ---------------------------------------------------------------------------
# Pick a download tool
# ---------------------------------------------------------------------------
fetch() {
    url="$1"
    output="$2"
    if command -v curl >/dev/null 2>&1; then
        curl -fsSL -o "$output" "$url"
    elif command -v wget >/dev/null 2>&1; then
        wget -qO "$output" "$url"
    else
        err "neither curl nor wget found; install one and retry"
    fi
}

fetch_stdout() {
    url="$1"
    if command -v curl >/dev/null 2>&1; then
        curl -fsSL "$url"
    elif command -v wget >/dev/null 2>&1; then
        wget -qO- "$url"
    else
        err "neither curl nor wget found; install one and retry"
    fi
}

# ---------------------------------------------------------------------------
# Resolve latest version if not specified
# ---------------------------------------------------------------------------
if [ -z "$VERSION" ]; then
    VERSION="$(fetch_stdout "https://api.github.com/repos/${REPO}/releases/latest" \
        | grep '"tag_name"' | head -1 | sed 's/.*"tag_name": *"//;s/".*//')"
    [ -z "$VERSION" ] && err "could not determine latest release; pass --version explicitly"
fi

# ---------------------------------------------------------------------------
# Resolve install directory
# ---------------------------------------------------------------------------
if [ -z "$INSTALL_DIR" ]; then
    if [ -d "$HOME/.local/bin" ]; then
        INSTALL_DIR="$HOME/.local/bin"
    elif [ -w "/usr/local/bin" ]; then
        INSTALL_DIR="/usr/local/bin"
    else
        INSTALL_DIR="$HOME/.local/bin"
    fi
fi

TARGET="$(detect_target)"
ARCHIVE="${BIN_NAME}-${VERSION}-${TARGET}.zip"
URL="https://github.com/${REPO}/releases/download/${VERSION}/${ARCHIVE}"

printf "  target:  %s\n" "$TARGET"
printf "  version: %s\n" "$VERSION"
printf "  url:     %s\n" "$URL"
printf "  dest:    %s\n" "$INSTALL_DIR"
echo

# ---------------------------------------------------------------------------
# Download, extract, install
# ---------------------------------------------------------------------------
TMP_DIR="$(mktemp -d)"
trap 'rm -rf "$TMP_DIR"' EXIT

printf "Downloading %s ...\n" "$ARCHIVE"
fetch "$URL" "${TMP_DIR}/${ARCHIVE}"

if ! command -v unzip >/dev/null 2>&1; then
    err "unzip is required to extract the archive"
fi

unzip -qo "${TMP_DIR}/${ARCHIVE}" -d "${TMP_DIR}/extract"

mkdir -p "$INSTALL_DIR"
cp "${TMP_DIR}/extract/${BIN_NAME}" "${INSTALL_DIR}/${BIN_NAME}"
chmod +x "${INSTALL_DIR}/${BIN_NAME}"

# Remove macOS quarantine flag (unsigned binary would be blocked by Gatekeeper)
case "$(uname -s)" in
    Darwin*) xattr -d com.apple.quarantine "${INSTALL_DIR}/${BIN_NAME}" 2>/dev/null || true ;;
esac

printf "\n✓ Installed %s to %s/%s\n" "$VERSION" "$INSTALL_DIR" "$BIN_NAME"

# ---------------------------------------------------------------------------
# PATH hint
# ---------------------------------------------------------------------------
case ":${PATH}:" in
    *":${INSTALL_DIR}:"*) ;;
    *)
        echo
        echo "NOTE: ${INSTALL_DIR} is not in your PATH."
        echo "Add it with:"
        echo
        echo "  export PATH=\"${INSTALL_DIR}:\$PATH\""
        echo
        ;;
esac
