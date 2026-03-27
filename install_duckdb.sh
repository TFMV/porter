#!/usr/bin/env bash

set -euo pipefail

INSTALL_URL="https://dbc.columnar.tech/install.sh"
TARGET_DRIVER="duckdb"

echo "==> Installing dbc from Columnar..."

# Check for curl
if ! command -v curl >/dev/null 2>&1; then
  echo "Error: curl is required but not installed." >&2
  exit 1
fi

# Install dbc
curl -LsSf "$INSTALL_URL" | sh

# Verify dbc exists
if ! command -v dbc >/dev/null 2>&1; then
  echo "Error: dbc installation failed (binary not found in PATH)." >&2
  exit 1
fi

echo "==> Installing driver: $TARGET_DRIVER"
dbc install "$TARGET_DRIVER"

echo "==> Done."
echo "Installed drivers:"
dbc list || true
