#!/usr/bin/env bash
# Helper script to bootstrap a Python virtual environment for dbt development.

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
VENV_DIR="$ROOT_DIR/.venv"

if [[ ! -f "$ROOT_DIR/requirements.txt" ]]; then
  echo "requirements.txt not found at $ROOT_DIR. Aborting."
  exit 1
fi

PYTHON_BIN="${PYTHON_BIN:-python3}"

if [[ ! -x "$(command -v "$PYTHON_BIN")" ]]; then
  echo "Python executable '$PYTHON_BIN' not found. Set PYTHON_BIN to a valid interpreter."
  exit 1
fi

echo "Creating virtual environment at $VENV_DIR"
"$PYTHON_BIN" -m venv "$VENV_DIR"

echo "Activating virtual environment and installing dependencies"
# shellcheck disable=SC1090
source "$VENV_DIR/bin/activate"
pip install --upgrade pip
pip install -r "$ROOT_DIR/requirements.txt"

echo ""
echo "Virtual environment ready."
echo "Activate it with: source \"$VENV_DIR/bin/activate\""
echo "Remember to populate .env using scripts/create_env.py if you have not already."


