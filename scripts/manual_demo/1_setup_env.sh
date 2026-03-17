#!/usr/bin/env bash
set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/_common.sh"

if [[ -f ".env" ]]; then
  log ".env already exists. Leaving it unchanged."
else
  cp ".env.example" ".env"
  log "Created .env from .env.example."
fi

log "Environment variables are ready."
