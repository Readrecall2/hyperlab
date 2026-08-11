#!/usr/bin/env sh
set -eu
cd "$(dirname "$0")/.."
python -m pytest
python -m hyperlab doctor
python -m hyperlab demo --strategy all --hours 1200 --output reports/ci-demo
