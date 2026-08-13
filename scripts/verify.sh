#!/usr/bin/env sh
set -eu
cd "$(dirname "$0")/.."
python scripts/verify_release.py --auto --check-manifest
python -m ruff check .
python -m mypy src/hyperlab
python -m pytest --cov=hyperlab --cov-report=term-missing
python -m hyperlab doctor
python -m hyperlab demo --strategy all --hours 1200 --output reports/ci-demo
