#!/usr/bin/env bash
# Agent test runner: quiet stdout, artifacts in test-results/.
#
# Default: WOW-scoped tests (mypy, pyflakes, wowutil, parse_created_tables,
# goodcause, oca, signature). Full NYCDB dataset suite is slow — use only
# for pre-merge:
#   FULL=1 ./scripts/run_tests_for_agent.sh
#   ./scripts/run_tests_for_agent.sh --full
#
# Ad-hoc scope:
#   ./scripts/run_tests_for_agent.sh tests/test_wowutil.py
set -euo pipefail

OUT_DIR="${OUT_DIR:-test-results}"
mkdir -p "$OUT_DIR"

test -f .env || cp .env.example .env

DEFAULT_TARGETS=(
  tests/test_mypy.py
  tests/test_pyflakes.py
  tests/test_parse_created_tables.py
  tests/test_wowutil.py
  tests/test_goodcauseutil.py
  tests/test_ocautil.py
  tests/test_signatureutil.py
)

PYTEST_ARGS=()
if [[ "${FULL:-}" == "1" ]] || [[ "${1:-}" == "--full" ]]; then
  if [[ "${1:-}" == "--full" ]]; then
    shift
  fi
  PYTEST_ARGS=("$@")
elif [[ $# -gt 0 ]]; then
  PYTEST_ARGS=("$@")
else
  PYTEST_ARGS=("${DEFAULT_TARGETS[@]}")
fi

docker-compose up -d db >/dev/null 2>&1

set +e
docker-compose run --rm app pytest "${PYTEST_ARGS[@]}" \
  --tb=short --no-header -ra \
  --junitxml="/app/$OUT_DIR/junit.xml" \
  > "$OUT_DIR/latest.log" 2>&1
EXIT=$?
set -e

python3 scripts/summarize_junit.py "$OUT_DIR/junit.xml" \
  --runner pytest --log "$OUT_DIR/latest.log" \
  --out-dir "$OUT_DIR" || true

echo "nycdb-k8s-loader: wrote $OUT_DIR/summary.md (exit $EXIT)"
exit $EXIT
