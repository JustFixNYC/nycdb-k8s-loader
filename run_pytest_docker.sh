#!/usr/bin/env bash
set -euo pipefail

# Runs pytest inside the docker-compose app service.
# Usage:
#   ./run_pytest_docker.sh
#   ./run_pytest_docker.sh tests/test_wowutil.py -q

test -f .env || cp .env.example .env

docker-compose up -d db
docker-compose run --rm app pytest "$@"
