#!/usr/bin/env bash
# Run the Spark test suite inside the running spark container, so no local JDK
# or pyspark install is needed. Usage:
#
#   ./scripts/test_spark.sh                      # transform tests
#   ./scripts/test_spark.sh tests/unit           # whole unit suite
set -euo pipefail

SERVICE="${SPARK_SERVICE:-spark}"
CONTAINER="${SPARK_CONTAINER:-tickstream-spark}"
TARGET="${*:-tests/unit/test_transforms.py}"

docker cp tests "${CONTAINER}:/app/tests" >/dev/null
docker compose exec -T -u root "${SERVICE}" python3 -m pip install --quiet pytest >/dev/null

PY4J=$(docker compose exec -T "${SERVICE}" sh -c 'ls /opt/spark/python/lib/py4j-*-src.zip' | tr -d '\r')

docker compose exec -T -u root -w /app \
  -e "PYTHONPATH=/app/src:/opt/spark/python:${PY4J}" \
  "${SERVICE}" python3 -m pytest ${TARGET}
