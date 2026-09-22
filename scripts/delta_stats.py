"""Summarise a Delta table from its transaction log without starting Spark.

Reads ``_delta_log/*.json`` and sums the ``numRecords`` stats written on each
``add`` action. Run it inside a container that can see the table path:

    docker exec -i tickstream-spark python3 - /opt/tickstream/delta/raw_trades \\
        < scripts/delta_stats.py
"""

from __future__ import annotations

import json
import os
import sys


def summarize(path: str) -> dict:
    log_dir = os.path.join(path, "_delta_log")
    if not os.path.isdir(log_dir):
        return {"path": path, "error": "no _delta_log directory"}

    total_records = 0
    commits: list[int] = []
    for name in sorted(os.listdir(log_dir)):
        if not name.endswith(".json"):
            continue
        with open(os.path.join(log_dir, name), encoding="utf-8") as handle:
            for line in handle:
                line = line.strip()
                if not line:
                    continue
                action = json.loads(line)
                add = action.get("add")
                if not add:
                    continue
                stats = add.get("stats")
                if stats:
                    total_records += json.loads(stats).get("numRecords", 0)
                if add.get("modificationTime"):
                    commits.append(add["modificationTime"])

    result: dict = {
        "path": path,
        "total_records": total_records,
        "add_actions": len(commits),
    }
    if commits:
        commits.sort()
        span_seconds = (commits[-1] - commits[0]) / 1000
        result["first_ms"] = commits[0]
        result["last_ms"] = commits[-1]
        result["span_seconds"] = round(span_seconds, 3)
        if span_seconds > 0:
            result["records_per_sec"] = round(total_records / span_seconds, 1)
            result["commits_per_sec"] = round(len(commits) / span_seconds, 3)
    return result


if __name__ == "__main__":
    print(json.dumps(summarize(sys.argv[1]), indent=2))
