#!/usr/bin/env python3
import csv
import os
from datetime import datetime, timezone
from pathlib import Path

OUT = Path("out/demo.csv")

def main():
    OUT.parent.mkdir(parents=True, exist_ok=True)

    now = datetime.now(timezone.utc)
    # simple changing number so you can see it update
    base = (now.hour * 60 + now.minute) / 10.0 + 100

    row = [
        now.isoformat(),
        round(base, 2),
        round(base + 0.2, 2),
        round(base - 0.1, 2),
        round(base + 0.05, 2),
        os.environ.get("GITHUB_RUN_NUMBER", ""),
        os.environ.get("GITHUB_SHA", "")[:7],
    ]

    is_new = not OUT.exists()
    with OUT.open("a", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        if is_new:
            w.writerow(["ts_utc", "open", "high", "low", "close", "run_number", "sha"])
        w.writerow(row)

    print("Wrote:", row)

if __name__ == "__main__":
    main()
