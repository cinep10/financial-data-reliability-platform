#!/usr/bin/env python3
import argparse
import os
import subprocess
import sys
from pathlib import Path


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", default=os.getenv("DB_HOST", "127.0.0.1"))
    ap.add_argument("--port", type=int, default=int(os.getenv("DB_PORT", "3306")))
    ap.add_argument("--user", default=os.getenv("DB_USER", "nethru"))
    ap.add_argument("--password", default=os.getenv("DB_PASSWORD", "nethru1234"))
    ap.add_argument("--db", default=os.getenv("DB_NAME", "weblog"))
    ap.add_argument("--profile-id", required=True)
    ap.add_argument("--dt-from", required=True)
    ap.add_argument("--dt-to", required=True)
    args = ap.parse_args()

    this_dir = Path(__file__).resolve().parent
    reasoner = this_dir / "llm_incident_reasoner.py"
    recommender = this_dir / "ai_action_recommender.py"

    common = [
        "--host", args.host,
        "--port", str(args.port),
        "--user", args.user,
        "--password", args.password,
        "--db", args.db,
        "--profile-id", args.profile_id,
        "--dt-from", args.dt_from,
        "--dt-to", args.dt_to,
    ]

    subprocess.check_call([sys.executable, str(reasoner)] + common)
    subprocess.check_call([sys.executable, str(recommender)] + common)

    print(
        f"[DONE] AI daily summary pipeline completed: "
        f"profile_id={args.profile_id}, dt_from={args.dt_from}, dt_to={args.dt_to}"
    )


if __name__ == "__main__":
    main()
