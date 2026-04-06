from __future__ import annotations

import argparse
import json
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta
from pathlib import Path

@dataclass
class ScenarioSpec:
    scenario_name: str
    scenario_type: str
    traffic_multiplier: float
    login_multiplier: float
    loan_view_multiplier: float
    loan_start_multiplier: float
    card_start_multiplier: float
    missing_rate_bump: float
    note: str

SCENARIOS = {
    "campaign": ScenarioSpec("campaign_push", "campaign", 1.35, 1.20, 1.45, 1.35, 1.25, 0.00, "marketing uplift; top funnel expands"),
    "holiday": ScenarioSpec("holiday_quiet", "holiday", 0.70, 0.75, 0.65, 0.60, 0.60, 0.00, "holiday traffic drop; lower conversion opportunity"),
    "weather": ScenarioSpec("weather_shock", "weather", 0.82, 0.90, 0.95, 0.90, 0.90, 0.01, "weather related traffic shift"),
    "system_issue": ScenarioSpec("system_issue", "system_status", 0.88, 0.92, 0.80, 0.72, 0.72, 0.05, "service degradation; missing rate up, funnel down"),
}

def daterange(start: str, end: str):
    cur = datetime.strptime(start, "%Y-%m-%d").date()
    end_dt = datetime.strptime(end, "%Y-%m-%d").date()
    while cur <= end_dt:
        yield cur.isoformat()
        cur += timedelta(days=1)

def main():
    ap = argparse.ArgumentParser(description="Generate scenario plan and execution guide for simulator experiments")
    ap.add_argument("--scenario", choices=list(SCENARIOS.keys()), required=True)
    ap.add_argument("--dt-from", required=True)
    ap.add_argument("--dt-to", required=True)
    ap.add_argument("--profile-id", default="finance_bank")
    ap.add_argument("--out-dir", default="scenario_runs")
    args = ap.parse_args()

    spec = SCENARIOS[args.scenario]
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    plan = {
        "profile_id": args.profile_id,
        "dt_from": args.dt_from,
        "dt_to": args.dt_to,
        "scenario": asdict(spec),
        "dates": list(daterange(args.dt_from, args.dt_to)),
        "recommended_steps": [
            "1. Run simulator with scenario-specific profile/config",
            "2. Run pre-ML backfill pipeline",
            "3. Run ML backfill pipeline",
            "4. Compare scenario_experiment_result_day vs baseline period",
        ],
        "example_command": f"bash ./deploy/run_full_backfill_pipeline.sh {args.dt_from} {args.dt_to} {args.profile_id}",
    }

    (out_dir / f"{args.scenario}_plan.json").write_text(json.dumps(plan, ensure_ascii=False, indent=2), encoding="utf-8")

    profile_patch = {
        "scenario_name": spec.scenario_name,
        "scenario_type": spec.scenario_type,
        "traffic_multiplier": spec.traffic_multiplier,
        "login_multiplier": spec.login_multiplier,
        "loan_view_multiplier": spec.loan_view_multiplier,
        "loan_start_multiplier": spec.loan_start_multiplier,
        "card_start_multiplier": spec.card_start_multiplier,
        "missing_rate_bump": spec.missing_rate_bump,
        "note": spec.note,
    }
    (out_dir / f"{args.scenario}_profile_patch.json").write_text(json.dumps(profile_patch, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"[OK] scenario plan written: {out_dir}/{args.scenario}_plan.json")
    print(f"[OK] profile patch written: {out_dir}/{args.scenario}_profile_patch.json")

if __name__ == "__main__":
    main()
