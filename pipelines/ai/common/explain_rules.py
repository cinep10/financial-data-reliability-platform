from __future__ import annotations

from typing import Any, Dict


def _fmt_num(v: Any) -> str:
    if v is None:
        return "null"
    try:
        return f"{float(v):.4f}"
    except Exception:
        return str(v)


def explain_stream_issue(row: Dict[str, Any]) -> Dict[str, str]:
    primary_issue = (row.get("primary_stream_issue") or "").strip().lower()
    predicted_label = (row.get("predicted_label") or "").strip().lower()
    issue = primary_issue or predicted_label or "normal"

    missing_rate = row.get("missing_rate")
    duplicate_ratio = row.get("duplicate_ratio")
    ordering_gap_score = row.get("ordering_gap_score")
    avg_event_delay_ms = row.get("avg_event_delay_ms")
    predicted_risk = row.get("predicted_risk")

    if issue == "missing":
        return {
            "technical_reason": (
                f"missing_rate={_fmt_num(missing_rate)} 상승으로 completeness anomaly가 감지됨"
            ),
            "ops_reason": "consumer 처리 누락, adapter 누락, source event 유실 여부 점검 필요",
            "short_message": (
                f"missing 위험. missing_rate={_fmt_num(missing_rate)}, "
                f"predicted_risk={_fmt_num(predicted_risk)}"
            ),
        }

    if issue == "duplicate":
        return {
            "technical_reason": (
                f"duplicate_ratio={_fmt_num(duplicate_ratio)} 상승으로 duplicate anomaly 가능성이 높음"
            ),
            "ops_reason": "producer retry, duplicate send, idempotency, dedup 로직 점검 필요",
            "short_message": (
                f"duplicate 위험. duplicate_ratio={_fmt_num(duplicate_ratio)}, "
                f"predicted_risk={_fmt_num(predicted_risk)}"
            ),
        }

    if issue == "delay":
        return {
            "technical_reason": (
                f"avg_event_delay_ms={_fmt_num(avg_event_delay_ms)} 상승으로 latency anomaly가 감지됨"
            ),
            "ops_reason": "consumer lag, downstream 병목, 처리 지연, backpressure 여부 점검 필요",
            "short_message": (
                f"delay 위험. avg_event_delay_ms={_fmt_num(avg_event_delay_ms)}, "
                f"predicted_risk={_fmt_num(predicted_risk)}"
            ),
        }

    if issue == "ordering":
        return {
            "technical_reason": (
                f"ordering_gap_score={_fmt_num(ordering_gap_score)} 상승으로 ordering anomaly가 감지됨"
            ),
            "ops_reason": "partition key, event sequence, merge order, ordering 보장 로직 점검 필요",
            "short_message": (
                f"ordering 위험. ordering_gap_score={_fmt_num(ordering_gap_score)}, "
                f"predicted_risk={_fmt_num(predicted_risk)}"
            ),
        }

    return {
        "technical_reason": "주요 스트림 신호가 기준 범위 내에 있으며 뚜렷한 anomaly가 감지되지 않음",
        "ops_reason": "즉시 조치 필요성은 낮음. 모니터링 지속 권장",
        "short_message": f"normal 상태. predicted_risk={_fmt_num(predicted_risk)}",
    }


def recommend_actions(row: Dict[str, Any]) -> Dict[str, str]:
    primary_issue = (row.get("primary_stream_issue") or "").strip().lower()
    predicted_label = (row.get("predicted_label") or "").strip().lower()
    issue = primary_issue or predicted_label or "normal"

    if issue == "missing":
        return {
            "priority": "high",
            "action_type": "consumer_check",
            "action_message": "consumer offset, adapter 처리 누락, source event 유실 여부를 우선 점검",
        }
    if issue == "duplicate":
        return {
            "priority": "high",
            "action_type": "dedup_check",
            "action_message": "producer retry, duplicate publish, idempotency key, dedup 정책 확인",
        }
    if issue == "delay":
        return {
            "priority": "medium",
            "action_type": "latency_check",
            "action_message": "consumer lag, broker backlog, downstream 병목 및 처리 지연 확인",
        }
    if issue == "ordering":
        return {
            "priority": "medium",
            "action_type": "ordering_check",
            "action_message": "partition key, ordering 보장 로직, merge 순서 및 재처리 순서 확인",
        }

    return {
        "priority": "low",
        "action_type": "monitor",
        "action_message": "즉시 조치보다는 지속 모니터링 권장",
    }
