"""QuantLog event validation."""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any
from uuid import UUID

from quantlog.events.io import RawEventLine, discover_jsonl_files, iter_jsonl_file
from quantlog.events.schema import (
    ALLOWED_ENVIRONMENTS,
    ALLOWED_SEVERITIES,
    ALLOWED_SOURCE_SYSTEMS,
    CLOSEST_TO_ENTRY_SIDES,
    COMBO_MODULE_LABELS,
    EVENT_PAYLOAD_REQUIRED,
    GATE_SUMMARY_GATE_KEYS,
    GATE_SUMMARY_STATUSES,
    NO_ACTION_REASONS_ALLOWED,
    REQUIRED_ENVELOPE_FIELDS,
    RISK_GUARD_DECISIONS,
    TRADE_ACTION_DECISIONS,
    TRADE_EXECUTED_DIRECTIONS,
    DECISION_CHAIN_EVENT_TYPES,
)


@dataclass(slots=True, frozen=True)
class ValidationIssue:
    level: str  # error|warn
    path: Path
    line_number: int
    message: str


@dataclass(slots=True, frozen=True)
class ValidationReport:
    files_scanned: int
    lines_scanned: int
    events_valid: int
    issues: list[ValidationIssue]


def validation_issue_code(message: str) -> str:
    """Stable bucket for aggregating validation messages (ops / CI summaries)."""
    if ": " in message:
        return message.split(": ", 1)[0].strip()
    return message


def aggregate_validation_issue_codes(issues: list[ValidationIssue]) -> dict[str, int]:
    counts: Counter[str] = Counter()
    for issue in issues:
        counts[validation_issue_code(issue.message)] += 1
    return dict(counts)


def _is_utc_iso8601(value: Any) -> bool:
    if not isinstance(value, str):
        return False
    try:
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
        return dt.tzinfo is not None
    except ValueError:
        return False


def _validate_uuid(value: Any) -> bool:
    if not isinstance(value, str):
        return False
    try:
        UUID(value)
        return True
    except ValueError:
        return False


def _num_in_closed_unit_interval(value: Any) -> bool:
    if isinstance(value, bool):
        return False
    if isinstance(value, (int, float)):
        return 0.0 <= float(value) <= 1.0
    return False


def _non_negative_int_not_bool(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool) and value >= 0


def _signal_evaluated_optional_issues(payload: dict[str, Any]) -> list[tuple[str, str]]:
    """Return (level, message) tuples for optional `signal_evaluated` desk-grade fields."""
    rows: list[tuple[str, str]] = []

    gs = payload.get("gate_summary")
    if gs is not None:
        if not isinstance(gs, dict):
            rows.append(("error", "signal_evaluated_invalid_gate_summary_not_object"))
        else:
            for gate_key, gate_val in gs.items():
                if gate_key not in GATE_SUMMARY_GATE_KEYS:
                    rows.append(("warn", f"signal_evaluated_unknown_gate_summary_key: {gate_key!r}"))
                else:
                    if (
                        not isinstance(gate_val, str)
                        or gate_val not in GATE_SUMMARY_STATUSES
                    ):
                        rows.append(
                            (
                                "error",
                                f"signal_evaluated_invalid_gate_summary_status[{gate_key}]: {gate_val!r}",
                            )
                        )

    def _blocking_gate(field: str) -> None:
        if field not in payload:
            return
        val = payload[field]
        if val is None:
            return
        if not isinstance(val, str) or val not in GATE_SUMMARY_GATE_KEYS:
            rows.append(("error", f"signal_evaluated_invalid_{field}: {val!r}"))

    _blocking_gate("blocked_by_primary_gate")
    _blocking_gate("blocked_by_secondary_gate")

    ep = payload.get("evaluation_path")
    if ep is not None:
        if not isinstance(ep, list):
            rows.append(("error", "signal_evaluated_invalid_evaluation_path_not_array"))
        else:
            for idx, seg in enumerate(ep):
                if not isinstance(seg, str):
                    rows.append(
                        (
                            "error",
                            f"signal_evaluated_invalid_evaluation_path_segment[{idx}]: {seg!r}",
                        )
                    )
                elif seg not in GATE_SUMMARY_GATE_KEYS:
                    rows.append(
                        ("warn", f"signal_evaluated_unknown_evaluation_path_gate: {seg!r}")
                    )

    if "new_bar_detected" in payload and not isinstance(payload["new_bar_detected"], bool):
        rows.append(("error", f"signal_evaluated_invalid_new_bar_detected: {payload['new_bar_detected']!r}"))
    if "same_bar_guard_triggered" in payload and not isinstance(
        payload["same_bar_guard_triggered"], bool
    ):
        rows.append(
            (
                "error",
                f"signal_evaluated_invalid_same_bar_guard_triggered: {payload['same_bar_guard_triggered']!r}",
            )
        )

    sk = payload.get("same_bar_skip_count_for_bar")
    if sk is not None and not _non_negative_int_not_bool(sk):
        rows.append(("error", f"signal_evaluated_invalid_same_bar_skip_count_for_bar: {sk!r}"))

    for ts_key in ("bar_ts", "poll_ts"):
        if ts_key in payload and payload[ts_key] is not None:
            if not _is_utc_iso8601(payload[ts_key]):
                rows.append(("error", f"signal_evaluated_invalid_{ts_key}"))

    ns = payload.get("near_entry_score")
    if ns is not None and not _num_in_closed_unit_interval(ns):
        rows.append(("error", f"signal_evaluated_invalid_near_entry_score: {ns!r}"))

    for k in ("combo_active_modules_count_long", "combo_active_modules_count_short", "active_modules_count_long", "active_modules_count_short"):
        if k in payload and payload[k] is not None:
            v = payload[k]
            if not _non_negative_int_not_bool(v):
                rows.append(("error", f"signal_evaluated_invalid_{k}: {v!r}"))

    for k in ("entry_distance_long", "entry_distance_short"):
        if k in payload and payload[k] is not None:
            v = payload[k]
            if not _non_negative_int_not_bool(v):
                rows.append(("error", f"signal_evaluated_invalid_{k}: {v!r}"))

    ces = payload.get("closest_to_entry_side")
    if ces is not None and (not isinstance(ces, str) or ces not in CLOSEST_TO_ENTRY_SIDES):
        rows.append(("error", f"signal_evaluated_invalid_closest_to_entry_side: {ces!r}"))

    for side_key in ("missing_modules_long", "missing_modules_short"):
        arr = payload.get(side_key)
        if arr is None:
            continue
        if not isinstance(arr, list):
            rows.append(("error", f"signal_evaluated_invalid_{side_key}_not_array"))
            continue
        for item in arr:
            if not isinstance(item, str) or item not in COMBO_MODULE_LABELS:
                rows.append(("error", f"signal_evaluated_invalid_{side_key}_label: {item!r}"))

    for mod_key in ("modules_long", "modules_short"):
        mobj = payload.get(mod_key)
        if mobj is None:
            continue
        if not isinstance(mobj, dict):
            rows.append(("error", f"signal_evaluated_invalid_{mod_key}_not_object"))
            continue
        for mk, mv in mobj.items():
            if mk not in COMBO_MODULE_LABELS:
                rows.append(("warn", f"signal_evaluated_unknown_module_key[{mod_key}]: {mk!r}"))
            if not isinstance(mv, bool):
                rows.append(("error", f"signal_evaluated_invalid_{mod_key}[{mk}]: {mv!r}"))

    for bkey in ("setup_candidate", "entry_ready"):
        if bkey in payload and not isinstance(payload[bkey], bool):
            rows.append(("error", f"signal_evaluated_invalid_{bkey}: {payload[bkey]!r}"))

    cs = payload.get("candidate_strength")
    if cs is not None and not _num_in_closed_unit_interval(cs):
        rows.append(("error", f"signal_evaluated_invalid_candidate_strength: {cs!r}"))

    tsnap = payload.get("threshold_snapshot")
    if tsnap is not None and not isinstance(tsnap, dict):
        rows.append(("error", "signal_evaluated_invalid_threshold_snapshot_not_object"))

    return rows


def validate_raw_event(raw_line: RawEventLine) -> list[ValidationIssue]:
    issues: list[ValidationIssue] = []
    if raw_line.parsed is None:
        issues.append(
            ValidationIssue(
                level="error",
                path=raw_line.path,
                line_number=raw_line.line_number,
                message=f"invalid_json: {raw_line.parse_error}",
            )
        )
        return issues

    event = raw_line.parsed
    missing = REQUIRED_ENVELOPE_FIELDS - set(event.keys())
    for field_name in sorted(missing):
        issues.append(
            ValidationIssue(
                level="error",
                path=raw_line.path,
                line_number=raw_line.line_number,
                message=f"missing_required_field: {field_name}",
            )
        )

    if "event_id" in event and not _validate_uuid(event["event_id"]):
        issues.append(
            ValidationIssue(
                level="error",
                path=raw_line.path,
                line_number=raw_line.line_number,
                message="invalid_event_id_uuid",
            )
        )

    if "timestamp_utc" in event and not _is_utc_iso8601(event["timestamp_utc"]):
        issues.append(
            ValidationIssue(
                level="error",
                path=raw_line.path,
                line_number=raw_line.line_number,
                message="invalid_timestamp_utc",
            )
        )

    if "ingested_at_utc" in event and not _is_utc_iso8601(event["ingested_at_utc"]):
        issues.append(
            ValidationIssue(
                level="error",
                path=raw_line.path,
                line_number=raw_line.line_number,
                message="invalid_ingested_at_utc",
            )
        )
    elif "timestamp_utc" in event and "ingested_at_utc" in event:
        ts_dt = datetime.fromisoformat(str(event["timestamp_utc"]).replace("Z", "+00:00"))
        ingest_dt = datetime.fromisoformat(str(event["ingested_at_utc"]).replace("Z", "+00:00"))
        if ingest_dt < ts_dt:
            issues.append(
                ValidationIssue(
                    level="warn",
                    path=raw_line.path,
                    line_number=raw_line.line_number,
                    message="ingested_before_event_timestamp",
                )
            )

    source_system = event.get("source_system")
    if source_system is not None and source_system not in ALLOWED_SOURCE_SYSTEMS:
        issues.append(
            ValidationIssue(
                level="error",
                path=raw_line.path,
                line_number=raw_line.line_number,
                message=f"invalid_source_system: {source_system}",
            )
        )

    severity = event.get("severity")
    if severity is not None and severity not in ALLOWED_SEVERITIES:
        issues.append(
            ValidationIssue(
                level="error",
                path=raw_line.path,
                line_number=raw_line.line_number,
                message=f"invalid_severity: {severity}",
            )
        )

    environment = event.get("environment")
    if environment is not None and environment not in ALLOWED_ENVIRONMENTS:
        issues.append(
            ValidationIssue(
                level="error",
                path=raw_line.path,
                line_number=raw_line.line_number,
                message=f"invalid_environment: {environment}",
            )
        )

    source_seq = event.get("source_seq")
    if source_seq is not None and (not isinstance(source_seq, int) or source_seq < 1):
        issues.append(
            ValidationIssue(
                level="error",
                path=raw_line.path,
                line_number=raw_line.line_number,
                message="invalid_source_seq",
            )
        )

    # Required correlation fields: key may be present with JSON null — still invalid.
    for text_field in ("run_id", "session_id", "trace_id"):
        if text_field not in event:
            continue
        value = event[text_field]
        if not isinstance(value, str) or not value.strip():
            issues.append(
                ValidationIssue(
                    level="error",
                    path=raw_line.path,
                    line_number=raw_line.line_number,
                    message=f"invalid_{text_field}",
                )
            )

    ss = event.get("source_system")
    et_chain = event.get("event_type")
    if ss == "quantbuild" and et_chain in DECISION_CHAIN_EVENT_TYPES:
        dcid = event.get("decision_cycle_id")
        if not isinstance(dcid, str) or not dcid.strip():
            issues.append(
                ValidationIssue(
                    level="error",
                    path=raw_line.path,
                    line_number=raw_line.line_number,
                    message="missing_decision_cycle_id_quantbuild_chain",
                )
            )

    payload = event.get("payload")
    if payload is not None and not isinstance(payload, dict):
        issues.append(
            ValidationIssue(
                level="error",
                path=raw_line.path,
                line_number=raw_line.line_number,
                message="payload_not_object",
            )
        )
        return issues

    event_type = event.get("event_type")
    required_payload = EVENT_PAYLOAD_REQUIRED.get(event_type)
    if required_payload is None:
        issues.append(
            ValidationIssue(
                level="warn",
                path=raw_line.path,
                line_number=raw_line.line_number,
                message=f"unknown_event_type: {event_type}",
            )
        )
    elif isinstance(payload, dict):
        missing_payload_fields = required_payload - set(payload.keys())
        for field_name in sorted(missing_payload_fields):
            issues.append(
                ValidationIssue(
                    level="error",
                    path=raw_line.path,
                    line_number=raw_line.line_number,
                    message=f"missing_payload_field[{event_type}]: {field_name}",
                )
                )

    if event_type == "signal_evaluated" and isinstance(payload, dict):
        for level, msg in _signal_evaluated_optional_issues(payload):
            issues.append(
                ValidationIssue(
                    level=level,
                    path=raw_line.path,
                    line_number=raw_line.line_number,
                    message=msg,
                )
            )

    if event_type == "trade_action" and isinstance(payload, dict):
        decision = str(payload.get("decision", "")).upper()
        if decision not in TRADE_ACTION_DECISIONS:
            issues.append(
                ValidationIssue(
                    level="error",
                    path=raw_line.path,
                    line_number=raw_line.line_number,
                    message=f"invalid_trade_action_decision: {decision}",
                )
            )
        elif decision == "NO_ACTION" and "reason" in payload:
            reason = payload["reason"]
            if not isinstance(reason, str) or reason not in NO_ACTION_REASONS_ALLOWED:
                issues.append(
                    ValidationIssue(
                        level="error",
                        path=raw_line.path,
                        line_number=raw_line.line_number,
                        message=f"invalid_no_action_reason: {reason!r}",
                    )
                )
        elif decision == "ENTER":
            tid = payload.get("trade_id")
            if not isinstance(tid, str) or not tid.strip():
                issues.append(
                    ValidationIssue(
                        level="error",
                        path=raw_line.path,
                        line_number=raw_line.line_number,
                        message="trade_action_enter_missing_trade_id",
                    )
                )

    if event_type == "risk_guard_decision" and isinstance(payload, dict):
        decision = str(payload.get("decision", "")).upper()
        if decision not in RISK_GUARD_DECISIONS:
            issues.append(
                ValidationIssue(
                    level="error",
                    path=raw_line.path,
                    line_number=raw_line.line_number,
                    message=f"invalid_risk_guard_decision: {decision}",
                )
            )

    if event_type == "signal_filtered" and isinstance(payload, dict):
        fr = payload.get("filter_reason")
        if not isinstance(fr, str) or fr not in NO_ACTION_REASONS_ALLOWED:
            issues.append(
                ValidationIssue(
                    level="error",
                    path=raw_line.path,
                    line_number=raw_line.line_number,
                    message=f"invalid_signal_filtered_reason: {fr!r}",
                )
            )

    if event_type == "trade_executed" and isinstance(payload, dict):
        direction = str(payload.get("direction", "")).upper()
        if direction not in TRADE_EXECUTED_DIRECTIONS:
            issues.append(
                ValidationIssue(
                    level="error",
                    path=raw_line.path,
                    line_number=raw_line.line_number,
                    message=f"invalid_trade_executed_direction: {direction}",
                )
            )

    if event_type in {"order_submitted", "order_filled", "order_rejected"} and not event.get(
        "order_ref"
    ):
        issues.append(
            ValidationIssue(
                level="warn",
                path=raw_line.path,
                line_number=raw_line.line_number,
                message="execution_event_missing_order_ref",
            )
        )

    if event_type == "trade_executed" and not event.get("order_ref"):
        issues.append(
            ValidationIssue(
                level="warn",
                path=raw_line.path,
                line_number=raw_line.line_number,
                message="trade_executed_missing_order_ref",
            )
        )

    if event_type == "governance_state_changed" and not event.get("account_id"):
        issues.append(
            ValidationIssue(
                level="warn",
                path=raw_line.path,
                line_number=raw_line.line_number,
                message="governance_event_missing_account_id",
            )
        )

    return issues


def _monotonic_source_seq_issues(
    raw_line: RawEventLine, seq_last: dict[str, int]
) -> list[ValidationIssue]:
    """Enforce strictly increasing source_seq per emitter stream within one JSONL file."""
    issues: list[ValidationIssue] = []
    if raw_line.parsed is None:
        return issues
    event = raw_line.parsed
    sq = event.get("source_seq")
    if not isinstance(sq, int) or sq < 1:
        return issues
    run_id = event.get("run_id")
    session_id = event.get("session_id")
    source_system = event.get("source_system")
    source_component = event.get("source_component")
    if not (
        isinstance(run_id, str)
        and run_id.strip()
        and isinstance(session_id, str)
        and session_id.strip()
        and isinstance(source_system, str)
        and source_system.strip()
        and isinstance(source_component, str)
        and source_component.strip()
    ):
        return issues
    key = f"{source_system.strip()}|{source_component.strip()}|{run_id.strip()}|{session_id.strip()}"
    prev = seq_last.get(key)
    if prev is not None and sq <= prev:
        issues.append(
            ValidationIssue(
                level="error",
                path=raw_line.path,
                line_number=raw_line.line_number,
                message=f"source_seq_not_monotonic: stream={key!r} prev={prev} current={sq}",
            )
        )
    else:
        seq_last[key] = sq
    return issues


def validate_path(path: Path) -> ValidationReport:
    jsonl_files = discover_jsonl_files(path)
    issues: list[ValidationIssue] = []
    lines_scanned = 0
    events_valid = 0

    for jsonl_path in jsonl_files:
        seq_last: dict[str, int] = {}
        for raw_line in iter_jsonl_file(jsonl_path):
            lines_scanned += 1
            event_issues = validate_raw_event(raw_line)
            mono_issues = _monotonic_source_seq_issues(raw_line, seq_last)
            combined = event_issues + mono_issues
            issues.extend(combined)
            if not any(issue.level == "error" for issue in combined):
                events_valid += 1

    return ValidationReport(
        files_scanned=len(jsonl_files),
        lines_scanned=lines_scanned,
        events_valid=events_valid,
        issues=issues,
    )

