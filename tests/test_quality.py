from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from quantlog.ingest.adapters import QuantBuildEmitter
from quantlog.quality.service import score_run
from quantlog.events.io import discover_jsonl_files


class TestQuality(unittest.TestCase):
    def test_score_run_passes_for_clean_dataset(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            qb = QuantBuildEmitter.from_base_path(
                base_path=root,
                environment="paper",
                run_id="run_quality_test",
                session_id="session_quality_test",
            )
            qb.emit(
                event_type="signal_evaluated",
                trace_id="trace_quality_1",
                timestamp_utc="2026-03-29T18:00:00Z",
                payload={
                    "signal_type": "ict_sweep",
                    "signal_direction": "LONG",
                    "confidence": 0.7,
                },
            )
            qb.emit(
                event_type="risk_guard_decision",
                trace_id="trace_quality_1",
                timestamp_utc="2026-03-29T18:00:01Z",
                payload={"guard_name": "spread_guard", "decision": "BLOCK", "reason": "spread"},
            )
            qb.emit(
                event_type="trade_action",
                trace_id="trace_quality_1",
                timestamp_utc="2026-03-29T18:00:02Z",
                payload={"decision": "NO_ACTION", "reason": "blocked"},
            )

            report = score_run(root, max_gap_seconds=300, pass_threshold=95)
            self.assertTrue(report.passed)
            self.assertGreaterEqual(report.score, 95)
            self.assertEqual(report.errors_total, 0)
            self.assertEqual(report.duplicate_event_ids, 0)

    def test_score_run_fails_when_anomalies_are_present(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            generated_root = root / "generated"
            date = "2026-03-29"

            import subprocess
            import sys

            cmd = [
                sys.executable,
                "scripts/generate_sample_day.py",
                "--output-path",
                str(generated_root),
                "--date",
                date,
                "--traces",
                "15",
                "--inject-anomalies",
            ]
            completed = subprocess.run(cmd, check=False)
            self.assertEqual(completed.returncode, 0)

            day_path = generated_root / date
            self.assertTrue(len(discover_jsonl_files(day_path)) > 0)
            report = score_run(day_path, max_gap_seconds=300, pass_threshold=95)
            self.assertFalse(report.passed)
            self.assertGreater(report.duplicate_event_ids, 0)
            self.assertGreater(report.missing_trace_ids, 0)


if __name__ == "__main__":
    unittest.main()

