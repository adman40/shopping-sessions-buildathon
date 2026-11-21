#!/usr/bin/env python3
"""Behavioral tests for output value-nullability validation."""

from datetime import date
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

import polars as pl

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "scripts"))

from run_fixtures import NULLABLE_COLUMNS, SCHEMAS, load_actual


class FixtureSchemaContracts(unittest.TestCase):
    @staticmethod
    def user_daily_frame(sessions):
        return pl.DataFrame(
            {
                "user_id": pl.Series(["u1"], dtype=pl.String),
                "day": pl.Series([date(2026, 10, 3)], dtype=pl.Date),
                "sessions": pl.Series([sessions], dtype=pl.Int64),
                "events": pl.Series([1], dtype=pl.Int64),
                "revenue_cents": pl.Series([0], dtype=pl.Int64),
                "top_category": pl.Series([None], dtype=pl.String),
            }
        )

    def load_user_daily(self, frame):
        with tempfile.TemporaryDirectory() as temporary:
            folder = Path(temporary) / "user_daily"
            folder.mkdir()
            frame.write_parquet(folder / "part-0.parquet")
            return load_actual(
                folder,
                SCHEMAS["user_daily"],
                NULLABLE_COLUMNS["user_daily"],
            )

    def test_top_category_may_be_null(self):
        actual = self.load_user_daily(self.user_daily_frame(sessions=1))
        self.assertEqual(actual["top_category"].to_list(), [None])

    def test_required_column_may_not_be_null(self):
        with self.assertRaisesRegex(ValueError, "sessions contains null values"):
            self.load_user_daily(self.user_daily_frame(sessions=None))

    def test_candidate_debug_csv_cannot_override_parquet(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            candidate = root / "candidate"
            reference = root / "reference"
            candidate.mkdir()
            reference.mkdir()

            csv_tables = {
                "user_daily": (
                    "user_id,day,sessions,events,revenue_cents,top_category\n"
                    "u1,2026-10-03,1,1,0,\n"
                ),
                "campaign_daily": (
                    "campaign_id,channel,day,sessions,purchasing_sessions,"
                    "revenue_cents,assisted_sessions\n"
                    "organic,organic,2026-10-03,1,0,0,0\n"
                ),
            }
            for table, content in csv_tables.items():
                (candidate / f"{table}.csv").write_text(content, encoding="utf-8")
                (reference / f"{table}.csv").write_text(content, encoding="utf-8")

            user_folder = candidate / "user_daily"
            user_folder.mkdir()
            self.user_daily_frame(sessions=2).write_parquet(
                user_folder / "part-0.parquet"
            )
            campaign_folder = candidate / "campaign_daily"
            campaign_folder.mkdir()
            pl.DataFrame(
                {
                    "campaign_id": pl.Series(["organic"], dtype=pl.String),
                    "channel": pl.Series(["organic"], dtype=pl.String),
                    "day": pl.Series([date(2026, 10, 3)], dtype=pl.Date),
                    "sessions": pl.Series([2], dtype=pl.Int64),
                    "purchasing_sessions": pl.Series([0], dtype=pl.Int64),
                    "revenue_cents": pl.Series([0], dtype=pl.Int64),
                    "assisted_sessions": pl.Series([0], dtype=pl.Int64),
                }
            ).write_parquet(campaign_folder / "part-0.parquet")

            process = subprocess.run(
                [
                    sys.executable,
                    str(ROOT / "scripts" / "compare.py"),
                    "--reference-right",
                    str(candidate),
                    str(reference),
                ],
                capture_output=True,
                text=True,
            )
        self.assertEqual(process.returncode, 1, process.stderr)
        self.assertIn("DIFFER", process.stdout)


if __name__ == "__main__":
    unittest.main()
