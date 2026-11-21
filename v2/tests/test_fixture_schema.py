#!/usr/bin/env python3
"""Behavioral tests for output value-nullability validation."""

from datetime import date
from pathlib import Path
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


if __name__ == "__main__":
    unittest.main()
