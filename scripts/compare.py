#!/usr/bin/env python3
"""Exact, order-insensitive diff of two output folders."""
import argparse
from pathlib import Path
import sys

from run_fixtures import NULLABLE_COLUMNS, SCHEMAS, load_actual, load_expected


def load_output(folder, table):
    return load_actual(folder / table, SCHEMAS[table], NULLABLE_COLUMNS[table])


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--reference-right", action="store_true")
    parser.add_argument("left", type=Path)
    parser.add_argument("right", type=Path)
    args = parser.parse_args()

    matched = True
    for table, schema in SCHEMAS.items():
        a = load_output(args.left, table)
        if args.reference_right:
            b = load_expected(args.right / f"{table}.csv", schema)
        else:
            b = load_output(args.right, table)
        keys = [column for column in ("user_id", "campaign_id", "day") if column in schema]
        duplicate_a = a.height - a.n_unique(subset=keys)
        duplicate_b = b.height - b.n_unique(subset=keys)
        only_a = a.join(b, on=list(schema), how="anti", nulls_equal=True)
        only_b = b.join(a, on=list(schema), how="anti", nulls_equal=True)
        print(f"{table}: rows {a.height} vs {b.height}; only in A {only_a.height}; "
              f"only in B {only_b.height}; duplicate keys {duplicate_a} vs {duplicate_b}")
        if duplicate_a or duplicate_b or only_a.height or only_b.height:
            matched = False
            print(only_a.sort(keys).head(5))
            print(only_b.sort(keys).head(5))
    print("MATCH" if matched else "DIFFER")
    return 0 if matched else 1


if __name__ == "__main__":
    sys.exit(main())
