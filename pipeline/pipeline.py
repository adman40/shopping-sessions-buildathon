#!/usr/bin/env python3
"""Shopping Sessions reference-oriented Polars implementation.

The parser deliberately implements the track's small CSV dialect instead of
relying on a CSV reader whose malformed-line and CRLF behavior differs from the
rules. Polars handles the sorting and aggregations; attribution uses a linear
per-person sweep so it does not materialize a sessions-by-clicks range join.
"""

from __future__ import annotations

import glob
import heapq
import json
import os
import re
import sys
import time
from collections import Counter, defaultdict, deque
from datetime import datetime, timezone
from pathlib import Path

NCPU = os.environ.get("NCPU")
if NCPU:
    # Must be set before polars is imported.
    os.environ.setdefault("POLARS_MAX_THREADS", NCPU)

import polars as pl  # noqa: E402

DATA = os.environ.get("DATA_DIR", "/data")
OUT = os.environ.get("OUTPUT_DIR", "/output")

DAY_MS = 86_400_000
WEEK_MS = 7 * DAY_MS
SESSION_GAP_MS = 1_800_000
LATE_MS = DAY_MS
EPOCH = datetime(1970, 1, 1)
ISO_TS = re.compile(
    r"^([0-9]{4})-([0-9]{2})-([0-9]{2})T([0-9]{2}):([0-9]{2}):([0-9]{2})"
    r"(Z|([+-])([0-9]{2}):([0-9]{2}))$"
)
ASCII_DIGITS = re.compile(r"^[0-9]+$")

USER_DAILY = {
    "user_id": pl.String,
    "day": pl.Date,
    "sessions": pl.Int64,
    "events": pl.Int64,
    "revenue_cents": pl.Int64,
    "top_category": pl.String,
}
CAMPAIGN_DAILY = {
    "campaign_id": pl.String,
    "channel": pl.String,
    "day": pl.Date,
    "sessions": pl.Int64,
    "purchasing_sessions": pl.Int64,
    "revenue_cents": pl.Int64,
    "assisted_sessions": pl.Int64,
}
EVENT_SCHEMA = {
    "person": pl.String,
    "ts": pl.Int64,
    "event_type": pl.String,
    "category": pl.String,
    "price": pl.Int64,
}
SESSION_SCHEMA = {
    "session_id": pl.Int64,
    "person": pl.String,
    "session_start": pl.Int64,
    "events": pl.Int64,
    "revenue_cents": pl.Int64,
    "purchasing": pl.Boolean,
    "day": pl.Date,
}

_started = time.perf_counter()
_lap = _started


def phase(name: str) -> None:
    """Emit concise timing diagnostics without writing to stdout."""
    global _lap
    now = time.perf_counter()
    print(
        f"[timing] {name}: {now - _lap:.3f}s (total {now - _started:.3f}s)",
        file=sys.stderr,
        flush=True,
    )
    _lap = now


def parse_fields(raw: bytes, width: int) -> tuple[str, ...] | None:
    """Parse one newline-free record using the exact restricted CSV dialect."""
    if not raw:
        return None
    pieces = raw.split(b",")
    if len(pieces) != width:
        return None
    values: list[str] = []
    for piece in pieces:
        if piece.startswith(b'"'):
            if len(piece) < 2 or not piece.endswith(b'"') or b'"' in piece[1:-1]:
                return None
            piece = piece[1:-1]
        elif b'"' in piece:
            return None
        try:
            values.append(piece.decode("utf-8"))
        except UnicodeDecodeError:
            return None
    return tuple(values)


def csv_rows(paths: list[str], width: int):
    """Yield valid records, skipping the first physical line of every file."""
    for path in paths:
        with open(path, "rb") as handle:
            next(handle, None)
            for line in handle:
                # Binary iteration splits only on LF and therefore preserves CR.
                if line.endswith(b"\n"):
                    line = line[:-1]
                row = parse_fields(line, width)
                if row is not None:
                    yield row


def timestamp_ms(value: str) -> int | None:
    """Parse F1/F2/F3 exactly, returning UTC epoch milliseconds."""
    if ASCII_DIGITS.fullmatch(value):
        return int(value)
    match = ISO_TS.fullmatch(value)
    if match is None:
        return None
    year, month, day, hour, minute, second = map(int, match.group(1, 2, 3, 4, 5, 6))
    zone, sign, offset_hour, offset_minute = match.group(7, 8, 9, 10)
    if zone != "Z":
        oh, om = int(offset_hour), int(offset_minute)
        if oh > 23 or om > 59:
            return None
    else:
        oh = om = 0
    try:
        local = datetime(year, month, day, hour, minute, second)
    except ValueError:
        return None
    seconds = int((local - EPOCH).total_seconds())
    offset = (oh * 60 + om) * 60
    if zone != "Z" and sign == "+":
        seconds -= offset
    elif zone != "Z":
        seconds += offset
    return seconds * 1000


def manifest_timestamp(value: str) -> int:
    parsed = datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
    return int(parsed.timestamp()) * 1000


class Components:
    """Union-find whose root is always the byte-wise smallest member."""

    def __init__(self) -> None:
        self.parent: dict[str, str] = {}

    def find(self, value: str) -> str:
        parent = self.parent
        root = value
        while parent[root] != root:
            root = parent[root]
        while parent[value] != value:
            following = parent[value]
            parent[value] = root
            value = following
        return root

    def union(self, left: str, right: str) -> None:
        for value in (left, right):
            if value not in self.parent:
                self.parent[value] = value
        a, b = self.find(left), self.find(right)
        if a != b:
            small, large = (a, b) if a < b else (b, a)
            self.parent[large] = small

    def canonical(self, value: str) -> str:
        return self.find(value) if value in self.parent else value


def read_dimension(path: str) -> dict[str, str]:
    rows = csv_rows([path], 2)
    return {key: value for key, value in rows}


def build_sessions(
    events: list[tuple[str, int, str, str, int | None]],
) -> tuple[pl.DataFrame, pl.DataFrame]:
    if not events:
        return pl.DataFrame(schema=SESSION_SCHEMA), pl.DataFrame(schema=USER_DAILY)

    frame = pl.DataFrame(events, schema=EVENT_SCHEMA, orient="row").sort(
        ["person", "ts"]
    )
    frame = frame.with_columns(
        (
            (pl.col("person") != pl.col("person").shift(1))
            | ((pl.col("ts") - pl.col("ts").shift(1)) > SESSION_GAP_MS)
        )
        .fill_null(True)
        .cast(pl.Int64)
        .cum_sum()
        .alias("session_id")
    )
    sessions = (
        frame.group_by("session_id", maintain_order=True)
        .agg(
            pl.col("person").first(),
            pl.col("ts").min().alias("session_start"),
            pl.len().cast(pl.Int64).alias("events"),
            pl.col("price").fill_null(0).sum().cast(pl.Int64).alias("revenue_cents"),
            pl.col("price").is_not_null().any().alias("purchasing"),
        )
        .with_columns(
            pl.from_epoch("session_start", time_unit="ms").dt.date().alias("day")
        )
        .cast(SESSION_SCHEMA)
    )

    user_daily = (
        sessions.group_by(["person", "day"])
        .agg(
            pl.len().cast(pl.Int64).alias("sessions"),
            pl.col("events").sum().cast(pl.Int64),
            pl.col("revenue_cents").sum().cast(pl.Int64),
        )
        .rename({"person": "user_id"})
    )
    category_winners = (
        frame.filter(pl.col("price").is_not_null())
        .join(sessions.select("session_id", "day"), on="session_id", how="left")
        .group_by(["person", "day", "category"])
        .agg(pl.col("price").sum().alias("category_revenue"))
        .sort(
            ["person", "day", "category_revenue", "category"],
            descending=[False, False, True, False],
        )
        .unique(["person", "day"], keep="first", maintain_order=True)
        .select(
            pl.col("person").alias("user_id"),
            "day",
            pl.col("category").alias("top_category"),
        )
    )
    user_daily = (
        user_daily.join(category_winners, on=["user_id", "day"], how="left")
        .select(list(USER_DAILY))
        .cast(USER_DAILY)
    )
    return sessions, user_daily


def campaign_output(
    sessions: pl.DataFrame,
    clicks_by_person: dict[str, list[tuple[int, str, str]]],
    campaigns: dict[str, str],
) -> pl.DataFrame:
    if sessions.is_empty():
        return pl.DataFrame(schema=CAMPAIGN_DAILY)

    sessions_by_person: dict[str, list[dict[str, object]]] = defaultdict(list)
    for row in sessions.sort(["person", "session_start"]).iter_rows(named=True):
        sessions_by_person[row["person"]].append(row)

    # Value order: credited sessions, purchasing sessions, revenue, assists.
    totals: dict[tuple[str, object], list[int]] = {}

    def bucket(campaign: str, day: object) -> list[int]:
        return totals.setdefault((campaign, day), [0, 0, 0, 0])

    for person, person_sessions in sessions_by_person.items():
        clicks = sorted(
            clicks_by_person.get(person, ()), key=lambda row: (row[0], row[1])
        )
        active: deque[tuple[int, str, str]] = deque()
        campaign_counts: Counter[str] = Counter()
        # (-timestamp, click_id, campaign) chooses latest ts then smallest click_id.
        credit_heap: list[tuple[int, str, str]] = []
        cursor = 0
        for session in person_sessions:
            start = int(session["session_start"])
            cutoff = start - WEEK_MS
            while cursor < len(clicks) and clicks[cursor][0] <= start:
                click = clicks[cursor]
                active.append(click)
                campaign_counts[click[2]] += 1
                heapq.heappush(credit_heap, (-click[0], click[1], click[2]))
                cursor += 1
            while active and active[0][0] < cutoff:
                expired = active.popleft()
                campaign_counts[expired[2]] -= 1
                if campaign_counts[expired[2]] == 0:
                    del campaign_counts[expired[2]]
            while credit_heap and -credit_heap[0][0] < cutoff:
                heapq.heappop(credit_heap)

            credited = credit_heap[0][2] if credit_heap else "organic"
            day = session["day"]
            values = bucket(credited, day)
            values[0] += 1
            values[1] += int(bool(session["purchasing"]))
            values[2] += int(session["revenue_cents"])
            for assisting in campaign_counts:
                bucket(assisting, day)[3] += 1

    rows = [
        (
            campaign,
            "organic" if campaign == "organic" else campaigns[campaign],
            day,
            values[0],
            values[1],
            values[2],
            values[3],
        )
        for (campaign, day), values in totals.items()
    ]
    return pl.DataFrame(rows, schema=CAMPAIGN_DAILY, orient="row").cast(CAMPAIGN_DAILY)


def main() -> None:
    with open(os.path.join(DATA, "manifest.json"), encoding="utf-8") as handle:
        manifest = json.load(handle)
    window_start = manifest_timestamp(manifest["window_start"])
    window_end = manifest_timestamp(manifest["window_end"])

    event_files = sorted(glob.glob(os.path.join(DATA, "events", "*.csv")))
    click_files = sorted(glob.glob(os.path.join(DATA, "clicks", "*.csv")))
    link_files = sorted(glob.glob(os.path.join(DATA, "links", "*.csv")))
    products = read_dimension(os.path.join(DATA, "products.csv"))
    campaigns = read_dimension(os.path.join(DATA, "campaigns.csv"))

    components = Components()
    for left, right in csv_rows(link_files, 2):
        if left and right:
            components.union(left, right)
    phase("manifest, dimensions, and links")

    # Deduplication intentionally precedes all event validation.
    event_copies: dict[str, tuple[int, tuple[str, ...]]] = {}
    for row in csv_rows(event_files, 7):
        event_id = row[0]
        if not event_id:
            continue
        ingest = timestamp_ms(row[3])
        if ingest is None:
            # ingest_ts is guaranteed valid F1, but do not let unexpected dirt crash.
            continue
        previous = event_copies.get(event_id)
        if previous is None or ingest < previous[0]:
            event_copies[event_id] = (ingest, row)

    events: list[tuple[str, int, str, str, int | None]] = []
    for ingest, row in event_copies.values():
        _, user, raw_ts, _, event_type, product, raw_price = row
        if not user or event_type not in ("view", "cart", "purchase") or not product:
            continue
        ts = timestamp_ms(raw_ts)
        if ts is None or ts < window_start or ts >= window_end or ingest - ts > LATE_MS:
            continue
        price: int | None = None
        if event_type == "purchase":
            if not ASCII_DIGITS.fullmatch(raw_price):
                continue
            price = int(raw_price)
            if price < 1 or price > 10_000_000:
                continue
        events.append(
            (
                components.canonical(user),
                ts,
                event_type,
                products.get(product, "unknown"),
                price,
            )
        )
    del event_copies
    phase(f"events parsed and validated ({len(events)} surviving)")

    sessions, user_daily = build_sessions(events)
    del events
    phase(f"events sessionized ({sessions.height} sessions)")

    click_ids: dict[str, tuple[str, ...]] = {}
    for row in csv_rows(click_files, 4):
        if row[0] and row[0] not in click_ids:
            click_ids[row[0]] = row
    clicks_by_person: dict[str, list[tuple[int, str, str]]] = defaultdict(list)
    click_window_start = window_start - WEEK_MS
    for click_id, row in click_ids.items():
        _, user, campaign, raw_ts = row
        if not user or campaign not in campaigns:
            continue
        ts = timestamp_ms(raw_ts)
        if ts is None or ts < click_window_start or ts >= window_end:
            continue
        clicks_by_person[components.canonical(user)].append((ts, click_id, campaign))
    del click_ids
    phase("clicks parsed and validated")

    campaign_daily = campaign_output(sessions, clicks_by_person, campaigns)
    phase("sessions attributed and aggregated")

    for name, frame in (("user_daily", user_daily), ("campaign_daily", campaign_daily)):
        folder = Path(OUT, name)
        folder.mkdir(parents=True, exist_ok=True)
        for old in folder.glob("*.parquet"):
            old.unlink()
        frame.write_parquet(folder / "part-0.parquet")
    phase("outputs written")


if __name__ == "__main__":
    main()
