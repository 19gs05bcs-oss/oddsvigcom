#!/usr/bin/env python3
"""
fetchday.py ciktisini Supabase `fixture` tablosuna yazar.

Env:
  SUPABASE_URL=https://xxxx.supabase.co
  SUPABASE_KEY=service_role_secret

  python3 import_fixture_supabase.py day_0.json.gz
  python3 import_fixture_supabase.py day_0.json.gz --batch-size 10
"""
from __future__ import annotations

import argparse
import gzip
import json
import os
import sys
from datetime import datetime, timezone
from typing import Any

from flashscore_markets import kickoff_iso
from import_season_supabase import upsert_adaptive


def load_day(path: str) -> dict:
    opener = gzip.open if path.endswith(".gz") else open
    with opener(path, "rt", encoding="utf-8") as f:
        return json.load(f)


def now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _int_or_none(v: Any) -> int | None:
    if v is None or v == "":
        return None
    try:
        return int(v)
    except (TypeError, ValueError):
        return None


def row_from_match(
    m: dict[str, Any],
    *,
    bulletin_date: str,
    day_offset: int,
    source: str,
    bookmakers: dict[str, str],
    scraped_at: str,
) -> dict[str, Any]:
    odds = m.get("odds") or []
    if not isinstance(odds, list):
        odds = []
    kickoff_ts = _int_or_none(m.get("kickoff_ts"))
    kickoff_at = kickoff_iso(m.get("kickoff_ts"))
    return {
        "match_id": str(m["match_id"]),
        "source": source,
        "bulletin_date": bulletin_date,
        "day_offset": int(day_offset),
        "league": m.get("league") or None,
        "league_country": m.get("league_country") or None,
        "kickoff_ts": kickoff_ts,
        "kickoff_at": kickoff_at,
        "home_name": m.get("home_name") or None,
        "away_name": m.get("away_name") or None,
        "home_id": m.get("home_id") or None,
        "away_id": m.get("away_id") or None,
        "home_slug": m.get("home_slug") or None,
        "away_slug": m.get("away_slug") or None,
        "home_score": _int_or_none(m.get("home_score")),
        "away_score": _int_or_none(m.get("away_score")),
        "home_ht_score": _int_or_none(m.get("home_ht_score")),
        "away_ht_score": _int_or_none(m.get("away_ht_score")),
        "match_url": m.get("match_url") or None,
        "odds": odds,
        "bookmakers": bookmakers or {},
        "odds_count": len(odds),
        "scraped_at": scraped_at,
        "updated_at": scraped_at,
    }


def rows_from_payload(data: dict[str, Any]) -> tuple[list[dict[str, Any]], str, int]:
    matches = data.get("matches") or []
    bookmakers = {str(k): str(v) for k, v in (data.get("bookmakers") or {}).items()}
    bulletin_date = data.get("bulletin_date") or datetime.now(timezone.utc).strftime("%Y-%m-%d")
    day_offset = int(data.get("day_offset") or 0)
    source = str(data.get("source") or "flashscore")
    scraped_at = data.get("generated_at") or now_iso()
    rows = [
        row_from_match(
            m,
            bulletin_date=bulletin_date,
            day_offset=day_offset,
            source=source,
            bookmakers=bookmakers,
            scraped_at=scraped_at,
        )
        for m in matches
        if m.get("match_id")
    ]
    return rows, bulletin_date, day_offset


def upsert_day_payload(
    data: dict[str, Any],
    url: str,
    key: str,
    *,
    batch_size: int = 15,
) -> int:
    """Payload → fixture upsert. Donen: yazilan satir sayisi."""
    rows, bulletin_date, day_offset = rows_from_payload(data)
    print(
        f"[+] {len(rows)} fixture  date={bulletin_date}  offset={day_offset}",
        file=sys.stderr,
    )
    batch = max(1, batch_size)
    for i in range(0, len(rows), batch):
        chunk = rows[i : i + batch]
        print(f"  upsert {i + 1}-{i + len(chunk)} / {len(rows)}", file=sys.stderr)
        upsert_adaptive(url, key, "fixture", chunk, "match_id")
    print("[+] supabase ok", file=sys.stderr)
    return len(rows)


def main() -> int:
    ap = argparse.ArgumentParser(description="Gunluk bulten → Supabase fixture")
    ap.add_argument("input", help="fetchday .json / .json.gz")
    ap.add_argument("--batch-size", type=int, default=15)
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    url = (os.environ.get("SUPABASE_URL") or "").strip()
    key = (os.environ.get("SUPABASE_KEY") or "").strip()
    if not args.dry_run and (not url or not key):
        print("SUPABASE_URL ve SUPABASE_KEY gerekli", file=sys.stderr)
        return 2

    data = load_day(args.input)
    if args.dry_run:
        rows, bulletin_date, day_offset = rows_from_payload(data)
        print(
            f"[+] {len(rows)} fixture  date={bulletin_date}  offset={day_offset}",
            file=sys.stderr,
        )
        print(json.dumps(rows[:1], ensure_ascii=False, indent=2)[:2000])
        return 0

    upsert_day_payload(data, url, key, batch_size=args.batch_size)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
