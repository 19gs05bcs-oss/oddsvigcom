#!/usr/bin/env python3
"""
Flashscore gunluk bulten (f_1_{offset}_...) + her mac icin odds → Supabase `fixture`.

offset=0 ≈ bugun (UTC gun sinirina yakin). Compact odds (schema_version=2).
Varsayilan: dogrudan Supabase'e yazar (git'e json koyma).

Env:
  SUPABASE_URL=https://xxxx.supabase.co
  SUPABASE_KEY=service_role_secret

  python3 fetchday.py --offset 0
  python3 fetchday.py --offset 0 --limit 5
  python3 fetchday.py --offset 0 -o day_0.json.gz          # ekstra yerel yedek
  python3 fetchday.py --offset 0 --skip-supabase -o day.json.gz
"""
from __future__ import annotations

import argparse
import os
import sys
import time
from collections import Counter
from datetime import datetime, timezone
from typing import Any

import requests

from fetchseason import (
    FEED_HEADERS,
    fetch_odds_for_match,
    write_output,
)
from flashscore_markets import kickoff_iso
from import_fixture_supabase import row_from_match
from import_season_supabase import upsert_adaptive

DAY_FEED = "https://5.flashscore.ninja/5/x/feed/f_1_{offset}_3_en-uk_1"


def fetch_day_feed(offset: int) -> str:
    # fetchseason ile ayni: bare requests.get (SESSION yok)
    url = DAY_FEED.format(offset=int(offset))
    r = requests.get(url, headers=FEED_HEADERS, timeout=60)
    r.raise_for_status()
    return r.text


def upsert_fixtures(
    payload: dict[str, Any],
    url: str,
    key: str,
    *,
    batch_size: int = 15,
) -> int:
    """Payload → Supabase fixture (eski import_fixture API: row_from_match)."""
    matches = payload.get("matches") or []
    bookmakers = {str(k): str(v) for k, v in (payload.get("bookmakers") or {}).items()}
    bulletin_date = payload.get("bulletin_date") or datetime.now(timezone.utc).strftime("%Y-%m-%d")
    day_offset = int(payload.get("day_offset") or 0)
    source = str(payload.get("source") or "flashscore")
    scraped_at = payload.get("generated_at") or datetime.now(timezone.utc).strftime(
        "%Y-%m-%dT%H:%M:%SZ"
    )
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


def _fields(rec: str) -> dict[str, str]:
    out: dict[str, str] = {}
    for part in rec.split("¬"):
        if "÷" not in part:
            continue
        k, v = part.split("÷", 1)
        out[k] = v
    return out


def parse_day_feed(raw: str) -> list[dict[str, Any]]:
    """Gunluk feed → mac listesi (odds bos)."""
    matches: list[dict[str, Any]] = []
    country = ""
    league_name = ""

    for rec in raw.split("~"):
        f = _fields(rec)
        if not f:
            continue

        if "ZA" in f or ("ZL" in f and "AA" not in f):
            country = (f.get("ZA") or country or "").strip()
            league_name = (f.get("ZL") or league_name or "").strip()
            continue

        mid = (f.get("AA") or "").strip()
        if not mid:
            continue

        home = (f.get("AE") or "").strip()
        away = (f.get("AF") or "").strip()
        # Season feed ile ayni: JA/JB = takim id (odds participant).
        # PX/PY gunluk feed'de baska alan; H/A side token bozuluyordu.
        home_id = (f.get("JA") or f.get("PX") or "").strip()
        away_id = (f.get("JB") or f.get("PY") or "").strip()
        home_slug = (f.get("WU") or "").strip()
        away_slug = (f.get("WV") or "").strip()
        kickoff = (f.get("AD") or "").strip()
        home_score = (f.get("AG") or "").strip()
        away_score = (f.get("AH") or "").strip()
        # AG/AH=FT, BA/BB=1Y (nadiren), BC/BD=2Y → 1Y = BA/BB veya FT-2Y
        home_ht_score, away_ht_score = "", ""
        try:
            ba, bb = (f.get("BA") or "").strip(), (f.get("BB") or "").strip()
            if ba != "" and bb != "":
                home_ht_score, away_ht_score = str(int(ba)), str(int(bb))
            else:
                if home_score != "" and (f.get("BC") or "").strip() != "":
                    h = int(home_score) - int(str(f.get("BC")).strip())
                    if h >= 0:
                        home_ht_score = str(h)
                if away_score != "" and (f.get("BD") or "").strip() != "":
                    a = int(away_score) - int(str(f.get("BD")).strip())
                    if a >= 0:
                        away_ht_score = str(a)
        except ValueError:
            home_ht_score, away_ht_score = "", ""

        # AN=y → odds tab acik; MW = oran veren bookmaker id'leri (ope2 fallback).
        odds_flag = (f.get("AN") or "").strip().lower()
        odds_bookmaker_ids = (f.get("MW") or "").strip()

        c = country
        lg = league_name
        if c and lg:
            league = f"{c}: {lg}"
        else:
            league = lg or c or ""

        match_url = ""
        if home_slug and away_slug and home_id and away_id:
            match_url = (
                f"https://www.flashscore.co.uk/match/football/"
                f"{away_slug}-{away_id}/{home_slug}-{home_id}/"
                f"odds/1x2/full-time/?mid={mid}"
            )

        matches.append({
            "match_id": mid,
            "league": league,
            "league_country": c,
            "kickoff_ts": kickoff,
            "home_name": home,
            "away_name": away,
            "home_id": home_id,
            "away_id": away_id,
            "home_slug": home_slug,
            "away_slug": away_slug,
            "home_score": home_score,
            "away_score": away_score,
            "home_ht_score": home_ht_score,
            "away_ht_score": away_ht_score,
            "odds_flag": odds_flag,
            "odds_bookmaker_ids": odds_bookmaker_ids,
            "match_url": match_url,
            "odds": [],
        })

    return matches


def bulletin_date_from_matches(matches: list[dict[str, Any]]) -> str:
    days: list[str] = []
    for m in matches:
        iso = kickoff_iso(m.get("kickoff_ts"))
        if iso:
            days.append(iso[:10])
    if days:
        return Counter(days).most_common(1)[0][0]
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def main() -> int:
    ap = argparse.ArgumentParser(description="Flashscore gunluk bulten + odds → Supabase")
    ap.add_argument("--offset", type=int, default=0, help="0=bugun, 1=yarin, ...")
    ap.add_argument(
        "-o",
        "--out",
        "--output",
        dest="output",
        default="",
        help="Yerel .json / .json.gz (Actions: --out)",
    )
    ap.add_argument("--limit", type=int, default=0, help="Max mac (0=hepsi)")
    ap.add_argument("--delay", type=float, default=0.12, help="Odds istekleri arasi sn")
    ap.add_argument("--batch-size", type=int, default=15, help="Supabase upsert batch")
    ap.add_argument(
        "--skip-supabase",
        action="store_true",
        help="Supabase'e yazma",
    )
    args = ap.parse_args()

    url = (os.environ.get("SUPABASE_URL") or "").strip()
    key = (
        (os.environ.get("SUPABASE_KEY") or "").strip()
        or (os.environ.get("SUPABASE_SERVICE_KEY") or "").strip()
    )
    do_supabase = not args.skip_supabase
    if do_supabase and (not url or not key):
        print(
            "[!] SUPABASE_URL + SUPABASE_KEY (service_role) gerekli. "
            "Actions step env'ye secrets.SUPABASE_URL / secrets.SUPABASE_KEY ekle; "
            "veya gecici: --skip-supabase --out ...",
            file=sys.stderr,
        )
        return 2
    if not do_supabase and not args.output:
        print("--skip-supabase ile --out gerekli", file=sys.stderr)
        return 2

    print(f"[+] Day feed offset={args.offset}", file=sys.stderr)
    raw = fetch_day_feed(args.offset)
    matches = parse_day_feed(raw)
    if args.limit and args.limit > 0:
        matches = matches[: args.limit]
    print(f"[+] {len(matches)} mac listelendi", file=sys.stderr)

    bookmakers: dict[str, str] = {}
    errors: list[dict[str, str]] = []
    no_odds = 0

    for i, m in enumerate(matches, 1):
        mid = m["match_id"]
        label = f"{m.get('home_name')}–{m.get('away_name')}"
        mw = m.get("odds_bookmaker_ids") or ""
        flag = m.get("odds_flag") or ""
        print(
            f"  [{i}/{len(matches)}] {mid} {label}"
            + (f"  AN={flag}" if flag else "")
            + (f"  MW={mw[:40]}" if mw else ""),
            file=sys.stderr,
        )
        result, err = fetch_odds_for_match(m)
        if err:
            errors.append({"match_id": mid, "error": err})
            print(f"    [!] {err}", file=sys.stderr)
        elif result is None:
            no_odds += 1
            print("    [.] no odds (oce+ope2)", file=sys.stderr)
        else:
            m["odds"] = result.get("odds") or []
            for k, v in (result.get("bookmakers") or {}).items():
                bookmakers[str(k)] = str(v)
            if not m["odds"]:
                no_odds += 1
                print("    [.] empty odds", file=sys.stderr)
            else:
                print(f"    [+] {len(m['odds'])} odds rows", file=sys.stderr)
        time.sleep(max(0.0, args.delay))

    bdate = bulletin_date_from_matches(matches)
    payload = {
        "schema_version": 2,
        "source": "flashscore",
        "feed": "day",
        "day_offset": args.offset,
        "bulletin_date": bdate,
        "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "bookmakers": bookmakers,
        "matches": matches,
        "errors": errors,
        "no_odds_count": no_odds,
    }

    if args.output:
        # Actions --out=....json bekliyorsa .gz ekleme; .json.gz ise gzip.
        use_gzip = args.output.endswith(".gz")
        written = write_output(args.output, payload, use_gzip=use_gzip)
        print(f"[+] {written}", file=sys.stderr)

    print(
        f"[+] date={bdate}  matches={len(matches)}  "
        f"bookmakers={len(bookmakers)}  no_odds={no_odds}  errors={len(errors)}",
        file=sys.stderr,
    )

    if do_supabase:
        upsert_fixtures(payload, url, key, batch_size=args.batch_size)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
