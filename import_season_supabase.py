#!/usr/bin/env python3
"""
Compact .json.gz sezon dosyasini Supabase `events` (+ `seasons`) tablosuna yazar.

Env:
  SUPABASE_URL=https://xxxx.supabase.co
  SUPABASE_KEY=service_role_secret

Kullanim:
  python3 import_season_supabase.py data/season_odds/england_....json.gz
  python3 import_season_supabase.py season.json.gz --batch-size 2
"""
from __future__ import annotations

import argparse, gzip, json, os, sys, time
from datetime import datetime, timezone

import requests

from flashscore_markets import (
    build_markets_blob,
    competition_label,
    kickoff_iso,
    season_label_from_slug,
)

SOURCE = "flashscore"


def load_season(path: str) -> dict:
    opener = gzip.open if path.endswith(".gz") else open
    with opener(path, "rt", encoding="utf-8") as f:
        return json.load(f)


def now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def supabase_headers(key: str) -> dict:
    return {
        "apikey": key,
        "Authorization": "Bearer " + key,
        "Content-Type": "application/json",
        # return=minimal: response body yok, daha hizli
        "Prefer": "resolution=merge-duplicates,return=minimal",
    }


def _is_timeout(resp: requests.Response) -> bool:
    if resp.status_code in (408, 504, 524, 570):
        return True
    text = (resp.text or "").lower()
    return (
        resp.status_code == 500
        and ("57014" in text or "statement timeout" in text or "canceling statement" in text)
    )


def upsert_rows(url: str, key: str, table: str, rows: list, on_conflict: str,
                retries: int = 4) -> None:
    """Tek HTTP POST; timeout'ta retry. Cok buyukse cagiran split eder."""
    if not rows:
        return
    endpoint = url.rstrip("/") + "/rest/v1/" + table + "?on_conflict=" + on_conflict
    last_err = None
    for attempt in range(1, retries + 1):
        try:
            resp = requests.post(
                endpoint,
                headers=supabase_headers(key),
                json=rows,
                timeout=180,
            )
        except requests.RequestException as e:
            last_err = e
            time.sleep(min(2 ** attempt, 20))
            continue
        if resp.status_code < 300:
            return
        last_err = RuntimeError(
            f"Supabase upsert {table} failed: {resp.status_code} {resp.text[:500]}"
        )
        if _is_timeout(resp) or resp.status_code >= 500:
            time.sleep(min(2 ** attempt, 20))
            continue
        raise last_err
    raise last_err  # type: ignore[misc]


def upsert_adaptive(url: str, key: str, table: str, rows: list, on_conflict: str,
                    depth: int = 0) -> None:
    """
    Timeout olursa batch'i ikiye bolerek dener; en sonda tek satir.
    markets_json buyuk oldugu icin 50'lik batch PostgREST statement_timeout yer.
    """
    if not rows:
        return
    try:
        upsert_rows(url, key, table, rows, on_conflict)
        return
    except Exception as e:
        msg = str(e).lower()
        timed_out = "57014" in msg or "timeout" in msg or "canceling statement" in msg
        if not timed_out or len(rows) == 1:
            raise
        mid = max(1, len(rows) // 2)
        print(
            f"    [!] timeout ({len(rows)} satir) — bolunuyor {mid}+{len(rows)-mid}",
            file=sys.stderr,
        )
        time.sleep(1.0 + depth * 0.5)
        upsert_adaptive(url, key, table, rows[:mid], on_conflict, depth + 1)
        time.sleep(0.3)
        upsert_adaptive(url, key, table, rows[mid:], on_conflict, depth + 1)


def slim_blob_for_db(blob: dict) -> dict:
    """
    Web icin: selection'da odds + hangi bookmaker'dan geldigi.
    Tum bookmaker dict'ini atar (payload kuculur); gz yedekte full kalir.
    """
    markets = []
    for m in blob.get("markets") or []:
        sels = []
        for s in m.get("selections") or []:
            sels.append({
                "key": s.get("key"),
                "name": s.get("name"),
                "odds": s.get("odds"),
                "opening": s.get("opening"),
                "bookmaker_id": s.get("bookmaker_id"),
                "bookmaker_name": s.get("bookmaker_name"),
                "suspended": s.get("suspended"),
            })
        markets.append({
            "key": m.get("key"),
            "name": m.get("name"),
            "type": m.get("type"),
            "scope": m.get("scope"),
            "line": m.get("line"),
            "selections": sels,
        })
    return {
        "bookmakers": blob.get("bookmakers") or {},
        "markets": markets,
    }


def match_to_event(m: dict, meta: dict, bookmakers: dict, ts: str,
                   slim: bool = True) -> dict:
    blob, digest, sel_count = build_markets_blob(
        m.get("odds") or [],
        bookmakers,
        m.get("home_name") or "Home",
        m.get("away_name") or "Away",
    )
    if slim:
        blob = slim_blob_for_db(blob)
        # hash slim haliyle tutarli olsun
        import hashlib
        raw = json.dumps(blob, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
        digest = hashlib.sha256(raw.encode("utf-8")).hexdigest()

    mid = m["match_id"]
    kickoff = kickoff_iso(m.get("kickoff_ts"))
    hs, aws = m.get("home_score"), m.get("away_score")
    try:
        home_score = int(hs) if hs not in (None, "") else None
    except ValueError:
        home_score = None
    try:
        away_score = int(aws) if aws not in (None, "") else None
    except ValueError:
        away_score = None

    finished = home_score is not None and away_score is not None
    return {
        "id": f"{SOURCE}:{mid}",
        "source": SOURCE,
        "source_event_id": mid,
        "sport": "football",
        "competition": competition_label(meta.get("league_slug")),
        "home_team": m.get("home_name"),
        "away_team": m.get("away_name"),
        "kickoff_at": kickoff,
        "status": "finished" if finished else "scheduled",
        "is_closed": 1 if finished else 0,
        "markets_json": blob,
        "markets_hash": digest,
        "odds_updated_at": ts,
        "opening_captured_at": ts,
        "closing_captured_at": ts if finished else None,
        "created_at": ts,
        "updated_at": ts,
        "round": m.get("round"),
        "home_score": home_score,
        "away_score": away_score,
        "season_slug": meta.get("league_slug"),
        "home_team_id": m.get("home_id"),
        "away_team_id": m.get("away_id"),
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("path", help="season .json veya .json.gz")
    ap.add_argument("--batch-size", type=int, default=5,
                     help="Baslangic batch (timeout olursa otomatik bolunur). Varsayilan 5.")
    ap.add_argument("--full-bookmakers", action="store_true",
                     help="markets_json icine her bookmaker oranini da yaz (daha buyuk, yavas).")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    url = os.environ.get("SUPABASE_URL", "").strip()
    key = os.environ.get("SUPABASE_KEY", "").strip()
    if not args.dry_run and (not url or not key):
        print("[!] SUPABASE_URL ve SUPABASE_KEY gerekli", file=sys.stderr)
        sys.exit(2)

    data = load_season(args.path)
    if data.get("schema_version") != 2:
        print("[!] schema_version=2 bekleniyor (compact format)", file=sys.stderr)
        sys.exit(2)

    bookmakers = data.get("bookmakers") or {}
    meta = {
        "league_slug": data.get("league_slug"),
        "template_id": data.get("template_id"),
        "season_code": data.get("season_code"),
    }
    ts = now_iso()
    matches = data.get("matches") or []
    slim = not args.full_bookmakers
    print(f"[+] {len(matches)} mac, {len(bookmakers)} bookmaker "
          f"(markets_json={'slim' if slim else 'full'})", file=sys.stderr)

    with_1x2 = 0
    broken_side = 0
    ou_ok = 0
    ou_bad = 0
    for m in matches:
        saw_1x2 = False
        for row in m.get("odds") or []:
            if not (isinstance(row, list) and len(row) >= 4):
                continue
            side = str(row[3])
            if "EventOddsItemHandicap" in side or "{'__typename'" in side:
                broken_side += 1
            if row[1] == "OVER_UNDER":
                if side.startswith("OVER:") or side.startswith("UNDER:"):
                    ou_ok += 1
                else:
                    ou_bad += 1
            if (not saw_1x2 and row[1] == "HOME_DRAW_AWAY"
                    and row[2] == "FULL_TIME" and side in ("H", "D", "A")):
                with_1x2 += 1
                saw_1x2 = True
    print(f"[+] FT 1X2 olan mac: {with_1x2}/{len(matches)}", file=sys.stderr)
    print(f"[+] OVER/UNDER side OK/bad: {ou_ok}/{ou_bad}", file=sys.stderr)
    if broken_side or ou_bad > ou_ok:
        print(
            "[!] UYARI: handicap/OU side token'lari bozuk veya eksik "
            "(eski scrape). Duzeltilmis fetchseason.py ile sezonu yeniden cek.",
            file=sys.stderr,
        )

    events = [
        match_to_event(m, meta, bookmakers, ts, slim=slim) for m in matches
    ]
    season_slug = meta.get("league_slug") or "unknown"
    season_row = {
        "id": season_slug,
        "source": SOURCE,
        "competition": competition_label(season_slug),
        "season_label": season_label_from_slug(season_slug),
        "template_id": meta.get("template_id"),
        "season_code": str(meta.get("season_code") or ""),
        "match_count": len(events),
        "bookmaker_count": len(bookmakers),
        "updated_at": ts,
    }

    if args.dry_run:
        sample = events[0] if events else {}
        mk = (sample.get("markets_json") or {}).get("markets") or []
        size = len(json.dumps(sample.get("markets_json") or {}, separators=(",", ":")))
        print("[dry-run] season", season_row, file=sys.stderr)
        print(f"[dry-run] first event markets={len(mk)} json_bytes≈{size}",
              file=sys.stderr)
        if mk:
            print("[dry-run] first market", json.dumps(mk[0], ensure_ascii=False)[:400],
                  file=sys.stderr)
        print("[dry-run] ok — yazma yapilmadi", file=sys.stderr)
        return

    upsert_adaptive(url, key, "seasons", [season_row], on_conflict="id")
    print("[+] seasons upsert ok", file=sys.stderr)

    done = 0
    for i in range(0, len(events), args.batch_size):
        batch = events[i:i + args.batch_size]
        upsert_adaptive(url, key, "events", batch, on_conflict="id")
        done = min(i + len(batch), len(events))
        print(f"    [{done}/{len(events)}] events", file=sys.stderr)
        time.sleep(0.25)

    print("[+] Supabase yazimi tamam", file=sys.stderr)


if __name__ == "__main__":
    main()
