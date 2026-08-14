#!/usr/bin/env python3
"""
watcher.py — surekli calisan servis (Koyeb Worker).

Yapilanlar:
  1) Bulteni periyodik tarar (yeni mac + skor guncellemesi).
  2) Kickoff'a yaklasan maclarda odds'u sik poll'lar (closing snapshot
     aslinda: kickoff anindaki/hemen sonrasindaki son basarili poll).
  3) Mac bitince (skor geldiginde) o mac icin `events` + `quotes`
     tablolarina arsivler — bir daha ayrica "arsiv cek" gerekmez.
  4) Durum `match_track_state` tablosunda tutulur — process restart
     olursa (Koyeb redeploy, crash) kaldigi yerden devam eder.

Mevcut fetchday.py / fetchseason.py / import_*.py / flashscore_markets.pyx
fonksiyonlarini oldugu gibi kullanir — normalize/odds-cekme mantigi
tekrar yazilmadi.

Env:
  SUPABASE_URL=https://xxxx.supabase.co
  SUPABASE_KEY=service_role_secret   (SUPABASE_SERVICE_KEY de kabul edilir)
  DRY_RUN=1   -> supabase'e yazma yapmaz, sadece loglar (test icin)
  ONCE=1      -> tek tick calisip cikar (test icin)

Calistirma:
  python3 watcher.py
  DRY_RUN=1 ONCE=1 python3 watcher.py     # dry-run / sağlama
"""
from __future__ import annotations

import json
import os
import re
import sys
import time
import traceback
from collections import defaultdict
from datetime import datetime, timezone

import requests

from fetchday import fetch_day_feed, parse_day_feed, upsert_fixtures, bulletin_date_from_matches
from fetchseason import fetch_odds_for_match
from import_fixture_supabase import row_from_match
from import_season_supabase import upsert_adaptive, supabase_headers
from flashscore_markets import build_markets_blob, competition_label
from archive_cache_server import ArchiveCacheServer

# ---------------- ayarlar (saniye) ----------------

TICK = 20                          # ana dongu nabzi
BULLETIN_INTERVAL = 300            # bulten (yeni mac + skor) her 5 dk
SEASONS_REFRESH_INTERVAL = 3600    # seasons index her saat
SCHEDULED_POLL = 1800              # kickoff'a >20dk kala odds poll araligi
NEAR_POLL = 90                     # kickoff'a <=20dk kala odds poll araligi
NEAR_WINDOW = 20 * 60              # "yakin" esigi
POST_KICKOFF_GRACE = 15 * 60       # kickoff sonrasi odds poll'a devam sinirI
OFFSETS_TO_WATCH = [0]             # bugun; [-1, 0] gecmis gunun kuyrugunu da yakalar

SUPABASE_URL = (os.environ.get("SUPABASE_URL") or "").rstrip("/")
SUPABASE_KEY = (
    os.environ.get("SUPABASE_KEY") or os.environ.get("SUPABASE_SERVICE_KEY") or ""
)

if not SUPABASE_URL or not SUPABASE_KEY:
    print("[!] SUPABASE_URL + SUPABASE_KEY (service_role) gerekli", file=sys.stderr)
    sys.exit(2)

DRY_RUN = (os.environ.get("DRY_RUN") or "").strip().lower() in ("1", "true", "yes")
ONCE = (os.environ.get("ONCE") or "").strip().lower() in ("1", "true", "yes")

# ---------------- arsiv cache + HTTP servis (oddsvig.com bunu okur) ----------------

CACHE_API_TOKEN = os.environ.get("CACHE_API_TOKEN", "")
CACHE_MAX_MB = int(os.environ.get("CACHE_MAX_MB", "300"))
CACHE_PORT = int(os.environ.get("PORT", "8000"))

cache_server = ArchiveCacheServer(
    supabase_url=SUPABASE_URL,
    supabase_key=SUPABASE_KEY,
    auth_token=CACHE_API_TOKEN,
    max_mb=CACHE_MAX_MB,
    port=CACHE_PORT,
)


def now_ts() -> int:
    return int(time.time())


def now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def rest_get(path: str, params: dict | None = None) -> list:
    r = requests.get(
        f"{SUPABASE_URL}/rest/v1/{path}",
        headers=supabase_headers(SUPABASE_KEY),
        params=params,
        timeout=30,
    )
    r.raise_for_status()
    return r.json()


# ==================================================================
# season_slug cozumleme — DB'ye SQL yerine, seasons tablosunu bir kere
# cekip Python tarafinda index'leyip eslestiriyoruz (daha az DB round-trip)
# ==================================================================

_LEAGUE_SLUG_RE = re.compile(r"/football/([^/]+/[^/]+)/?")
_YEAR_RE = re.compile(r"^(.*)-(\d{4})(?:-(\d{4}))?$")
_COMP_YEAR_SUFFIX_RE = re.compile(r"\s+\d{4}(?:-\d{4})?$")


def competition_slug_from_league(league: str) -> str | None:
    """'COUNTRY: Name: /football/country/league-slug/' -> 'country/league-slug'"""
    m = _LEAGUE_SLUG_RE.search(league or "")
    return m.group(1) if m else None


def _normalize_competition_text(s: str | None) -> str:
    """Karsilastirma icin: bosluk sikistir + casefold."""
    return re.sub(r"\s+", " ", (s or "").strip()).casefold()


def _strip_year_suffix(s: str | None) -> str:
    """seasons.competition 'Premier League 2019-2020' -> 'Premier League'."""
    return _COMP_YEAR_SUFFIX_RE.sub("", (s or "").strip()).strip()


class SeasonIndex:
    """seasons tablosunun bellekteki kopyasi + prefix->en-yeni-yil eslemesi."""

    def __init__(self) -> None:
        self.by_id: set[str] = set()
        self.by_prefix: dict[str, list[tuple[int, str]]] = defaultdict(list)
        self.by_competition: dict[str, list[tuple[int, str]]] = defaultdict(list)
        self.loaded_at = 0

    def refresh(self) -> None:
        rows = rest_get(
            "seasons",
            {"select": "id,competition", "source": "eq.flashscore", "limit": "5000"},
        )
        by_id = {r["id"] for r in rows}
        by_prefix: dict[str, list[tuple[int, str]]] = defaultdict(list)
        by_competition: dict[str, list[tuple[int, str]]] = defaultdict(list)
        for r in rows:
            sid = r["id"]
            m = _YEAR_RE.match(sid)
            if not m:
                continue
            prefix, y1, y2 = m.group(1), m.group(2), m.group(3)
            year = int(y2 or y1)
            by_prefix[prefix].append((year, sid))
            comp = r.get("competition")
            if comp:
                country_from_id = prefix.split("/", 1)[0].replace("-", " ")
                league_name = _strip_year_suffix(comp)
                if league_name:
                    key = (
                        f"{_normalize_competition_text(country_from_id)}"
                        f"|{_normalize_competition_text(league_name)}"
                    )
                    by_competition[key].append((year, sid))
        for k in by_prefix:
            by_prefix[k].sort(reverse=True)
        for k in by_competition:
            by_competition[k].sort(reverse=True)
        self.by_id = by_id
        self.by_prefix = by_prefix
        self.by_competition = by_competition
        self.loaded_at = now_ts()
        print(f"[seasons] {len(by_id)} sezon index'lendi", file=sys.stderr)

    def resolve(self, league: str) -> str | None:
        """Once yilsiz (aktif) slug tam eslesmesi, sonra en yeni yilli slug
        varyanti (sadece league string'i /football/.../ URL iceriyorsa calisir
        — gunluk bultende bu yok). Bunlar bosa cikarsa 'ULKE: Lig Adi'
        formatini ulke + lig adi olarak ayirip seasons.competition'daki
        (yil son eki kirpilmis) lig adiyla + id'nin ulke segmentiyle
        bilesik anahtar olarak karsilastir — gunluk bultenin asil yolu bu."""
        prefix = competition_slug_from_league(league)
        if prefix:
            if prefix in self.by_id:
                return prefix
            cands = self.by_prefix.get(prefix)
            if cands:
                return cands[0][1]
        country, _, rest = (league or "").partition(":")
        # rest hem lig adini hem (gunluk bultende oldugu gibi) sonuna
        # eklenmis /football/.../ url'ini icerebilir — sadece ikinci ":"
        # oncesini lig adi olarak al, url'i at.
        name, _, _url = rest.partition(":")
        name = name.strip() or country.strip()
        key = f"{_normalize_competition_text(country)}|{_normalize_competition_text(name)}"
        comp_cands = self.by_competition.get(key)
        return comp_cands[0][1] if comp_cands else None


# ==================================================================
# match_track_state okuma/yazma
# ==================================================================

def state_upsert(rows: list[dict]) -> None:
    if not rows:
        return
    if DRY_RUN:
        print(f"[dry-run] match_track_state upsert: {rows}", file=sys.stderr)
        return
    upsert_adaptive(SUPABASE_URL, SUPABASE_KEY, "match_track_state", rows, "match_id")


def state_load_active() -> dict[str, dict]:
    rows = rest_get("match_track_state", {"select": "*", "phase": "neq.archived", "limit": "5000"})
    return {r["match_id"]: r for r in rows}


def fixture_by_id(match_id: str) -> dict | None:
    rows = rest_get("fixture", {"select": "*", "match_id": f"eq.{match_id}", "limit": "1"})
    return rows[0] if rows else None


# ==================================================================
# fixture.odds (compact) -> quotes satirlari
# ==================================================================

def quote_rows_from_fixture(fixture_row: dict, event_id: str, season_slug: str | None) -> list[dict]:
    odds = fixture_row.get("odds")
    if isinstance(odds, str):
        try:
            odds = json.loads(odds)
        except json.JSONDecodeError:
            odds = []
    kickoff_at = fixture_row.get("kickoff_at")
    out: list[dict] = []
    for row in odds or []:
        if not (isinstance(row, list) and len(row) >= 7):
            continue
        bm_id, btype, bscope, side, opening, current, active = row[:7]
        out.append({
            "event_id": event_id,
            "season_slug": season_slug,
            "bookmaker_id": int(bm_id) if bm_id not in (None, "") else None,
            "betting_type": btype,
            "betting_scope": bscope,
            "side": str(side),
            "opening": float(opening) if opening not in (None, "") else None,
            "current": float(current) if current not in (None, "") else None,
            "active": bool(active) if active is not None else True,
            "kickoff_at": kickoff_at,
        })
    return out


# ==================================================================
# arsivleme: bitmis fixture -> events + quotes
# ==================================================================

def archive_finished_fixture(fixture_row: dict, season_index: SeasonIndex) -> bool:
    league = fixture_row.get("league") or ""
    season_slug = season_index.resolve(league)
    if season_slug is None:
        print(f"[debug] season_slug eslesmedi, league={league!r}", file=sys.stderr)

    odds = fixture_row.get("odds")
    if isinstance(odds, str):
        try:
            odds = json.loads(odds)
        except json.JSONDecodeError:
            odds = []
    bookmakers = fixture_row.get("bookmakers") or {}
    if isinstance(bookmakers, str):
        try:
            bookmakers = json.loads(bookmakers)
        except json.JSONDecodeError:
            bookmakers = {}

    blob, digest, _sel_count = build_markets_blob(
        odds or [],
        bookmakers,
        fixture_row.get("home_name") or "Home",
        fixture_row.get("away_name") or "Away",
    )

    ts = now_iso()
    mid = fixture_row["match_id"]
    event_id = f"flashscore:{mid}"
    fallback_competition = (league.split(":")[0].strip() or None) if league else None

    event_row = {
        "id": event_id,
        "source": "flashscore",
        "source_event_id": mid,
        "sport": "football",
        "competition": competition_label(season_slug) if season_slug else fallback_competition,
        "home_team": fixture_row.get("home_name"),
        "away_team": fixture_row.get("away_name"),
        "kickoff_at": fixture_row.get("kickoff_at"),
        "status": "finished",
        "is_closed": 1,
        "markets_json": blob,
        "markets_hash": digest,
        "odds_updated_at": ts,
        "closing_captured_at": ts,
        "created_at": ts,
        "updated_at": ts,
        "home_score": fixture_row.get("home_score"),
        "away_score": fixture_row.get("away_score"),
        "home_ht_score": fixture_row.get("home_ht_score"),
        "away_ht_score": fixture_row.get("away_ht_score"),
        "season_slug": season_slug,
        "home_team_id": fixture_row.get("home_id"),
        "away_team_id": fixture_row.get("away_id"),
    }

    quotes = quote_rows_from_fixture(fixture_row, event_id, season_slug)

    if DRY_RUN:
        print(
            f"[dry-run] archive {mid}: events=1 quotes={len(quotes)} season_slug={season_slug}",
            file=sys.stderr,
        )
        return True

    try:
        upsert_adaptive(SUPABASE_URL, SUPABASE_KEY, "events", [event_row], "id")
    except Exception as e:
        print(f"[archive] events upsert basarisiz {mid}: {e}", file=sys.stderr)
        return False

    # sezon su an sicak (cache'te) ise yerinde guncelle; degilse no-op —
    # bir sonraki HTTP istegi zaten taze veriyle sezonu isitir
    cache_server.update_event(event_row, season_slug)

    if quotes:
        try:
            upsert_adaptive(
                SUPABASE_URL, SUPABASE_KEY, "quotes", quotes,
                "event_id,bookmaker_id,betting_type,betting_scope,side",
            )
        except Exception as e:
            print(f"[archive] quotes upsert basarisiz {mid}: {e}", file=sys.stderr)
            # events yazildi ama quotes basarisiz -> archived isaretleme,
            # bir sonraki bultende tekrar denensin
            return False

    print(
        f"[archive] {mid} -> events+quotes  season_slug={season_slug}  quotes={len(quotes)}",
        file=sys.stderr,
    )
    return True


# ==================================================================
# ana dongu
# ==================================================================

def parse_iso(ts: str | None) -> int:
    if not ts:
        return 0
    try:
        return int(datetime.fromisoformat(ts.replace("Z", "+00:00")).timestamp())
    except ValueError:
        return 0


def _tick(loop_start: int, season_index: SeasonIndex, active: dict, last_bulletin: int) -> int:
        if loop_start - season_index.loaded_at >= SEASONS_REFRESH_INTERVAL:
            try:
                season_index.refresh()
            except Exception:
                traceback.print_exc()

        # ---- 1) bulten: yeni mac + skor guncellemesi ----
        if loop_start - last_bulletin >= BULLETIN_INTERVAL:
            for offset in OFFSETS_TO_WATCH:
                try:
                    raw = fetch_day_feed(offset)
                    matches = parse_day_feed(raw)
                except Exception:
                    traceback.print_exc()
                    continue

                # fixture tablosuna baseline satirlari yaz (odds=[] ile,
                # asagidaki odds-poll dongusu daha sonra doldurur). watcher
                # artik fetchday.py'nin ayrica cron ile calistirilmasina
                # ihtiyac duymadan gunun fixture'larini kendisi yaziyor.
                if matches:
                    if DRY_RUN:
                        print(
                            f"[dry-run] fixture bulk upsert: {len(matches)} mac (offset={offset})",
                            file=sys.stderr,
                        )
                    else:
                        try:
                            fixture_payload = {
                                "matches": matches,
                                "bookmakers": {},
                                "bulletin_date": bulletin_date_from_matches(matches),
                                "day_offset": offset,
                                "source": "flashscore",
                            }
                            upsert_fixtures(fixture_payload, SUPABASE_URL, SUPABASE_KEY, batch_size=15)
                        except Exception:
                            traceback.print_exc()

                new_state_rows = []
                for m in matches:
                    mid = m["match_id"]
                    kickoff_ts_raw = m.get("kickoff_ts")
                    kickoff_ts = int(kickoff_ts_raw) if kickoff_ts_raw else None

                    if mid not in active:
                        st = {
                            "match_id": mid,
                            "phase": "scheduled",
                            "kickoff_ts": kickoff_ts,
                            "last_polled_at": None,
                        }
                        active[mid] = st
                        new_state_rows.append(st)

                    finished = (
                        m.get("home_score") not in (None, "")
                        and m.get("away_score") not in (None, "")
                    )
                    st = active.get(mid)
                    if finished and st and st.get("phase") != "archived":
                        fixture_row = fixture_by_id(mid)
                        if fixture_row and archive_finished_fixture(fixture_row, season_index):
                            st["phase"] = "archived"
                            state_upsert([{
                                "match_id": mid,
                                "phase": "archived",
                                "archived_at": now_iso(),
                            }])

                if new_state_rows:
                    state_upsert(new_state_rows)
                    print(f"[bulletin] +{len(new_state_rows)} yeni mac izleniyor", file=sys.stderr)

            last_bulletin = loop_start

        # ---- 2) odds poll: aktif her mac icin, kickoff yakinligina gore siklik ----
        for mid in list(active.keys()):
            st = active[mid]
            if st.get("phase") == "archived":
                del active[mid]
                continue

            kickoff_ts = st.get("kickoff_ts")
            if not kickoff_ts:
                continue

            to_kickoff = kickoff_ts - loop_start
            if to_kickoff < -POST_KICKOFF_GRACE:
                continue  # kickoff'tan uzun sure sonra — odds artik degismez

            interval = NEAR_POLL if to_kickoff <= NEAR_WINDOW else SCHEDULED_POLL
            if loop_start - parse_iso(st.get("last_polled_at")) < interval:
                continue

            fixture_row = fixture_by_id(mid)
            if not fixture_row:
                continue

            m = {
                "match_id": mid,
                "home_id": fixture_row.get("home_id"),
                "away_id": fixture_row.get("away_id"),
                "home_slug": fixture_row.get("home_slug"),
                "away_slug": fixture_row.get("away_slug"),
                "home_name": fixture_row.get("home_name"),
                "away_name": fixture_row.get("away_name"),
                "match_url": fixture_row.get("match_url"),
            }
            result, err = fetch_odds_for_match(m)
            ts = now_iso()
            if err:
                print(f"[odds] {mid} hata: {err}", file=sys.stderr)
            elif result:
                row = row_from_match(
                    {**fixture_row, **m, "odds": result.get("odds") or []},
                    bulletin_date=fixture_row.get("bulletin_date"),
                    day_offset=fixture_row.get("day_offset") or 0,
                    source="flashscore",
                    bookmakers=result.get("bookmakers") or {},
                    scraped_at=ts,
                )
                if DRY_RUN:
                    print(f"[dry-run] fixture upsert: {mid}", file=sys.stderr)
                else:
                    try:
                        upsert_adaptive(SUPABASE_URL, SUPABASE_KEY, "fixture", [row], "match_id")
                    except Exception:
                        traceback.print_exc()

            new_phase = "near_kickoff" if to_kickoff <= NEAR_WINDOW else "scheduled"
            st["phase"] = new_phase
            st["last_polled_at"] = ts
            state_upsert([{"match_id": mid, "phase": new_phase, "last_polled_at": ts}])

        return last_bulletin


def run() -> None:
    season_index = SeasonIndex()
    season_index.refresh()

    last_bulletin = 0
    active = state_load_active()
    print(f"[boot] {len(active)} aktif mac state'ten yuklendi", file=sys.stderr)
    if DRY_RUN:
        print("[boot] DRY_RUN aktif — supabase'e yazma yapilmayacak", file=sys.stderr)
    if ONCE:
        print("[boot] ONCE aktif — tek tick sonra cikilacak", file=sys.stderr)

    cache_server.start()

    while True:
        loop_start = now_ts()
        try:
            last_bulletin = _tick(loop_start, season_index, active, last_bulletin)
        except Exception:
            traceback.print_exc()

        if ONCE:
            print("[once] tek tick tamamlandi, cikiliyor", file=sys.stderr)
            return

        elapsed = now_ts() - loop_start
        time.sleep(max(1, TICK - elapsed))


if __name__ == "__main__":
    try:
        run()
    except KeyboardInterrupt:
        pass
