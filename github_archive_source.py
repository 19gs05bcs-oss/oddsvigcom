"""
github_archive_source.py — watcher.py icine gomulen, Supabase'e HIC
dokunmadan private "listener" reposundaki data/season_odds altindaki
tarihsel sezon json.gz dosyalarini dogrudan GitHub'dan okuyup
ArchiveCacheServer'in mevcut LRU/gzip cache mimarisine besleyen kaynak.

Neden Supabase yok:
  330 sezon = 250k+ mac; hepsini Postgres'e upsert edip sonra PostgREST
  uzerinden geri okumak hem yavas hem gereksiz — dosyalar zaten
  GitHub'da duruyor, gerektiginde indirilip donusturulur.

Bellek stratejisi (archive_cache_server.py ile AYNI prensip — bu yuzden
Koyeb'in kisitli RAM'inde 330 sezon birden patlamiyor):
  1) Baslangicta (arka plan thread) SADECE hafif index cikarilir:
     her dosya bir kez indirilip meta'si (league_slug, mac/bookmaker
     sayisi) okunur, HAM MAC LISTESI HEMEN ATILIR. Index ~330 kucuk
     dict — onemsiz bellek.
  2) Bir sezon ilk kez /archive/season/<slug> ile istendiginde, O TEK
     dosya indirilip web-event formatina cevrilir ve
     ArchiveCacheServer'in mevcut LRU cache'ine (CACHE_MAX_MB siniri
     dahil) girer. Supabase-miss davranisiyla birebir ayni — sadece
     kaynak Postgres yerine GitHub.

Kullanim (watcher.py icinde):
    from github_archive_source import GithubArchiveSource

    github_source = GithubArchiveSource(
        repo="19gs05bcs-oss/listener",
        ref=os.environ["LISTENER_REF"],
        path="data/season_odds",
        token=os.environ["GITHUB_TOKEN"],
    )
    github_source.start_background()   # index'i arka planda cikar, bloklamaz

    cache_server = ArchiveCacheServer(..., github_source=github_source)
    cache_server.start()
"""
from __future__ import annotations

import base64
import gzip
import hashlib
import io
import json
import sys
import threading
import time

import requests

from flashscore_markets import (
    build_markets_blob,
    competition_label,
    kickoff_iso,
    season_label_from_slug,
)

API_ROOT = "https://api.github.com"
SOURCE = "flashscore"


def _gh_headers(token: str) -> dict:
    return {
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }


def _slim_blob(blob: dict) -> dict:
    """import_season_supabase.slim_blob_for_db ile ayni: tum bookmaker
    grid'ini atar, sadece en iyi oran + kaynagi kalir."""
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
    return {"bookmakers": blob.get("bookmakers") or {}, "markets": markets}


def _quote_rows_from_odds(odds: list, event_id: str, season_slug: str, kickoff: str | None) -> list[dict]:
    """watcher.py'nin quote_rows_from_fixture() ile BİREBİR AYNI mantık —
    markets_json'u unnest etmeye gerek yok, ham odds listesi zaten flat."""
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
            "kickoff_at": kickoff,
        })
    return out


def _match_to_event(m: dict, league_slug: str, bookmakers: dict, ts: str) -> dict:
    """import_season_supabase.match_to_event ile ayni cikti semasi
    (EVENT_COLS ile birebir uyumlu) — sadece Supabase'e yazmiyoruz."""
    blob, _digest, _n = build_markets_blob(
        m.get("odds") or [], bookmakers,
        m.get("home_name") or "Home", m.get("away_name") or "Away",
    )
    blob = _slim_blob(blob)
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
        "competition": competition_label(league_slug),
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
        "season_slug": league_slug,
        "home_team_id": m.get("home_id"),
        "away_team_id": m.get("away_id"),
    }


class GithubArchiveSource:
    def __init__(self, repo: str, ref: str, path: str, token: str):
        self.repo = repo
        self.ref = ref
        self.path = path
        self.token = token

        self._lock = threading.RLock()
        # slug -> {"sha","name","match_count","bookmaker_count","season_label","template_id","season_code"}
        self._index: dict[str, dict] = {}
        self._ready = False
        self._error: str | None = None

    # ---------------- GitHub HTTP ----------------

    def _list_dir(self) -> list[dict]:
        url = f"{API_ROOT}/repos/{self.repo}/contents/{self.path}"
        r = requests.get(url, headers=_gh_headers(self.token), params={"ref": self.ref}, timeout=30)
        r.raise_for_status()
        items = r.json()
        return [it for it in items if it.get("type") == "file" and it["name"].endswith(".json.gz")]

    def _fetch_blob_bytes(self, sha: str) -> bytes:
        url = f"{API_ROOT}/repos/{self.repo}/git/blobs/{sha}"
        last_err = None
        for attempt in range(1, 5):
            try:
                r = requests.get(url, headers=_gh_headers(self.token), timeout=60)
            except requests.RequestException as e:
                last_err = e
                time.sleep(min(2 ** attempt, 20))
                continue
            if r.status_code == 200:
                data = r.json()
                return base64.b64decode(data["content"])
            if r.status_code in (403, 429):
                time.sleep(min(2 ** attempt, 20))
                continue
            last_err = RuntimeError(f"blob {sha} fetch failed: {r.status_code} {r.text[:200]}")
            time.sleep(min(2 ** attempt, 20))
        raise last_err  # type: ignore[misc]

    def _load_season_json(self, sha: str) -> dict:
        raw = self._fetch_blob_bytes(sha)
        with gzip.GzipFile(fileobj=io.BytesIO(raw)) as f:
            return json.loads(f.read().decode("utf-8"))

    # ---------------- Index (hafif — sadece meta) ----------------

    def build_index(self) -> None:
        try:
            items = self._list_dir()
        except Exception as exc:  # noqa: BLE001
            self._error = str(exc)
            print(f"[github-archive] [!] dizin listelenemedi: {exc}", file=sys.stderr)
            return

        print(f"[github-archive] {len(items)} sezon dosyasi bulundu, index cikariliyor...",
              file=sys.stderr)
        index: dict[str, dict] = {}
        for i, item in enumerate(items, 1):
            try:
                data = self._load_season_json(item["sha"])
            except Exception as exc:  # noqa: BLE001
                print(f"[github-archive] [!] {item['name']} atlandi: {exc}", file=sys.stderr)
                continue
            slug = data.get("league_slug") or item["name"]
            index[slug] = {
                "sha": item["sha"],
                "name": item["name"],
                "match_count": len(data.get("matches") or []),
                "bookmaker_count": len(data.get("bookmakers") or {}),
                "season_label": season_label_from_slug(slug),
                "template_id": data.get("template_id"),
                "season_code": str(data.get("season_code") or ""),
            }
            # data burada scope disina cikinca GC edilir — ham mac listesi RAM'de kalmaz
            if i % 25 == 0 or i == len(items):
                print(f"[github-archive] index {i}/{len(items)}", file=sys.stderr)

        with self._lock:
            self._index = index
            self._ready = True
        print(f"[github-archive] index hazir: {len(index)} sezon", file=sys.stderr)

    def start_background(self) -> None:
        threading.Thread(target=self.build_index, daemon=True, name="github-archive-index").start()

    # ---------------- ArchiveCacheServer'in kullandigi arayuz ----------------

    def is_ready(self) -> bool:
        with self._lock:
            return self._ready

    def has(self, slug: str) -> bool:
        with self._lock:
            return slug in self._index

    def meta_rows(self) -> list[dict]:
        """seasons_meta() ile ayni sekil (id/source/competition/season_label/...)."""
        with self._lock:
            items = list(self._index.items())
        return [
            {
                "id": slug,
                "source": SOURCE,
                "competition": competition_label(slug),
                "season_label": v["season_label"],
                "template_id": v["template_id"],
                "season_code": v["season_code"],
                "match_count": v["match_count"],
                "bookmaker_count": v["bookmaker_count"],
                "updated_at": None,
            }
            for slug, v in items
        ]

    def load_events(self, slug: str) -> list[dict] | None:
        """Tek sezonu GitHub'dan indirir + web-event formatina cevirir.
        Donen liste ArchiveCacheServer.get_season_gz() tarafindan aynen
        Postgres'ten gelen 'events' listesi gibi islenir (LRU'ya girer)."""
        with self._lock:
            entry = self._index.get(slug)
        if entry is None:
            return None
        data = self._load_season_json(entry["sha"])
        bookmakers = data.get("bookmakers") or {}
        league_slug = data.get("league_slug") or slug
        ts = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        return [
            _match_to_event(m, league_slug, bookmakers, ts)
            for m in (data.get("matches") or [])
        ]

    def load_quotes(self, slug: str) -> list[dict] | None:
        """Tek sezonu GitHub'dan indirir + FLAT quote satirlarina cevirir
        (markets_json'u hic unnest etmeden — ham odds listesi zaten flat).
        get_quotes_season_gz()'in Postgres-miss durumundaki karsiligi;
        donen liste watcher.py'nin quote_rows_from_fixture() ciktisiyla
        BİREBİR AYNI sekildedir, ArchiveCacheServer bu ikisini ayirt etmez."""
        with self._lock:
            entry = self._index.get(slug)
        if entry is None:
            return None
        data = self._load_season_json(entry["sha"])
        out: list[dict] = []
        for m in (data.get("matches") or []):
            mid = m.get("match_id")
            if not mid:
                continue
            event_id = f"{SOURCE}:{mid}"
            kickoff = kickoff_iso(m.get("kickoff_ts"))
            out.extend(_quote_rows_from_odds(m.get("odds") or [], event_id, slug, kickoff))
        return out
