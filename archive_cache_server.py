"""
archive_cache_server.py — watcher.py icine gomulen, bellek-butceli
arsiv cache + HTTP servis katmani.

Amac:
  Frontend (oddsvig.com), arsivlenmis mac/oran verisini (events +
  markets_json) artik dogrudan Supabase/Postgres'ten degil, watcher'in
  bu HTTP endpoint'lerinden okur. Boylece Supabase uzerindeki agir
  (buyuk markets_json blob'lu, sik tekrarlanan) okuma yuku ortadan
  kalkar; Supabase sadece watcher'in yazma trafigini gorur.

Bellek stratejisi:
  Tum arsivi (250k+ mac) surekli RAM'de tutmak yerine, sezon bazli
  gzip'li JSON onbellegi + LRU tahliye kullanilir. Bir sezon ilk
  istekte Postgres/PostgREST'ten cekilip gzip'lenerek saklanir;
  CACHE_MAX_MB asilinca en eski kullanilan sezon(lar) atilir.
  watcher yeni bir mac arsivledikce, o mac'in sezonu su an cache'te
  ise yerinde guncellenir (sifirdan cekmeye gerek kalmaz); cache'te
  degilse hicbir sey yapilmaz (bir sonraki istekte zaten taze cekilir).

Kullanim (watcher.py icinde):
    from archive_cache_server import ArchiveCacheServer

    cache_server = ArchiveCacheServer(
        supabase_url=SUPABASE_URL,
        supabase_key=SUPABASE_KEY,
        auth_token=os.environ.get("CACHE_API_TOKEN", ""),
        max_mb=int(os.environ.get("CACHE_MAX_MB", "300")),
        quotes_max_mb=int(os.environ.get("QUOTES_CACHE_MAX_MB", "150")),
        port=int(os.environ.get("PORT", "8000")),
    )
    cache_server.start()  # arka plan thread'inde HTTP server acar

    # her basarili archive_finished_fixture(...) sonrasinda:
    cache_server.update_event(event_row, season_slug)
    cache_server.update_quotes(event_id, quotes, season_slug)

quotes cache — NEDEN:
  oddsvig.com/scorepop tarafinda /analyze aramasi eskiden DuckDB ile
  events.markets_json'u ATTACH edilen Postgres'ten UNNEST ederek
  "quotes_flat" tablosunu SIFIRDAN insa ediyordu — bu hem agir (Railway'i
  502/OOM'a goturuyordu) hem de GEREKSIZDI: watcher zaten her mac
  arsivlerken ayni satirlari (quote_rows_from_fixture) hesaplayip
  Supabase'in "quotes" tablosuna yaziyor. Yani flat veri zaten var —
  Railway'in onu markets_json'dan yeniden turetmesine gerek yok.
  Bu cache, watcher'in zaten urettigi bu satirlari (Postgres'e ayrica
  gitmeden) dogrudan bellekte tutup HTTP'den servis eder.
"""
from __future__ import annotations

import gzip
import json
import sys
import threading
import time
import traceback
from collections import OrderedDict
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse

import requests

from smart_match_report import build_smart_match_report

REST_PAGE_SIZE = 1000  # PostgREST varsayilan max-rows genelde 1000'dir
META_SEASON_CACHE_MAX = 40  # /smart-match/report icin: en fazla N sezonun hafif meta'si tutulur


class _SeasonEntry:
    __slots__ = ("gz_bytes", "event_count", "built_at", "events_by_id")

    def __init__(self, gz_bytes: bytes, events_by_id: dict, built_at: float):
        self.gz_bytes = gz_bytes
        self.event_count = len(events_by_id)
        self.built_at = built_at
        self.events_by_id = events_by_id  # id -> event dict (decompressed, guncelleme icin)


class _QuotesSeasonEntry:
    __slots__ = ("gz_bytes", "row_count", "built_at", "quotes_by_event")

    def __init__(self, gz_bytes: bytes, quotes_by_event: dict, built_at: float):
        self.gz_bytes = gz_bytes
        self.row_count = sum(len(v) for v in quotes_by_event.values())
        self.built_at = built_at
        self.quotes_by_event = quotes_by_event  # event_id -> [quote_row, ...]


class ArchiveCacheServer:
    EVENT_COLS = (
        "id,source,source_event_id,sport,competition,home_team,away_team,"
        "kickoff_at,status,is_closed,markets_json,markets_hash,"
        "odds_updated_at,opening_captured_at,closing_captured_at,"
        "created_at,updated_at,round,home_score,away_score,"
        "home_ht_score,away_ht_score,season_slug,home_team_id,away_team_id"
    )

    QUOTE_COLS = (
        "event_id,season_slug,bookmaker_id,betting_type,betting_scope,"
        "side,opening,current,active,kickoff_at"
    )

    # markets_json YOK — smart-match taramasi bunu hic gormez (bellek/bant tasarrufu)
    EVENT_META_COLS = (
        "id,season_slug,competition,home_team,away_team,kickoff_at,"
        "home_score,away_score"
    )

    def __init__(
        self,
        supabase_url: str,
        supabase_key: str,
        auth_token: str = "",
        max_mb: int = 300,
        quotes_max_mb: int = 150,
        port: int = 8000,
        github_source=None,  # GithubArchiveSource | None — tarihsel arsiv, Supabase'e yazmadan
    ) -> None:
        self.supabase_url = supabase_url.rstrip("/")
        self.supabase_key = supabase_key
        self.auth_token = auth_token
        self.max_bytes = max_mb * 1024 * 1024
        self.quotes_max_bytes = quotes_max_mb * 1024 * 1024
        self.port = port
        self.github_source = github_source

        self._lock = threading.RLock()
        # LRU: en son kullanilan sona eklenir; asim durumunda basdan atilir
        self._seasons: "OrderedDict[str, _SeasonEntry]" = OrderedDict()
        self._current_bytes = 0
        self._seasons_meta_cache: list | None = None
        self._seasons_meta_at = 0.0

        # quotes icin AYRI LRU + bellek butcesi (events cache'inden bagimsiz)
        self._quotes_lock = threading.RLock()
        self._quotes_seasons: "OrderedDict[str, _QuotesSeasonEntry]" = OrderedDict()
        self._quotes_current_bytes = 0

        # smart-match/report icin: hafif (markets_json'suz) event meta, sezon basina LRU
        self._meta_lock = threading.RLock()
        self._meta_seasons: "OrderedDict[str, list]" = OrderedDict()

    # ---------------- Supabase REST yardimcilari ----------------

    def _headers(self) -> dict:
        return {
            "apikey": self.supabase_key,
            "Authorization": f"Bearer {self.supabase_key}",
            "Content-Type": "application/json",
        }

    def _rest_get_paginated(self, table: str, params: dict) -> list:
        out: list = []
        offset = 0
        while True:
            headers = self._headers()
            headers["Range-Unit"] = "items"
            headers["Range"] = f"{offset}-{offset + REST_PAGE_SIZE - 1}"
            r = requests.get(
                f"{self.supabase_url}/rest/v1/{table}",
                headers=headers,
                params=params,
                timeout=30,
            )
            r.raise_for_status()
            batch = r.json()
            out.extend(batch)
            if len(batch) < REST_PAGE_SIZE:
                break
            offset += REST_PAGE_SIZE
        return out

    def _fetch_seasons_meta(self) -> list:
        return self._rest_get_paginated(
            "seasons",
            {
                "select": "id,source,competition,season_label,template_id,season_code,match_count,bookmaker_count,updated_at",
                "source": "eq.flashscore",
                "order": "season_label.desc",
            },
        )

    def _fetch_season_events(self, season_slug: str) -> list:
        return self._rest_get_paginated(
            "events",
            {
                "select": self.EVENT_COLS,
                "source": "eq.flashscore",
                "season_slug": f"eq.{season_slug}",
                "order": "kickoff_at.asc",
            },
        )

    def _fetch_season_meta_light(self, season_slug: str) -> list:
        return self._rest_get_paginated(
            "events",
            {
                "select": self.EVENT_META_COLS,
                "source": "eq.flashscore",
                "season_slug": f"eq.{season_slug}",
                "order": "kickoff_at.asc",
            },
        )

    def _fetch_season_quotes(self, season_slug: str) -> list:
        # quotes tablosu zaten flat (markets_json'daki gibi nested degil) —
        # burada hicbir unnest/parse gerekmiyor, dogrudan satirlari cekiyoruz.
        return self._rest_get_paginated(
            "quotes",
            {
                "select": self.QUOTE_COLS,
                "season_slug": f"eq.{season_slug}",
            },
        )

    # ---------------- Cache okuma / yazma ----------------

    def seasons_meta(self, ttl: int = 300) -> list:
        now = time.time()
        with self._lock:
            if self._seasons_meta_cache is not None and now - self._seasons_meta_at < ttl:
                return self._seasons_meta_cache
        data = self._fetch_seasons_meta()
        if self.github_source is not None:
            # Supabase'deki (canli takip) sezonlarla GitHub tarihsel arsivini
            # birlestir; ayni slug ikisinde de varsa Supabase kaydi kazanir.
            known = {row["id"] for row in data}
            data = data + [row for row in self.github_source.meta_rows() if row["id"] not in known]
        with self._lock:
            self._seasons_meta_cache = data
            self._seasons_meta_at = now
        return data

    def _evict_if_needed(self) -> None:
        # cagiran zaten lock tutuyor olmali
        while self._current_bytes > self.max_bytes and self._seasons:
            _slug, entry = self._seasons.popitem(last=False)  # en eski kullanilan
            self._current_bytes -= len(entry.gz_bytes)

    def get_season_gz(self, season_slug: str) -> bytes:
        with self._lock:
            entry = self._seasons.get(season_slug)
            if entry is not None:
                self._seasons.move_to_end(season_slug)  # LRU: en yeni kullanilan sona
                return entry.gz_bytes

        # cache disinda -> once GitHub tarihsel arsivine bak (Supabase'e hic
        # gitmeden), yoksa Postgres'e dus (lock disinda, IO blocklamasin)
        if self.github_source is not None and self.github_source.has(season_slug):
            events = self.github_source.load_events(season_slug) or []
        else:
            events = self._fetch_season_events(season_slug)
        events_by_id = {e["id"]: e for e in events}
        payload = json.dumps({"ok": True, "season": season_slug, "events": events}).encode("utf-8")
        gz = gzip.compress(payload, compresslevel=6)

        with self._lock:
            entry = _SeasonEntry(gz, events_by_id, time.time())
            self._seasons[season_slug] = entry
            self._seasons.move_to_end(season_slug)
            self._current_bytes += len(gz)
            self._evict_if_needed()
        return gz

    def update_event(self, event_row: dict, season_slug: str | None) -> None:
        """watcher basarili arsivleme sonrasi cagirir. Sezon cache'te
        degilse hicbir sey yapmaz (bir sonraki HTTP istegi taze ceker)."""
        if not season_slug:
            return
        with self._lock:
            entry = self._seasons.get(season_slug)
            if entry is None:
                return
            entry.events_by_id[event_row["id"]] = event_row
            events = list(entry.events_by_id.values())
            payload = json.dumps({"ok": True, "season": season_slug, "events": events}).encode("utf-8")
            gz = gzip.compress(payload, compresslevel=6)
            self._current_bytes += len(gz) - len(entry.gz_bytes)
            entry.gz_bytes = gz
            entry.event_count = len(events)
            entry.built_at = time.time()
            self._seasons.move_to_end(season_slug)
            self._evict_if_needed()

    # ---------------- quotes cache (flat — unnest gerekmiyor) ----------------

    def _evict_quotes_if_needed(self) -> None:
        # cagiran zaten _quotes_lock tutuyor olmali
        while self._quotes_current_bytes > self.quotes_max_bytes and self._quotes_seasons:
            _slug, entry = self._quotes_seasons.popitem(last=False)
            self._quotes_current_bytes -= len(entry.gz_bytes)

    def get_quotes_season_gz(self, season_slug: str) -> bytes:
        with self._quotes_lock:
            entry = self._quotes_seasons.get(season_slug)
            if entry is not None:
                self._quotes_seasons.move_to_end(season_slug)
                return entry.gz_bytes

        rows = self._fetch_season_quotes(season_slug)
        quotes_by_event: dict[str, list] = {}
        for r in rows:
            quotes_by_event.setdefault(r["event_id"], []).append(r)
        payload = json.dumps(
            {"ok": True, "season": season_slug, "quotes": rows}
        ).encode("utf-8")
        gz = gzip.compress(payload, compresslevel=6)

        with self._quotes_lock:
            entry = _QuotesSeasonEntry(gz, quotes_by_event, time.time())
            self._quotes_seasons[season_slug] = entry
            self._quotes_seasons.move_to_end(season_slug)
            self._quotes_current_bytes += len(gz)
            self._evict_quotes_if_needed()
        return gz

    def update_quotes(self, event_id: str, quotes: list[dict], season_slug: str | None) -> None:
        """watcher basarili arsivleme sonrasi cagirir (quote_rows_from_fixture
        ciktisiyla). Sezon cache'te degilse no-op — sonraki HTTP istegi
        Postgres'ten (zaten flat olan quotes tablosundan) taze ceker."""
        if not season_slug:
            return
        with self._quotes_lock:
            entry = self._quotes_seasons.get(season_slug)
            if entry is None:
                return
            entry.quotes_by_event[event_id] = quotes
            all_rows = [q for rows in entry.quotes_by_event.values() for q in rows]
            payload = json.dumps(
                {"ok": True, "season": season_slug, "quotes": all_rows}
            ).encode("utf-8")
            gz = gzip.compress(payload, compresslevel=6)
            self._quotes_current_bytes += len(gz) - len(entry.gz_bytes)
            entry.gz_bytes = gz
            entry.row_count = len(all_rows)
            entry.built_at = time.time()
            self._quotes_seasons.move_to_end(season_slug)
            self._evict_quotes_if_needed()

    # ---------------- smart-match/report: hafif meta + tek-gecis stream ----------------

    def _get_meta_light(self, season_slug: str) -> list:
        with self._meta_lock:
            rows = self._meta_seasons.get(season_slug)
            if rows is not None:
                self._meta_seasons.move_to_end(season_slug)
                return rows
        rows = self._fetch_season_meta_light(season_slug)
        with self._meta_lock:
            self._meta_seasons[season_slug] = rows
            self._meta_seasons.move_to_end(season_slug)
            while len(self._meta_seasons) > META_SEASON_CACHE_MAX:
                self._meta_seasons.popitem(last=False)
        return rows

    @staticmethod
    def _quote_dict_to_row(q: dict) -> list:
        """{bookmaker_id,betting_type,betting_scope,side,opening,current,active}
        -> [bm_id, btype, bscope, side, opening, current, active] (CompactOddsRow)."""
        return [
            q.get("bookmaker_id"),
            q.get("betting_type"),
            q.get("betting_scope"),
            q.get("side"),
            q.get("opening"),
            q.get("current"),
            q.get("active"),
        ]

    def iter_all_archive_matches(self, season_slugs: list[str] | None = None):
        """TEK GECISLIK jenerator — her seferinde bir sezonun meta+quotes'unu
        cekip (cache'ten veya Postgres'ten) satirlari uretir, sonra o sezonu
        birakip bir sonrakine gecer. HICBIR ZAMAN tum sezonlari ayni anda
        bellekte tutmaz — bu yuzden 283 sezon da olsa bellek tavani sabittir.
        """
        if season_slugs:
            slugs = list(season_slugs)
        else:
            slugs = [s["id"] for s in self.seasons_meta()]

        for slug in slugs:
            try:
                meta_rows = self._get_meta_light(slug)
            except Exception:
                traceback.print_exc()
                continue
            if not meta_rows:
                continue
            meta_by_id = {r["id"]: r for r in meta_rows}

            try:
                gz = self.get_quotes_season_gz(slug)
                quotes_payload = json.loads(gzip.decompress(gz))
            except Exception:
                traceback.print_exc()
                continue

            quotes_by_event: dict[str, list] = {}
            for q in quotes_payload.get("quotes", []):
                quotes_by_event.setdefault(q["event_id"], []).append(q)

            for event_id, ev in meta_by_id.items():
                qrows = quotes_by_event.get(event_id)
                if not qrows:
                    continue
                hs, aws = ev.get("home_score"), ev.get("away_score")
                if hs in (None, "") or aws in (None, ""):
                    continue
                try:
                    hs, aws = float(hs), float(aws)
                except (TypeError, ValueError):
                    continue
                yield {
                    "id": event_id,
                    "season": ev.get("season_slug") or slug,
                    "home": ev.get("home_team") or "Home",
                    "away": ev.get("away_team") or "Away",
                    "kickoff": ev.get("kickoff_at"),
                    "homeScore": hs,
                    "awayScore": aws,
                    "odds": [self._quote_dict_to_row(q) for q in qrows],
                    "bookmakers": None,  # quotes flat kayittan isim gelmiyor; grid id ile de calisir
                }
            # bu noktada meta_rows / quotes_payload / quotes_by_event artik
            # referans edilmiyor -> bir sonraki dongude GC edilebilir

    def run_smart_match_report(
        self,
        fixture: dict,
        season_slugs: list[str] | None = None,
        reference_bm: int | None = None,
        tolerance_pct: float = 0.03,
    ) -> dict:
        return build_smart_match_report(
            fixture=fixture,
            archive_iter=self.iter_all_archive_matches(season_slugs),
            archive_source="koyeb:quotes-stream",
            reference_bm=reference_bm,
            tolerance_pct=tolerance_pct,
        )

    def get_event(self, event_id: str) -> dict | None:
        with self._lock:
            for entry in self._seasons.values():
                hit = entry.events_by_id.get(event_id)
                if hit:
                    return hit
        # cache'te yoksa dogrudan tek satir cek (nadir yol, agir degil)
        rows = self._rest_get_paginated(
            "events", {"select": self.EVENT_COLS, "id": f"eq.{event_id}", "limit": "1"}
        )
        return rows[0] if rows else None

    def stats(self) -> dict:
        with self._lock:
            events_stats = {
                "cached_seasons": len(self._seasons),
                "cache_bytes": self._current_bytes,
                "cache_mb": round(self._current_bytes / (1024 * 1024), 2),
                "max_mb": round(self.max_bytes / (1024 * 1024), 2),
                "seasons": [
                    {"slug": s, "events": e.event_count, "built_at": e.built_at}
                    for s, e in self._seasons.items()
                ],
            }
        with self._quotes_lock:
            quotes_stats = {
                "cached_seasons": len(self._quotes_seasons),
                "cache_bytes": self._quotes_current_bytes,
                "cache_mb": round(self._quotes_current_bytes / (1024 * 1024), 2),
                "max_mb": round(self.quotes_max_bytes / (1024 * 1024), 2),
                "seasons": [
                    {"slug": s, "rows": e.row_count, "built_at": e.built_at}
                    for s, e in self._quotes_seasons.items()
                ],
            }
        return {"status": "ok", "events": events_stats, "quotes": quotes_stats}

    # ---------------- HTTP server ----------------

    def start(self) -> None:
        server = self  # closure icin

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, fmt, *args):  # noqa: A002 - stdlib imzasi
                print(f"[cache-http] {self.address_string()} {fmt % args}", file=sys.stderr)

            def _authed(self) -> bool:
                if not server.auth_token:
                    return True
                return self.headers.get("Authorization") == f"Bearer {server.auth_token}"

            def _send_json_gz(self, gz_bytes: bytes, status: int = 200) -> None:
                # Istemci gzip kabul ettigini soylemediyse (Accept-Encoding
                # yok — ornegin curl --compressed olmadan) sikistirilmis
                # bytes'i Content-Encoding: gzip ile yollamak, araya giren
                # proxy'lerin (Koyeb edge dahil) sessizce decode edip
                # header'i yanlis birakmasina yol acabiliyor — sonuc, istemci
                # tarafinda "gunzip: unknown format" gibi bir hataya donuyor.
                # Standart HTTP davranisi: sadece istemci istediyse sikistir.
                accept_enc = self.headers.get("Accept-Encoding", "")
                if "gzip" in accept_enc:
                    self.send_response(status)
                    self.send_header("Content-Type", "application/json")
                    self.send_header("Content-Encoding", "gzip")
                    self.send_header("Content-Length", str(len(gz_bytes)))
                    self.send_header("Cache-Control", "public, max-age=30")
                    self.end_headers()
                    self.wfile.write(gz_bytes)
                else:
                    body = gzip.decompress(gz_bytes)
                    self.send_response(status)
                    self.send_header("Content-Type", "application/json")
                    self.send_header("Content-Length", str(len(body)))
                    self.send_header("Cache-Control", "public, max-age=30")
                    self.end_headers()
                    self.wfile.write(body)

            def _send_json(self, obj: dict, status: int = 200) -> None:
                body = json.dumps(obj).encode("utf-8")
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def do_GET(self) -> None:  # noqa: N802 - stdlib imzasi
                try:
                    parsed = urlparse(self.path)
                    path = parsed.path

                    if path == "/health":
                        self._send_json(server.stats())
                        return

                    if not self._authed():
                        self._send_json({"ok": False, "error": "unauthorized"}, status=401)
                        return

                    if path == "/seasons":
                        self._send_json({"ok": True, "seasons": server.seasons_meta()})
                        return

                    if path.startswith("/archive/season/"):
                        slug = path[len("/archive/season/"):]
                        if not slug:
                            self._send_json({"ok": False, "error": "season slug gerekli"}, status=400)
                            return
                        gz = server.get_season_gz(slug)
                        self._send_json_gz(gz)
                        return

                    if path.startswith("/archive/event/"):
                        event_id = path[len("/archive/event/"):]
                        event = server.get_event(event_id)
                        if event is None:
                            self._send_json({"ok": False, "error": "not found"}, status=404)
                            return
                        self._send_json({"ok": True, "event": event})
                        return

                    if path.startswith("/quotes/season/"):
                        slug = path[len("/quotes/season/"):]
                        if not slug:
                            self._send_json({"ok": False, "error": "season slug gerekli"}, status=400)
                            return
                        gz = server.get_quotes_season_gz(slug)
                        self._send_json_gz(gz)
                        return

                    self._send_json({"ok": False, "error": "not found"}, status=404)
                except Exception as exc:  # noqa: BLE001 - HTTP handler ust seviye guvenlik agi
                    traceback.print_exc()
                    self._send_json({"ok": False, "error": str(exc)}, status=500)

            def do_POST(self) -> None:  # noqa: N802 - stdlib imzasi
                try:
                    parsed = urlparse(self.path)
                    path = parsed.path

                    if not self._authed():
                        self._send_json({"ok": False, "error": "unauthorized"}, status=401)
                        return

                    if path == "/smart-match/report":
                        length = int(self.headers.get("Content-Length") or 0)
                        raw = self.rfile.read(length) if length else b"{}"
                        try:
                            body = json.loads(raw or b"{}")
                        except json.JSONDecodeError:
                            self._send_json({"ok": False, "error": "gecersiz JSON body"}, status=400)
                            return

                        fixture = body.get("fixture") or {}
                        if not fixture.get("match_id") or not fixture.get("odds"):
                            self._send_json(
                                {"ok": False, "error": "fixture.match_id ve fixture.odds gerekli"},
                                status=400,
                            )
                            return
                        seasons = body.get("seasons") or None
                        try:
                            report = server.run_smart_match_report(
                                fixture=fixture,
                                season_slugs=seasons,
                                reference_bm=body.get("referenceBm"),
                                tolerance_pct=float(body.get("tolerancePct") or 0.03),
                            )
                        except Exception as exc:  # noqa: BLE001
                            traceback.print_exc()
                            self._send_json({"ok": False, "error": str(exc)}, status=500)
                            return
                        self._send_json({"ok": True, "report": report})
                        return

                    self._send_json({"ok": False, "error": "not found"}, status=404)
                except Exception as exc:  # noqa: BLE001
                    traceback.print_exc()
                    self._send_json({"ok": False, "error": str(exc)}, status=500)

        httpd = ThreadingHTTPServer(("0.0.0.0", self.port), Handler)
        thread = threading.Thread(target=httpd.serve_forever, daemon=True)
        thread.start()
        print(f"[cache-http] port {self.port} dinleniyor (max_mb={self.max_bytes // (1024*1024)})", file=sys.stderr)
