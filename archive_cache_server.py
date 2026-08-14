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
        port=int(os.environ.get("PORT", "8000")),
    )
    cache_server.start()  # arka plan thread'inde HTTP server acar

    # her basarili archive_finished_fixture(...) sonrasinda:
    cache_server.update_event(event_row, season_slug)
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

REST_PAGE_SIZE = 1000  # PostgREST varsayilan max-rows genelde 1000'dir


class _SeasonEntry:
    __slots__ = ("gz_bytes", "event_count", "built_at", "events_by_id")

    def __init__(self, gz_bytes: bytes, events_by_id: dict, built_at: float):
        self.gz_bytes = gz_bytes
        self.event_count = len(events_by_id)
        self.built_at = built_at
        self.events_by_id = events_by_id  # id -> event dict (decompressed, guncelleme icin)


class ArchiveCacheServer:
    EVENT_COLS = (
        "id,source,source_event_id,sport,competition,home_team,away_team,"
        "kickoff_at,status,is_closed,markets_json,markets_hash,"
        "odds_updated_at,opening_captured_at,closing_captured_at,"
        "created_at,updated_at,round,home_score,away_score,"
        "home_ht_score,away_ht_score,season_slug,home_team_id,away_team_id"
    )

    def __init__(
        self,
        supabase_url: str,
        supabase_key: str,
        auth_token: str = "",
        max_mb: int = 300,
        port: int = 8000,
    ) -> None:
        self.supabase_url = supabase_url.rstrip("/")
        self.supabase_key = supabase_key
        self.auth_token = auth_token
        self.max_bytes = max_mb * 1024 * 1024
        self.port = port

        self._lock = threading.RLock()
        # LRU: en son kullanilan sona eklenir; asim durumunda basdan atilir
        self._seasons: "OrderedDict[str, _SeasonEntry]" = OrderedDict()
        self._current_bytes = 0
        self._seasons_meta_cache: list | None = None
        self._seasons_meta_at = 0.0

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

    # ---------------- Cache okuma / yazma ----------------

    def seasons_meta(self, ttl: int = 300) -> list:
        now = time.time()
        with self._lock:
            if self._seasons_meta_cache is not None and now - self._seasons_meta_at < ttl:
                return self._seasons_meta_cache
        data = self._fetch_seasons_meta()
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

        # cache disinda -> Postgres'ten cek (lock disinda, IO blocklamasin)
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
            return {
                "status": "ok",
                "cached_seasons": len(self._seasons),
                "cache_bytes": self._current_bytes,
                "cache_mb": round(self._current_bytes / (1024 * 1024), 2),
                "max_mb": round(self.max_bytes / (1024 * 1024), 2),
                "seasons": [
                    {"slug": s, "events": e.event_count, "built_at": e.built_at}
                    for s, e in self._seasons.items()
                ],
            }

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
                # Istemci gercekten gzip kabul ediyorsa (tarayici / fetch / --compressed
                # curl) sikistirilmis gonder; etmiyorsa (duz curl, Koyeb'in proxy'si vb.)
                # duz JSON'a cevirip Content-Encoding basma. Aksi halde araya giren
                # proxy'ler govdeyi kendileri decompress edip header'i degistirmeden
                # birakabiliyor, bu da istemci tarafinda "unknown compression format"
                # hatasina yol aciyor.
                accept_encoding = self.headers.get("Accept-Encoding", "")
                client_wants_gzip = "gzip" in accept_encoding.lower()

                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Cache-Control", "public, max-age=30")
                if client_wants_gzip:
                    self.send_header("Content-Encoding", "gzip")
                    self.send_header("Content-Length", str(len(gz_bytes)))
                    self.end_headers()
                    self.wfile.write(gz_bytes)
                else:
                    body = gzip.decompress(gz_bytes)
                    self.send_header("Content-Length", str(len(body)))
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
                    if len(path) > 1 and path.endswith("/"):
                        path = path.rstrip("/")  # /health/ -> /health, /seasons/ -> /seasons

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

                    self._send_json({"ok": False, "error": "not found"}, status=404)
                except Exception as exc:  # noqa: BLE001 - HTTP handler ust seviye guvenlik agi
                    traceback.print_exc()
                    self._send_json({"ok": False, "error": str(exc)}, status=500)

        httpd = ThreadingHTTPServer(("0.0.0.0", self.port), Handler)
        thread = threading.Thread(target=httpd.serve_forever, daemon=True)
        thread.start()
        print(f"[cache-http] port {self.port} dinleniyor (max_mb={self.max_bytes // (1024*1024)})", file=sys.stderr)
