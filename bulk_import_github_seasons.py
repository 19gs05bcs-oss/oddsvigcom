#!/usr/bin/env python3
"""
bulk_import_github_seasons.py — private "listener" reposundaki
data/season_odds altindaki tum .json.gz sezon dosyalarini indirip
mevcut import_season_supabase.py ile Supabase'e (events+seasons) yazar.

Neden git blobs API (contents API degil):
  GitHub contents API tekil dosyalari sadece <=1MB icin base64 donuyor;
  season json.gz dosyalarinin bir kismi bunu asabilir. git blobs API
  (100MB'a kadar) sha uzerinden ayni base64 icerigi guvenilir sekilde
  verir, boylece dosya boyutuna gore dallanmaya gerek kalmaz.

Env:
  GITHUB_TOKEN=<repo scope, en az read, PAT>   (listener private oldugu icin zorunlu)
  SUPABASE_URL / SUPABASE_KEY                  (import_season_supabase.py'nin ihtiyaci)

Kullanim:
  python3 bulk_import_github_seasons.py \
      --repo 19gs05bcs-oss/listener \
      --ref 8b54e0432190b2d5eff589dabaf139cef2433f23 \
      --path data/season_odds \
      --out-dir ./season_odds_cache \
      --import

  --import verilmezse sadece indirir (dry-archive), Supabase'e yazmaz.
  Ikinci calistirmada ayni sha'ya sahip dosyalar tekrar indirilmez/
  import edilmez (./season_odds_cache/.manifest.json ile takip edilir).
"""
from __future__ import annotations

import argparse
import base64
import json
import os
import subprocess
import sys
import time

import requests

API_ROOT = "https://api.github.com"


def gh_headers(token: str) -> dict:
    return {
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }


def list_dir(repo: str, path: str, ref: str, token: str) -> list[dict]:
    url = f"{API_ROOT}/repos/{repo}/contents/{path}"
    r = requests.get(url, headers=gh_headers(token), params={"ref": ref}, timeout=30)
    if r.status_code == 404:
        print(f"[!] bulunamadi: {repo}/{path}@{ref} (repo adi / ref / token yetkisi kontrol et)", file=sys.stderr)
        sys.exit(2)
    r.raise_for_status()
    items = r.json()
    if not isinstance(items, list):
        print(f"[!] beklenmeyen yanit (dizin degil mi?): {items}", file=sys.stderr)
        sys.exit(2)
    return [it for it in items if it.get("type") == "file" and it["name"].endswith(".json.gz")]


def fetch_blob(repo: str, sha: str, token: str, retries: int = 4) -> bytes:
    url = f"{API_ROOT}/repos/{repo}/git/blobs/{sha}"
    last_err = None
    for attempt in range(1, retries + 1):
        try:
            r = requests.get(url, headers=gh_headers(token), timeout=60)
        except requests.RequestException as e:
            last_err = e
            time.sleep(min(2 ** attempt, 20))
            continue
        if r.status_code == 200:
            data = r.json()
            assert data.get("encoding") == "base64", f"beklenmeyen encoding: {data.get('encoding')}"
            return base64.b64decode(data["content"])
        if r.status_code in (403, 429):
            # rate limit — bekle
            reset = r.headers.get("X-RateLimit-Reset")
            wait = 20
            if reset:
                wait = max(1, int(reset) - int(time.time()) + 1)
            print(f"    [!] rate limit, {wait}s bekleniyor", file=sys.stderr)
            time.sleep(min(wait, 120))
            continue
        last_err = RuntimeError(f"blob fetch {sha} failed: {r.status_code} {r.text[:200]}")
        time.sleep(min(2 ** attempt, 20))
    raise last_err  # type: ignore[misc]


def load_manifest(out_dir: str) -> dict:
    path = os.path.join(out_dir, ".manifest.json")
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}


def save_manifest(out_dir: str, manifest: dict) -> None:
    path = os.path.join(out_dir, ".manifest.json")
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(manifest, f, ensure_ascii=False, indent=2)
    os.replace(tmp, path)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--repo", default="19gs05bcs-oss/listener")
    ap.add_argument("--ref", required=True, help="commit sha / branch / tag")
    ap.add_argument("--path", default="data/season_odds")
    ap.add_argument("--out-dir", default="./season_odds_cache")
    ap.add_argument("--import", dest="do_import", action="store_true",
                     help="indirdikten sonra import_season_supabase.py ile Supabase'e yaz")
    ap.add_argument("--batch-size", type=int, default=5, help="import_season_supabase.py --batch-size")
    ap.add_argument("--limit", type=int, default=0, help="test icin: ilk N dosyayla sinirla (0=hepsi)")
    ap.add_argument("--force", action="store_true", help="manifest'i yoksay, hepsini yeniden indir/import et")
    args = ap.parse_args()

    token = os.environ.get("GITHUB_TOKEN", "").strip()
    if not token:
        print("[!] GITHUB_TOKEN env degiskeni gerekli (repo private, en az 'repo' read scope)", file=sys.stderr)
        sys.exit(2)
    if args.do_import and not (os.environ.get("SUPABASE_URL") and os.environ.get("SUPABASE_KEY")):
        print("[!] --import icin SUPABASE_URL ve SUPABASE_KEY env degiskenleri gerekli", file=sys.stderr)
        sys.exit(2)

    os.makedirs(args.out_dir, exist_ok=True)
    manifest = {} if args.force else load_manifest(args.out_dir)

    files = list_dir(args.repo, args.path, args.ref, token)
    if args.limit:
        files = files[: args.limit]
    print(f"[+] {len(files)} adet .json.gz bulundu: {args.repo}/{args.path}@{args.ref}", file=sys.stderr)

    import_script = os.path.join(os.path.dirname(os.path.abspath(__file__)), "import_season_supabase.py")

    ok, skipped, failed = 0, 0, 0
    for i, item in enumerate(files, 1):
        name = item["name"]
        sha = item["sha"]
        local_path = os.path.join(args.out_dir, name)
        entry = manifest.get(name, {})

        # 1) indirme (sha degismediyse ve dosya diskteyse atla)
        if entry.get("sha") == sha and os.path.exists(local_path):
            print(f"[{i}/{len(files)}] {name} — degismemis, indirme atlandi", file=sys.stderr)
        else:
            print(f"[{i}/{len(files)}] {name} — indiriliyor ({item.get('size', 0) // 1024} KB)", file=sys.stderr)
            try:
                blob = fetch_blob(args.repo, sha, token)
            except Exception as e:
                print(f"    [!] indirme hatasi: {e}", file=sys.stderr)
                failed += 1
                continue
            with open(local_path, "wb") as f:
                f.write(blob)
            entry["sha"] = sha
            entry["downloaded_at"] = time.time()
            entry["imported"] = False
            manifest[name] = entry
            save_manifest(args.out_dir, manifest)

        # 2) Supabase import
        if not args.do_import:
            ok += 1
            continue
        if entry.get("imported") and entry.get("sha") == sha:
            print("    [=] zaten import edilmis, atlandi", file=sys.stderr)
            skipped += 1
            continue

        cmd = [sys.executable, import_script, local_path, "--batch-size", str(args.batch_size)]
        proc = subprocess.run(cmd, env=os.environ.copy())
        if proc.returncode != 0:
            print(f"    [!] import basarisiz (exit {proc.returncode}): {name}", file=sys.stderr)
            failed += 1
            continue

        entry["imported"] = True
        entry["imported_at"] = time.time()
        manifest[name] = entry
        save_manifest(args.out_dir, manifest)
        ok += 1

    print(f"\n[+] tamam: {ok} basarili, {skipped} atlandi (zaten import), {failed} hata", file=sys.stderr)
    if failed:
        sys.exit(1)


if __name__ == "__main__":
    main()
