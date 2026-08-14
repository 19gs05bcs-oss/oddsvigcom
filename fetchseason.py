#!/usr/bin/env python3
"""
Lig + sezon bazli fikstur listesini "tr_" feed'inden ceker (ayrica sezonun
en son bolumu icin sayfanin gomulu ilk-yukleme verisini de ceker), ardindan
mevcut odds API'siyle (global.ds.lsapp.eu) her mac icin oranlari toplar.

Kullanim (onerilen - slug yeter, seasonId sayfadan okunur):
  python3 fetchseason.py --league-slug england/premier-league-2024-2025 \
      --out premier_league_2024_2025.json.gz

Tek yil / karisik arsiv (Arjantin gibi 2024 ve 2019-2020 bir arada):
  python3 fetchseason.py --league-base argentina/liga-profesional --season 2024
  python3 fetchseason.py --league-base argentina/liga-profesional --list-seasons
  # NOT: #/UwflyvqC/ gibi hash stage id'leri sunucuya gitmez; slug yeter.

Cikti varsayilan olarak kompakt + gzip (.json.gz):
  - bookmaker isimleri bir kez dictionary'de
  - her oran satiri dizi: [bm_id, type, scope, side, opening, current, active]
  - indent yok, gzip kayipsiz sikistirma
  Eski verbose JSON icin: --pretty --no-gzip

--season-code '2024-2025' DEGIL, Flashscore'un ic sayisal kodudur
(PL 2024/25 = 184). Sayisal degilse ve --league-slug varsa script
HTML'deki seasonId alanindan otomatik cozer.

ONEMLI: HTML icinde iki ayri gomulu feed var:
  - initialFeeds['results']          -> Results sekmesinin tam ilk batch'i
                                         (~100 mac, orn. Round 28-38)
  - initialFeeds["summary-results"]  -> Summary sekmesinin kucuk onizlemesi
                                         (~18 mac, orn. sadece Round 37-38)
Script bilerek 'results' feed'ini okur. summary-results kullanilirsa
aradaki ~90 mac (orn. Round 30-36) tamamen kaybolur ve sezon ~280 mac
olarak kalir (380 yerine).
"""
import argparse, gzip, json, os, re, sys, time
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed

# Kompakt odds satiri semasi (schema_version=2):
# [bookmaker_id, betting_type, betting_scope, side, opening, current, active]
# side: "H"|"D"|"A" veya "score:1-0" / "handicap:-0.5" / "sel:..." / "p:<id>"
ODDS_SCHEMA = [
    "bookmaker_id", "betting_type", "betting_scope",
    "side", "opening_odds", "current_odds", "active",
]

HEADERS = {
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/151.0.0.0 Safari/537.36",
    "accept-language": "en-US,en;q=0.8",
}

FEED_HEADERS = {
    "user-agent": HEADERS["user-agent"],
    "accept": "*/*",
    "referer": "https://www.flashscore.co.uk/",
    "origin": "https://www.flashscore.co.uk",
    "x-fsign": "SW9D1eZo",
    "x-geoip": "1",
}

# Sonuc listesi feed'i: sport_id sabit 198 (futbol), template_id lig sablonu,
# season_code sezona ozel sayisal kod, page/total sayfalama.
RESULTS_FEED = "https://5.flashscore.ninja/5/x/feed/tr_1_{sport_id}_{template_id}_{season_code}_{page}_{total}_en-uk_1"
# Odds comparison (HAR: _hash=oce). GB geo bazen 0 market doner; MW/ope2 fallback var.
ODDS_ENDPOINT = "https://global.ds.lsapp.eu/odds/pq_graphql?_hash=oce&eventId={event_id}&projectId=5&geoIpCode=GB&geoIpSubdivisionCode=GB"
# Prematch menu + per-bookmaker overview (HAR: pobtm / ope2) — oynanmamis maclar icin.
PREMATCH_MENU_ENDPOINT = (
    "https://global.ds.lsapp.eu/odds/pq_graphql?_hash=pobtm"
    "&eventId={event_id}&projectId=5&geoIpCode=GB&geoIpSubdivisionCode=GB"
)
PREMATCH_ODDS_ENDPOINT = (
    "https://global.ds.lsapp.eu/odds/pq_graphql?_hash=ope2"
    "&eventId={event_id}&bookmakerId={bookmaker_id}"
    "&betType={bet_type}&betScope={bet_scope}"
)
# ope2 varsayilan marketler (pobtm menu yoksa)
PREMATCH_DEFAULT_MARKETS = [
    ("HOME_DRAW_AWAY", "FULL_TIME"),
    ("OVER_UNDER", "FULL_TIME"),
    ("BOTH_TEAMS_TO_SCORE", "FULL_TIME"),
    ("DOUBLE_CHANCE", "FULL_TIME"),
    ("ASIAN_HANDICAP", "FULL_TIME"),
    ("CORRECT_SCORE", "FULL_TIME"),
]
PREMATCH_MAX_BOOKMAKERS = 8


def parse_record(r):
    d = {}
    for part in r.split("\u00ac"):
        if "\u00f7" in part:
            k, _, v = part.partition("\u00f7")
            d[k] = v
    return d


def half_time_scores(ft_home, ft_away, sh_home, sh_away, ht_home=None, ht_away=None):
    """
    Flashscore skor alanlari:
      AG/AH = mac sonucu (FT)
      BA/BB = 1. yari (df_sur; list/tr_ feed'de genelde YOK)
      BC/BD = 2. yari golleri (list/tr_/gomulu results) — 1Y DEGIL
      1Y = BA/BB  veya  FT - BC/BD

    Once dogrudan HT (BA/BB) kullan; yoksa FT-2Y.
    """
    # 1) Dogudan 1Y (df_sur BA/BB)
    try:
        if ht_home not in (None, "") and ht_away not in (None, ""):
            h, a = int(ht_home), int(ht_away)
            if h >= 0 and a >= 0:
                return str(h), str(a)
    except (TypeError, ValueError):
        pass

    # 2) FT - 2Y
    try:
        if ft_home in (None, "") or sh_home in (None, ""):
            h = None
        else:
            h = int(ft_home) - int(sh_home)
            if h < 0:
                h = None
        if ft_away in (None, "") or sh_away in (None, ""):
            a = None
        else:
            a = int(ft_away) - int(sh_away)
            if a < 0:
                a = None
    except (TypeError, ValueError):
        return None, None
    return (None if h is None else str(h), None if a is None else str(a))


def records_to_matches(raw):
    matches = []
    for r in raw.split("~"):
        if not r.strip():
            continue
        d = parse_record(r)
        if "AA" in d:
            ft_h, ft_a = d.get("AG"), d.get("AH")
            ht_h, ht_a = half_time_scores(
                ft_h, ft_a,
                d.get("BC"), d.get("BD"),
                ht_home=d.get("BA"), ht_away=d.get("BB"),
            )
            matches.append({
                "match_id": d.get("AA"), "kickoff_ts": d.get("AD"),
                "home_name": d.get("AE"), "home_id": d.get("JA"), "home_slug": d.get("WU"),
                "away_name": d.get("AF"), "away_id": d.get("JB"), "away_slug": d.get("WV"),
                "home_score": ft_h, "away_score": ft_a,
                "home_ht_score": ht_h, "away_ht_score": ht_a,
                "round": d.get("ER"),
            })
    return matches


def fetch_df_sur_ht(event_id, ft_home=None, ft_away=None):
    """
    df_sur_1_{eventId}: BA/BB = 1Y, BC/BD = 2Y.
    List feed BC/BD eksikse buradan 1Y tamamlanir.
    """
    if not event_id:
        return None, None
    url = "https://5.flashscore.ninja/5/x/feed/df_sur_1_" + str(event_id)
    try:
        resp = requests.get(url, headers=FEED_HEADERS, timeout=15)
        if resp.status_code != 200 or not resp.text.strip():
            return None, None
    except Exception:
        return None, None
    ba = bb = bc = bd = None
    for rec in resp.text.split("~"):
        d = parse_record(rec)
        if "BA" in d and ba is None:
            ba = d.get("BA")
        if "BB" in d and bb is None:
            bb = d.get("BB")
        if "BC" in d and bc is None:
            bc = d.get("BC")
        if "BD" in d and bd is None:
            bd = d.get("BD")
    return half_time_scores(ft_home, ft_away, bc, bd, ht_home=ba, ht_away=bb)


def enrich_missing_ht(matches, workers=8, delay=0.05):
    """
    FT var ama 1Y yoksa df_sur ile BA/BB doldur.
    List/tr_ feed'de BC/BD genelde yeter; bu fallback eski/eksik feed icin.
    """
    need = [
        m for m in matches
        if m.get("match_id")
        and m.get("home_score") not in (None, "")
        and m.get("away_score") not in (None, "")
        and (
            m.get("home_ht_score") in (None, "")
            or m.get("away_ht_score") in (None, "")
        )
    ]
    if not need:
        return 0
    print("[+] HT eksik " + str(len(need)) +
          " mac icin df_sur fallback...", file=sys.stderr)
    filled = 0

    def one(m):
        if delay and delay > 0:
            time.sleep(delay)
        return m["match_id"], fetch_df_sur_ht(
            m["match_id"], m.get("home_score"), m.get("away_score")
        )

    by_id = {m["match_id"]: m for m in matches}
    with ThreadPoolExecutor(max_workers=max(1, int(workers))) as pool:
        futs = [pool.submit(one, m) for m in need]
        for fut in as_completed(futs):
            try:
                mid, (h, a) = fut.result()
            except Exception:
                continue
            if h is None or a is None:
                continue
            row = by_id.get(mid)
            if not row:
                continue
            row["home_ht_score"] = h
            row["away_ht_score"] = a
            filled += 1
    print("    [+] df_sur ile HT doldurulan: " + str(filled) + "/" + str(len(need)),
          file=sys.stderr)
    return filled


def fetch_results_page(sport_id, template_id, season_code, page, total):
    url = RESULTS_FEED.format(sport_id=sport_id, template_id=template_id,
                               season_code=season_code, page=page, total=total)
    resp = requests.get(url, headers=FEED_HEADERS, timeout=20)
    print("    [debug] GET " + url, file=sys.stderr)
    print("    [debug] status=" + str(resp.status_code) +
          " content-length=" + str(len(resp.content)), file=sys.stderr)
    if resp.status_code != 200:
        return None
    text = resp.text
    if not text.strip() or "AA\u00f7" not in text:
        return []
    return records_to_matches(text)


def _extract_initial_feed(html, feed_key):
    """
    HTML icinden cjs.initialFeeds['key'] / ["key"] blogunu bulup
    (matches, allEventsCount) dondurur. Yoksa ([], None).
    """
    for quote in ("'", '"'):
        marker = "cjs.initialFeeds[" + quote + feed_key + quote + "]"
        start = html.find(marker)
        if start == -1:
            continue
        data_marker = "data: `"
        dstart = html.find(data_marker, start)
        # Feed blogu marker'dan hemen sonra gelmeli; yoksa yanlis eslesme.
        if dstart == -1 or dstart > start + 300:
            continue
        content_start = dstart + len(data_marker)
        end = html.find("`", content_start)
        if end == -1:
            continue
        raw = html[content_start:end]

        total_hint = None
        tail = html[end:end + 300]
        m = re.search(r"allEventsCount:\s*(\d+)", tail)
        if m:
            total_hint = int(m.group(1))

        return records_to_matches(raw), total_hint
    return [], None


def _extract_page_meta(html):
    """HTML'den seasonId / tournamentTemplateId oku."""
    meta = {}
    m = re.search(r"seasonId:\s*(\d+)", html)
    if m:
        meta["season_code"] = m.group(1)
    m = re.search(r'tournamentTemplateId:\s*"([^"]+)"', html)
    if m:
        meta["template_id"] = m.group(1)
    return meta


def normalize_league_slug(raw):
    """
    URL / slug normalize: hash (#/stageId/), query, football/ prefix temizlenir.
    orn. 'argentina/liga-profesional-2024/#/UwflyvqC/' -> 'argentina/liga-profesional-2024'
    """
    s = (raw or "").strip()
    if not s:
        return ""
    s = s.split("#", 1)[0].split("?", 1)[0].strip()
    m = re.search(r"/football/(.+)$", s)
    if m:
        s = m.group(1)
    s = s.strip("/")
    if s.startswith("football/"):
        s = s[len("football/"):]
    return s.strip("/")


def _normalize_season_token(season):
    """'2024/2025' | '2024-2025' | '2024' -> '2024-2025' | '2024'."""
    s = (season or "").strip().replace("–", "-").replace("/", "-")
    s = re.sub(r"\s+", "", s)
    if re.fullmatch(r"\d{4}-\d{4}", s) or re.fullmatch(r"\d{4}", s):
        return s
    return s


def _split_league_slug(league_slug):
    """
    'argentina/liga-profesional-2024' -> (base, '2024')
    'england/premier-league-2024-2025' -> (base, '2024-2025')
    'argentina/liga-profesional' -> (base, None)  # guncel sezon URL'si
    """
    slug = normalize_league_slug(league_slug)
    if not slug:
        return "", None
    leaf = slug.split("/")[-1]
    m = re.match(r"^(?P<body>.+)-(?P<season>\d{4}-\d{4})$", leaf)
    if not m:
        m = re.match(r"^(?P<body>.+)-(?P<season>\d{4})$", leaf)
    if not m:
        return slug, None
    parts = slug.split("/")
    parts[-1] = m.group("body")
    return "/".join(parts), m.group("season")


def _season_keys_for_archive_entry(name, slug, league_base):
    """Arsiv satirindan eslestirme anahtarlari (tek yil + YYYY-YYYY)."""
    keys = set()
    slug = normalize_league_slug(slug)
    base = normalize_league_slug(league_base)
    leaf = slug.split("/")[-1] if slug else ""
    m = re.search(r"-(\d{4}-\d{4})$", leaf)
    if m:
        keys.add(m.group(1))
        return keys
    m = re.search(r"-(\d{4})$", leaf)
    if m:
        keys.add(m.group(1))
        return keys
    # Guncel sezon: URL'de yil yok (…/liga-profesional/) — isimden oku.
    if slug == base or not re.search(r"-\d{4}", leaf):
        name_n = (name or "").replace("–", "-")
        m = re.search(r"(\d{4})\s*/\s*(\d{2,4})", name_n)
        if m:
            y1, y2 = m.group(1), m.group(2)
            if len(y2) == 2:
                y2 = y1[:2] + y2
            keys.add(y1 + "-" + y2)
            return keys
        m = re.search(r"(\d{4})\s*-\s*(\d{4})", name_n)
        if m:
            keys.add(m.group(1) + "-" + m.group(2))
            return keys
        m = re.search(r"\b(\d{4})\b", name_n)
        if m:
            keys.add(m.group(1))
    return keys


def _parse_league_archive_data(html, league_base):
    """
    HTML icindeki `var league_archive_data = {...};` blogunu parse et.
    HAR: bu blok /archive/ ve sezon sayfalarinda da gomulu (28 sezon, tek+cift yil).
    """
    base = normalize_league_slug(league_base)
    m = re.search(r"var\s+league_archive_data\s*=\s*(\{.*?\});\s*", html, re.S)
    if not m:
        return []
    data = json.loads(m.group(1))
    out = []
    for row in data.get("seasons") or []:
        path = (row.get("url") or "").strip()
        slug = normalize_league_slug(path)
        if not slug:
            continue
        name = row.get("name") or slug
        out.append({
            "name": name,
            "url": path,
            "slug": slug,
            "keys": sorted(_season_keys_for_archive_entry(name, slug, base)),
        })
    return out


def fetch_archive_seasons(league_base):
    """
    /archive/ sayfasindaki league_archive_data.seasons listesini dondurur.
    Her eleman: {name, url, slug, keys}
    """
    base = normalize_league_slug(league_base)
    if not base:
        return []
    url = "https://www.flashscore.co.uk/football/" + base + "/archive/"
    resp = requests.get(url, headers=HEADERS, timeout=25)
    print("    [debug] GET " + url, file=sys.stderr)
    print("    [debug] status=" + str(resp.status_code) +
          " content-length=" + str(len(resp.content)), file=sys.stderr)
    resp.raise_for_status()
    out = _parse_league_archive_data(resp.text, base)
    if not out:
        print("    [!] league_archive_data bulunamadi", file=sys.stderr)
    return out


def resolve_league_slug(league_slug=None, league_base=None, season=None):
    """
    Tek yil (2024) ve cift yil (2019-2020) arsivini dogru slug'a cevirir.

    Oncelik:
      1) --league-slug dogrudan (hash temizlenmis)
      2) --league-base + --season → archive eslesmesi
      3) yanlis YYYY-YYYY uretilmis slug 404 olursa archive fallback
         (orn. …-2024-2025 yok → …-2024)
    """
    slug = normalize_league_slug(league_slug) if league_slug else None
    base = normalize_league_slug(league_base) if league_base else None
    season_tok = _normalize_season_token(season) if season else None

    if slug and not base:
        base, slug_season = _split_league_slug(slug)
        if not season_tok:
            season_tok = slug_season

    if slug and not season_tok:
        # Tam slug verilmis; archive'a gerek yok (cagiran sayfayi dener).
        return slug, None

    if not base or not season_tok:
        return slug, None

    seasons = []
    try:
        seasons = fetch_archive_seasons(base)
    except Exception as e:
        print("    [!] archive okunamadi: " + str(e), file=sys.stderr)

    def find_key(want):
        for row in seasons:
            if want in row["keys"]:
                return row
        return None

    hit = find_key(season_tok) if seasons else None
    fallback_note = None
    if not hit and seasons and re.fullmatch(r"\d{4}-\d{4}", season_tok):
        y1, y2 = season_tok.split("-")
        # Arjantin: 2024-2025 yok, 2024 / 2025 tekil var.
        hit = find_key(y1) or find_key(y2)
        if hit:
            fallback_note = (
                "istenilen '" + season_tok + "' archive'da yok; '"
                + ",".join(hit["keys"]) + "' kullanildi (" + hit["name"] + ")"
            )
    if not hit and seasons and re.fullmatch(r"\d{4}", season_tok):
        # Tek yil yoksa yil-yil+1 dene (PL tarzi varsayim).
        nxt = str(int(season_tok) + 1)
        hit = find_key(season_tok + "-" + nxt)
        if hit:
            fallback_note = (
                "istenilen '" + season_tok + "' yok; '"
                + ",".join(hit["keys"]) + "' kullanildi (" + hit["name"] + ")"
            )

    if hit:
        if fallback_note:
            print("    [!] " + fallback_note, file=sys.stderr)
        if slug and slug != hit["slug"]:
            print("    [!] slug '" + slug + "' -> archive '" + hit["slug"] + "'",
                  file=sys.stderr)
        print("    [+] archive cozum: " + hit["name"] + " -> " + hit["slug"],
              file=sys.stderr)
        return hit["slug"], None

    # Archive yok/miss: aday URL'leri HEAD/GET ile dene (404 ele).
    candidates = []
    for c in (
        base + "-" + season_tok,
        base,  # guncel sezon URL'si bazen yilsiz
    ):
        if c not in candidates:
            candidates.append(c)
    if re.fullmatch(r"\d{4}-\d{4}", season_tok):
        y1, y2 = season_tok.split("-")
        for c in (base + "-" + y1, base + "-" + y2):
            if c not in candidates:
                candidates.append(c)
    elif re.fullmatch(r"\d{4}", season_tok):
        c = base + "-" + season_tok + "-" + str(int(season_tok) + 1)
        if c not in candidates:
            candidates.append(c)

    for cand in candidates:
        try:
            url = "https://www.flashscore.co.uk/football/" + cand + "/"
            resp = requests.get(url, headers=HEADERS, timeout=15, allow_redirects=True)
            print("    [debug] probe " + url + " -> " + str(resp.status_code),
                  file=sys.stderr)
            if resp.status_code == 200 and "seasonId:" in resp.text:
                if cand != (slug or ""):
                    print("    [+] probe cozum: " + cand, file=sys.stderr)
                return cand, None
        except Exception as e:
            print("    [debug] probe hata " + cand + ": " + str(e), file=sys.stderr)

    if seasons:
        print("[!] Archive'da sezon bulunamadi: base=" + base +
              " season=" + season_tok, file=sys.stderr)
        print("    Mevcut sezonlar:", file=sys.stderr)
        for row in seasons[:25]:
            print("      " + ",".join(row["keys"] or ["?"]) +
                  "  " + row["name"] + "  -> " + row["slug"], file=sys.stderr)
        if len(seasons) > 25:
            print("      ... +" + str(len(seasons) - 25) + " daha", file=sys.stderr)
        return None, "archive_miss"

    print("[!] Sezon slug cozulemedi: base=" + base + " season=" + season_tok,
          file=sys.stderr)
    return None, "resolve_failed"

def fetch_league_page(league_slug):
    """
    Sezon arsiv sayfasini ceker. Donen dict:
      matches, season_code, template_id, all_events_count, error, resolved_slug
    """
    slug = normalize_league_slug(league_slug)
    url = "https://www.flashscore.co.uk/football/" + slug + "/"
    out = {
        "matches": [], "season_code": None, "template_id": None,
        "all_events_count": None, "error": None, "resolved_slug": slug,
    }
    try:
        resp = requests.get(url, headers=HEADERS, timeout=20)
        print("    [debug] GET " + url, file=sys.stderr)
        print("    [debug] status=" + str(resp.status_code) +
              " content-length=" + str(len(resp.content)), file=sys.stderr)
        if resp.status_code == 404:
            base, season = _split_league_slug(slug)
            if base and season:
                print("    [!] 404 — archive'dan sezon cozuluyor "
                      "(tek/cift yil slug farki olabilir)...", file=sys.stderr)
                resolved, err = resolve_league_slug(
                    league_slug=None, league_base=base, season=season
                )
                if resolved and resolved != slug:
                    return fetch_league_page(resolved)
            out["error"] = "404 Not Found"
            print("    [!] league sayfasi 404: " + url, file=sys.stderr)
            print("    Ipucu: --league-base ... --season 2024  veya "
                  "--list-seasons ile dogru slug'u bul.", file=sys.stderr)
            return out
        resp.raise_for_status()
        text = resp.text
    except Exception as e:
        out["error"] = str(e)
        print("    [!] league sayfasi cekilemedi (" + str(e) +
              "), en son mac bolumu eksik kalabilir", file=sys.stderr)
        return out

    meta = _extract_page_meta(text)
    out.update(meta)
    if meta.get("season_code"):
        print("    [debug] sayfadan seasonId=" + meta["season_code"], file=sys.stderr)
    if meta.get("template_id"):
        print("    [debug] sayfadan tournamentTemplateId=" + meta["template_id"],
              file=sys.stderr)

    # Once Results sekmesinin tam ilk batch'ini dene; yoksa summary'ye dus.
    for feed_key in ("results", "summary-results"):
        matches, total_hint = _extract_initial_feed(text, feed_key)
        if total_hint is not None:
            out["all_events_count"] = total_hint
            print("    [debug] feed='" + feed_key + "' allEventsCount=" +
                  str(total_hint), file=sys.stderr)
        if matches:
            out["matches"] = matches
            print("    [+] gomulu '" + feed_key + "' feed'inden " +
                  str(len(matches)) + " mac alindi", file=sys.stderr)
            if feed_key == "summary-results":
                print("    [!] UYARI: sadece summary-results bulundu "
                      "(kucuk onizleme). Ara haftalar eksik kalabilir; "
                      "results feed'i sayfada yok.", file=sys.stderr)
            return out

    print("    [!] results / summary-results gomulu verisi bulunamadi, "
          "sayfa yapisi degismis olabilir", file=sys.stderr)
    return out


def fetch_all_season_fixtures(sport_id, template_id, season_code, league_slug=None,
                               initial_total=3, max_pages=20, delay=1.0,
                               embedded_matches=None):
    """
    Once (varsa) league_slug / embedded_matches ile HTML'deki
    initialFeeds['results'] ilk batch'ini alir (tr_ XHR'da hic yer almayan
    son ~100 mac). Sonra tr_ sayfalarini (1, 2, 3, ...) geriye dogru ceker.
    PL 2024/25: results(~109) + tr_1/2/3(~281) = 380 unique mac.
    """
    all_matches = {}

    if embedded_matches is not None:
        for m in embedded_matches:
            all_matches[m["match_id"]] = m
    elif league_slug:
        page = fetch_league_page(league_slug)
        for m in page["matches"]:
            all_matches[m["match_id"]] = m
        time.sleep(delay)

    seen_page_hashes = set()

    page = 1
    consecutive_empty = 0
    while page <= max_pages:
        matches = fetch_results_page(sport_id, template_id, season_code, page, initial_total)
        if matches is None:
            print("    [!] sayfa " + str(page) + " hata verdi, durduruluyor", file=sys.stderr)
            break
        if not matches:
            consecutive_empty += 1
            print("    [debug] sayfa " + str(page) + " bos", file=sys.stderr)
            if consecutive_empty >= 2:
                break
            page += 1
            time.sleep(delay)
            continue

        # Ayni sayfa tekrar geliyorsa (server page'i yok sayip hep ayniyi
        # donduruyorsa) sonsuz donguye girmemek icin kontrol.
        page_hash = tuple(sorted(m["match_id"] for m in matches))
        if page_hash in seen_page_hashes:
            print("    [debug] sayfa " + str(page) + " tekrar (yeni veri yok), durduruluyor", file=sys.stderr)
            break
        seen_page_hashes.add(page_hash)
        consecutive_empty = 0

        new_count = 0
        for m in matches:
            if m["match_id"] not in all_matches:
                all_matches[m["match_id"]] = m
                new_count += 1
        print("    [+] sayfa " + str(page) + ": " + str(len(matches)) +
              " mac (" + str(new_count) + " yeni)", file=sys.stderr)

        if new_count == 0:
            break

        page += 1
        time.sleep(delay)

    return list(all_matches.values())


# Yanlis template+season birlesince tr_ feed baska lig (cogunlukla PL) dondurur.
_PL_TEAM_MARKERS = {
    "arsenal", "chelsea", "liverpool", "tottenham", "man utd", "man city",
    "west ham", "newcastle", "brighton", "crystal palace", "brentford",
    "wolves", "leicester", "everton", "aston villa", "fulham", "bournemouth",
    "southampton", "leeds", "burnley", "watford", "norwich", "nottingham",
    "ipswich", "sheffield utd", "luton",
}


def _reject_mixed_league(matches, league_slug):
    """england disi slug'da PL takimlari varsa hard-fail (yanlis template)."""
    if not league_slug or not matches:
        return
    country = league_slug.strip("/").split("/")[0].lower()
    if country in ("england", "eng"):
        return
    names = set()
    for m in matches:
        for k in ("home_name", "away_name"):
            n = (m.get(k) or "").strip().lower()
            if n:
                names.add(n)
    hit = sorted(names & _PL_TEAM_MARKERS)
    if len(hit) >= 4:
        print(
            "[!] KARISIK LIG: '" + league_slug + "' cekilirken Premier League "
            "takimlari gorundu (" + ", ".join(hit[:8]) + "). "
            "Muhtemel neden: yanlis --template-id (orn. PL dYlOSQOD) + baska "
            "lig seasonId. --template-id bos birak; slug sayfasindan okunsun.",
            file=sys.stderr,
        )
        sys.exit(3)


def _odds_headers(referer="https://www.flashscore.co.uk/"):
    headers = dict(HEADERS)
    headers.update({
        "accept": "*/*",
        "origin": "https://www.flashscore.co.uk",
        "referer": referer or "https://www.flashscore.co.uk/",
    })
    return headers


def fetch_odds(event_id, referer):
    url = ODDS_ENDPOINT.format(event_id=event_id)
    resp = requests.get(url, headers=_odds_headers(referer), timeout=20)
    resp.raise_for_status()
    return resp.json()


def parse_odds_bookmaker_ids(mw):
    """Gunluk feed MW alani: '16|841|28|...' → [16, 841, 28, ...]"""
    out = []
    seen = set()
    for part in str(mw or "").split("|"):
        part = part.strip()
        if not part.isdigit():
            continue
        bid = int(part)
        if bid in seen:
            continue
        seen.add(bid)
        out.append(bid)
    return out


def _overview_item_row(bookmaker_id, betting_type, betting_scope, side, item):
    if not item or not isinstance(item, dict):
        return None
    val = item.get("value")
    if val is None or val == "":
        return None
    return [
        bookmaker_id,
        betting_type,
        betting_scope,
        side,
        item.get("opening"),
        val,
        item.get("active", True),
    ]


def flatten_prematch_overview(overview, betting_scope="FULL_TIME"):
    """HAR ope2 findPrematchOddsForBookmaker → compact rows."""
    if not overview or not isinstance(overview, dict):
        return []
    bookmaker_id = overview.get("bookmakerId")
    betting_type = overview.get("type") or overview.get("bettingType")
    if bookmaker_id is None or not betting_type:
        return []
    rows = []
    scope = betting_scope or "FULL_TIME"

    def add(side, item):
        row = _overview_item_row(bookmaker_id, betting_type, scope, side, item)
        if row:
            rows.append(row)

    typename = overview.get("__typename") or ""

    if betting_type == "HOME_DRAW_AWAY" or "HomeDrawAway" in typename:
        add("H", overview.get("home"))
        add("D", overview.get("draw"))
        add("A", overview.get("away"))
        return rows

    if betting_type == "BOTH_TEAMS_TO_SCORE" or "BothTeamsToScore" in typename:
        add("btts:YES", overview.get("yes"))
        add("btts:NO", overview.get("no"))
        return rows

    if betting_type == "DOUBLE_CHANCE" or "DoubleChance" in typename:
        add("DC:1X", overview.get("homeOrDraw"))
        add("DC:X2", overview.get("awayOrDraw"))
        add("DC:12", overview.get("noDraw"))
        return rows

    if betting_type == "OVER_UNDER" or "OverUnder" in typename:
        for opp in overview.get("opportunities") or []:
            line = _handicap_line((opp or {}).get("handicap"))
            if line is None:
                continue
            add("OVER:" + line, (opp or {}).get("over"))
            add("UNDER:" + line, (opp or {}).get("under"))
        return rows

    if betting_type == "ASIAN_HANDICAP" or "AsianHandicap" in typename:
        for opp in overview.get("opportunities") or []:
            line = _handicap_line((opp or {}).get("handicap"))
            if line is None:
                continue
            add("H:" + line, (opp or {}).get("home"))
            add("A:" + line, (opp or {}).get("away"))
        return rows

    if betting_type == "CORRECT_SCORE" or "CorrectScore" in typename:
        for it in overview.get("items") or []:
            score = (it or {}).get("score")
            if score is None:
                continue
            add("score:" + str(score), (it or {}).get("item"))
        return rows

    # Bilinmeyen overview: home/draw/away varsa 1X2 gibi dene
    if overview.get("home") or overview.get("away"):
        add("H", overview.get("home"))
        add("D", overview.get("draw"))
        add("A", overview.get("away"))
    return rows


def fetch_prematch_menu(event_id, referer="https://www.flashscore.co.uk/"):
    url = PREMATCH_MENU_ENDPOINT.format(event_id=event_id)
    resp = requests.get(url, headers=_odds_headers(referer), timeout=20)
    resp.raise_for_status()
    root = (resp.json().get("data") or {}).get("getPrematchOddsBettingTypeMenu") or {}
    settings = root.get("settings") or {}
    bm_names = {}
    bm_ids = []
    for b in settings.get("bookmakers") or []:
        bm = b.get("bookmaker") if isinstance(b, dict) else None
        if not isinstance(bm, dict):
            continue
        bid = bm.get("id")
        if bid is None:
            continue
        bm_ids.append(int(bid))
        bm_names[str(bid)] = bm.get("name") or str(bid)
    markets = []
    for it in root.get("items") or []:
        bt = it.get("bettingType")
        bs = it.get("bettingScope") or "FULL_TIME"
        if bt:
            markets.append((bt, bs))
        # Menu item kendi bookmaker listesini tasir
        for bid in it.get("bookmakerIds") or []:
            try:
                bid_i = int(bid)
            except (TypeError, ValueError):
                continue
            if bid_i not in bm_ids:
                bm_ids.append(bid_i)
            bm_names.setdefault(str(bid_i), str(bid_i))
    return bm_ids, bm_names, markets or list(PREMATCH_DEFAULT_MARKETS)


def fetch_prematch_overview(event_id, bookmaker_id, bet_type, bet_scope, referer):
    url = PREMATCH_ODDS_ENDPOINT.format(
        event_id=event_id,
        bookmaker_id=bookmaker_id,
        bet_type=bet_type,
        bet_scope=bet_scope,
    )
    resp = requests.get(url, headers=_odds_headers(referer), timeout=20)
    if resp.status_code >= 400:
        return None
    try:
        data = resp.json()
    except Exception:
        return None
    return (data.get("data") or {}).get("findPrematchOddsForBookmaker")


def fetch_prematch_odds_for_match(m, referer="https://www.flashscore.co.uk/", delay=0.05):
    """
    oce bosken HAR akisi: pobtm menu + ope2 (MW bookmaker id'leri oncelikli).
    Gunluk feed'de MW = oran veren bookmaker listesi; GB oce bunlari atlayabilir.
    """
    event_id = m.get("match_id")
    if not event_id:
        return None, None

    mw_ids = parse_odds_bookmaker_ids(
        m.get("odds_bookmaker_ids") or m.get("MW") or m.get("mw")
    )
    bm_names = {}
    markets = list(PREMATCH_DEFAULT_MARKETS)
    menu_ids = []
    try:
        menu_ids, bm_names, markets = fetch_prematch_menu(event_id, referer=referer)
    except Exception as e:
        print("    [debug-odds] " + str(event_id) + " prematch_menu hata: " +
              str(e), file=sys.stderr)

    print("    [debug-odds] " + str(event_id) + " mw_ids=" + str(mw_ids) +
          " menu_ids=" + str(menu_ids), file=sys.stderr)

    # MW once (geo-disi bookmaker'lar burada), sonra menu
    ordered = []
    seen = set()
    for bid in mw_ids + menu_ids:
        if bid in seen:
            continue
        seen.add(bid)
        ordered.append(bid)
        bm_names.setdefault(str(bid), str(bid))
    if not ordered:
        print("    [debug-odds] " + str(event_id) + " ordered bos -> odds yok",
              file=sys.stderr)
        return None, None

    ordered = ordered[:PREMATCH_MAX_BOOKMAKERS]
    rows = []
    none_count = 0
    for bid in ordered:
        for bet_type, bet_scope in markets:
            overview = fetch_prematch_overview(
                event_id, bid, bet_type, bet_scope, referer
            )
            if overview is None:
                none_count += 1
            rows.extend(flatten_prematch_overview(overview, bet_scope))
            if delay and delay > 0:
                time.sleep(delay)
    print("    [debug-odds] " + str(event_id) + " rows=" + str(len(rows)) +
          " overview_none=" + str(none_count) + "/" + str(len(ordered) * len(markets)),
          file=sys.stderr)
    if not rows:
        return None, None
    return {"odds": rows, "bookmakers": bm_names}, None


def _handicap_line(handicap):
    """Flashscore handicap objesi / skaleri -> '2.5' gibi string."""
    if handicap is None:
        return None
    if isinstance(handicap, dict):
        v = handicap.get("value")
        return None if v is None else str(v)
    return str(handicap)


def _side_token(item, home_id, away_id, betting_type=None):
    """
    Tek token: web/DB'de secimi kayipsiz tasir.
      H / D / A
      DC:1X / DC:12 / DC:X2
      htft:1/1 ... (HALF_FULL_TIME winner alani)
      H:0.5 / A:-0.25
      OVER:2.5 / UNDER:2.5
      score:2:1
      btts:YES / btts:NO
    """
    pid = item.get("eventParticipantId")
    line = _handicap_line(item.get("handicap"))
    selection = item.get("selection")
    score = item.get("score")
    btts = item.get("bothTeamsToScore")
    winner = item.get("winner")

    # HT/FT: Flashscore "winner" alaninda "1/X", "X/2" vb. tasir
    if betting_type == "HALF_FULL_TIME" and winner:
        return "htft:" + str(winner)

    # Double Chance: home=1X, away=X2, null=12
    if betting_type == "DOUBLE_CHANCE":
        if pid == home_id:
            return "DC:1X"
        if pid == away_id:
            return "DC:X2"
        return "DC:12"

    if betting_type == "BOTH_TEAMS_TO_SCORE" and btts is not None:
        return "btts:YES" if btts else "btts:NO"

    if pid:
        if pid == home_id:
            side = "H"
        elif pid == away_id:
            side = "A"
        else:
            side = "p:" + str(pid)
        if line is not None:
            return side + ":" + line
        return side

    if selection is not None and line is not None:
        return str(selection) + ":" + line
    if selection is not None:
        return "sel:" + str(selection)
    if score is not None:
        return "score:" + str(score)
    if btts is not None:
        return "btts:YES" if btts else "btts:NO"
    if line is not None:
        # European handicap draw line
        return "D:" + line
    return "D"


def flatten_compact(odds_json, home_id, away_id):
    """
    Donen: (bookmakers_dict, compact_rows)
    compact_rows: [[bm_id, type, scope, side, opening, current, active], ...]
    """
    root = odds_json["data"]["findOddsByEventId"]
    bm_names = {str(b["bookmaker"]["id"]): b["bookmaker"]["name"]
                for b in root["settings"]["bookmakers"]}
    rows = []
    for market in root.get("odds", []):
        bookmaker_id = market.get("bookmakerId")
        betting_type = market.get("bettingType")
        betting_scope = market.get("bettingScope")
        for item in market.get("odds", []):
            rows.append([
                bookmaker_id,
                betting_type,
                betting_scope,
                _side_token(item, home_id, away_id, betting_type),
                item.get("opening"),
                item.get("value"),
                item.get("active"),
            ])
    return bm_names, rows


def flatten_pretty(odds_json, home_name, away_name, home_id, away_id):
    """Eski verbose satir formati (--pretty)."""
    root = odds_json["data"]["findOddsByEventId"]
    bm_names = {str(b["bookmaker"]["id"]): b["bookmaker"]["name"]
                for b in root["settings"]["bookmakers"]}
    rows = []
    for market in root.get("odds", []):
        bookmaker_id = market.get("bookmakerId")
        bookmaker_name = bm_names.get(str(bookmaker_id), str(bookmaker_id))
        betting_type = market.get("bettingType")
        betting_scope = market.get("bettingScope")
        for item in market.get("odds", []):
            side = _side_token(item, home_id, away_id, betting_type)
            if side == "H":
                side_label, side_code = home_name, "home"
            elif side == "A":
                side_label, side_code = away_name, "away"
            elif side == "D":
                side_label, side_code = "DRAW", "draw"
            elif side.startswith("score:"):
                side_label, side_code = side[6:], "score"
            elif side.startswith("handicap:"):
                side_label, side_code = side, "handicap"
            elif side.startswith("sel:"):
                side_label, side_code = side[4:], "selection"
            else:
                side_label, side_code = side, "unknown"
            rows.append({
                "bookmaker_id": bookmaker_id, "bookmaker_name": bookmaker_name,
                "betting_type": betting_type, "betting_scope": betting_scope,
                "side_code": side_code, "side_label": side_label,
                "opening_odds": item.get("opening"), "current_odds": item.get("value"),
                "active": item.get("active"),
            })
    return rows


def fetch_odds_for_match(m, pretty=False):
    referer = "https://www.flashscore.co.uk/"
    try:
        odds_json = fetch_odds(m["match_id"], referer=referer)
    except Exception as e:
        return None, "odds istegi basarisiz: " + str(e)
    root = odds_json.get("data", {}).get("findOddsByEventId") or {}
    oce_markets = root.get("odds") or []
    print("    [debug-odds] " + str(m.get("match_id")) + " oce_markets=" +
          str(len(oce_markets)), file=sys.stderr)

    # GB comparison bos ama MW/ope2 dolu olabilir (oynanmamis / geo-kisitli).
    if len(oce_markets) == 0:
        if pretty:
            return None, None
        return fetch_prematch_odds_for_match(m, referer=referer)

    if pretty:
        rows = flatten_pretty(odds_json, m["home_name"], m["away_name"],
                              m["home_id"], m["away_id"])
        return {"odds": rows, "bookmakers": {}}, None
    bm_names, rows = flatten_compact(odds_json, m["home_id"], m["away_id"])
    return {"odds": rows, "bookmakers": bm_names}, None


def write_output(path, payload, use_gzip=True, pretty=False):
    """JSON yaz; .gz soneki veya use_gzip=True ise gzip. Kayipsiz."""
    if use_gzip and not path.endswith(".gz"):
        path = path + ".gz"
    if pretty:
        body = json.dumps(payload, ensure_ascii=False, indent=2)
    else:
        body = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
    raw = body.encode("utf-8")
    if path.endswith(".gz") or use_gzip:
        if not path.endswith(".gz"):
            path = path + ".gz"
        with gzip.open(path, "wb", compresslevel=9) as f:
            f.write(raw)
    else:
        with open(path, "wb") as f:
            f.write(raw)
    size_mb = os.path.getsize(path) / (1024 * 1024)
    print("    [debug] yazildi: " + path + " (" + "{:.2f}".format(size_mb) + " MB)",
          file=sys.stderr)
    return path


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--sport-id", type=int, default=198)
    ap.add_argument("--template-id", default=None,
                     help="orn. dYlOSQOD. --league-slug varsa sayfadan da okunabilir.")
    ap.add_argument("--season-code", default=None,
                     help="Flashscore ic sayisal kodu, orn. 184 (2024/25). "
                          "'2024-2025' DEGIL. --league-slug varsa sayfadaki "
                          "seasonId'den otomatik cozulur.")
    ap.add_argument("--league-slug", default=None,
                     help="orn. england/premier-league-2024-2025 veya "
                          "argentina/liga-profesional-2024. "
                          "Hash (#/stageId/) gerekmez / yok sayilir.")
    ap.add_argument("--league-base", default=None,
                     help="Ulke/lig prefix (sezon haric), orn. argentina/liga-profesional. "
                          "--season ile birlikte archive'dan dogru slug cozulur.")
    ap.add_argument("--season", default=None,
                     help="Sezon etiketi: 2024 veya 2019-2020 (Flashscore URL bicimi). "
                          "--league-base ile kullan.")
    ap.add_argument("--list-seasons", action="store_true",
                     help="--league-base archive sezonlarini listele ve cik.")
    ap.add_argument("--initial-total", type=int, default=3)
    ap.add_argument("--max-pages", type=int, default=20)
    ap.add_argument("--out", default="season_odds.json.gz",
                     help="Cikti yolu. Varsayilan .json.gz (gzip).")
    ap.add_argument("--pretty", action="store_true",
                     help="Eski verbose JSON (indent + uzun alan adlari). Buyuk dosya uretir.")
    ap.add_argument("--no-gzip", action="store_true",
                     help="Gzip kapali (duz JSON). GitHub 100MB limitine takilabilir.")
    ap.add_argument("--limit", type=int, default=None)
    ap.add_argument("--delay", type=float, default=1.0)
    ap.add_argument("--workers", type=int, default=5)
    args = ap.parse_args()

    if args.list_seasons:
        base = args.league_base or (
            _split_league_slug(args.league_slug)[0] if args.league_slug else None
        )
        if not base:
            print("[!] --list-seasons icin --league-base gerekli "
                  "(orn. argentina/liga-profesional).", file=sys.stderr)
            sys.exit(2)
        try:
            rows = fetch_archive_seasons(base)
        except Exception as e:
            print("[!] archive hata: " + str(e), file=sys.stderr)
            sys.exit(1)
        for row in rows:
            keys = ",".join(row["keys"] or ["?"])
            print(keys + "\t" + row["slug"] + "\t" + row["name"])
        print("[+] " + str(len(rows)) + " sezon (" + normalize_league_slug(base) + ")",
              file=sys.stderr)
        sys.exit(0 if rows else 1)

    league_slug = normalize_league_slug(args.league_slug) if args.league_slug else None
    league_base = normalize_league_slug(args.league_base) if args.league_base else None
    season_label = args.season

    # base+season veya yanlis birlestirilmis slug → archive cozumu
    if league_base and season_label:
        resolved, err = resolve_league_slug(
            league_slug=league_slug, league_base=league_base, season=season_label
        )
        if err or not resolved:
            sys.exit(2)
        league_slug = resolved
    elif league_slug and re.search(r"-\d{4}-\d{4}$", league_slug.split("/")[-1]):
        # PL tarzi varsayilan sezon listesi Arjantin'de 404 uretir; early resolve.
        base, season = _split_league_slug(league_slug)
        # Sadece sayfa 404'te duzeltilir (fetch_league_page); burada zorlama yok.
        _ = (base, season)

    if not league_slug and (not args.template_id or not args.season_code):
        print("[!] --league-slug  VEYA  (--league-base + --season)  VEYA  "
              "(--template-id + --season-code) gerekli.", file=sys.stderr)
        print("    Ornek:", file=sys.stderr)
        print("      python3 fetchseason.py --league-slug england/premier-league-2024-2025",
              file=sys.stderr)
        print("      python3 fetchseason.py --league-base argentina/liga-profesional "
              "--season 2024", file=sys.stderr)
        print("      python3 fetchseason.py --league-base argentina/liga-profesional "
              "--list-seasons", file=sys.stderr)
        sys.exit(2)

    template_id = args.template_id
    season_code = args.season_code
    embedded_matches = None

    # season-code '2024-2025' gibi slug ise sayisal degildir; league sayfasindan coz.
    if season_code is not None and not str(season_code).strip().isdigit():
        print("[!] --season-code '" + str(season_code) + "' sayisal degil "
              "(bu bir sezon adi, Flashscore kodu degil).", file=sys.stderr)
        if not league_slug:
            print("    Premier League 2024/25 icin dogru deger: 184", file=sys.stderr)
            print("    veya --league-slug / --league-base+--season ver, "
                  "script seasonId'yi sayfadan okusun.", file=sys.stderr)
            sys.exit(2)
        print("    league slug var; seasonId sayfadan okunacak, "
              "verilen '" + str(season_code) + "' yok sayilacak.", file=sys.stderr)
        season_code = None

    if league_slug:
        print("[+] Lig sayfasi cekiliyor (" + league_slug + ")...", file=sys.stderr)
        page = fetch_league_page(league_slug)
        if page.get("resolved_slug"):
            league_slug = page["resolved_slug"]
        if page.get("error") and not page.get("season_code"):
            print("[!] Lig sayfasi basarisiz: " + str(page.get("error")), file=sys.stderr)
            base, _season = _split_league_slug(league_slug)
            if base:
                print("    Dene: python3 fetchseason.py --league-base " + base +
                      " --list-seasons", file=sys.stderr)
            sys.exit(2)
        embedded_matches = page["matches"]
        # Slug sayfasi kaynak: CLI'daki eski/yanlis template (orn. PL dYlOSQOD)
        # baska ligin seasonId'si ile birlestirilirse tr_ feed Ingiltere karistirir.
        page_season = page.get("season_code")
        page_template = page.get("template_id")
        if page_season:
            if season_code is not None and str(season_code) != str(page_season):
                print(
                    "    [!] --season-code=" + str(season_code) +
                    " sayfadaki " + str(page_season) +
                    " ile uyusmuyor → sayfa degeri kullanilacak",
                    file=sys.stderr,
                )
            season_code = page_season
            print("    [+] season-code sayfadan: " + str(season_code), file=sys.stderr)
        if page_template:
            if template_id is not None and str(template_id) != str(page_template):
                print(
                    "    [!] --template-id=" + str(template_id) +
                    " sayfadaki " + str(page_template) +
                    " ile uyusmuyor → sayfa degeri kullanilacak "
                    "(aksi halde baska lig maclari karisir)",
                    file=sys.stderr,
                )
            template_id = page_template
            print("    [+] template-id sayfadan: " + str(template_id), file=sys.stderr)
        time.sleep(args.delay)
    args.league_slug = league_slug  # output meta + mixed-league check


    if not template_id or not season_code:
        print("[!] template-id / season-code cozulemedi.", file=sys.stderr)
        print("    template_id=" + str(template_id) +
              " season_code=" + str(season_code), file=sys.stderr)
        print("    --template-id ve --season-code'u elle ver "
              "(DevTools > Network > tr_1_198_...).", file=sys.stderr)
        sys.exit(2)

    if not str(season_code).strip().isdigit():
        print("[!] season-code hala sayisal degil: " + str(season_code), file=sys.stderr)
        sys.exit(2)

    print("[+] Fikstur listesi cekiliyor (template=" + template_id +
          " season_code=" + str(season_code) + ")...", file=sys.stderr)
    matches = fetch_all_season_fixtures(
        args.sport_id, template_id, str(season_code),
        league_slug=None,  # embedded_matches zaten alindi
        embedded_matches=embedded_matches or [],
        initial_total=args.initial_total,
        max_pages=args.max_pages, delay=args.delay,
    )
    print("[+] Toplam " + str(len(matches)) + " mac bulundu.", file=sys.stderr)
    _reject_mixed_league(matches, args.league_slug)

    if args.limit:
        matches = matches[:args.limit]

    # FT var / HT yoksa df_sur BA/BB ile tamamla (backfill'e gerek kalmasin).
    enrich_missing_ht(matches, workers=max(2, min(args.workers, 8)), delay=0.05)
    ft_pre = sum(1 for m in matches if m.get("home_score") not in (None, ""))
    ht_pre = sum(
        1 for m in matches
        if m.get("home_ht_score") not in (None, "")
        and m.get("away_ht_score") not in (None, "")
    )
    print("[+] HT kapsami (odds oncesi): " + str(ht_pre) + "/" + str(ft_pre),
          file=sys.stderr)

    results, errors, no_odds = [], [], 0
    bookmakers = {}
    total = len(matches)
    done_count = 0
    pretty = args.pretty

    def worker(m):
        time.sleep(args.delay)
        return fetch_odds_for_match(m, pretty=pretty)

    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = {pool.submit(worker, m): m for m in matches}
        for fut in as_completed(futures):
            m = futures[fut]
            label = str(m['home_name']) + " - " + str(m['away_name']) + " (" + str(m.get('round')) + ")"
            done_count += 1
            try:
                payload, err = fut.result()
            except Exception as e:
                err = "beklenmeyen hata: " + str(e)
                payload = None
            print("[" + str(done_count) + "/" + str(total) + "] " + label, file=sys.stderr)
            odds_rows = []
            if err:
                errors.append({"match_id": m["match_id"], "label": label, "error": err})
            elif payload is None:
                no_odds += 1
            else:
                odds_rows = payload.get("odds") or []
                for bid, bname in payload.get("bookmakers", {}).items():
                    bookmakers[str(bid)] = bname
            # Oransiz / hatali olsa bile fikstur + HT yaz — backfill gerekmesin.
            results.append({
                "match_id": m["match_id"],
                "round": m.get("round"),
                "kickoff_ts": m["kickoff_ts"],
                "home_name": m["home_name"],
                "away_name": m["away_name"],
                "home_score": m["home_score"],
                "away_score": m["away_score"],
                "home_ht_score": m.get("home_ht_score"),
                "away_ht_score": m.get("away_ht_score"),
                "home_id": m.get("home_id"),
                "away_id": m.get("away_id"),
                "odds": odds_rows,
            })

    if pretty:
        output = {
            "schema_version": 1,
            "template_id": template_id, "season_code": str(season_code),
            "league_slug": args.league_slug,
            "matches": results, "errors": errors, "no_odds_count": no_odds,
            "total_fixtures_found": total,
        }
    else:
        output = {
            "schema_version": 2,
            "odds_columns": ODDS_SCHEMA,
            "template_id": template_id, "season_code": str(season_code),
            "league_slug": args.league_slug,
            "bookmakers": bookmakers,
            "matches": results, "errors": errors, "no_odds_count": no_odds,
            "total_fixtures_found": total,
        }

    out_path = write_output(args.out, output,
                            use_gzip=not args.no_gzip,
                            pretty=pretty)
    ft_n = sum(1 for m in results if m.get("home_score") not in (None, ""))
    ht_n = sum(
        1 for m in results
        if m.get("home_ht_score") not in (None, "")
        and m.get("away_ht_score") not in (None, "")
    )
    print("[+] Bitti. Basarili: " + str(len(results)) + ", Oransiz: " + str(no_odds) +
          ", Hatali: " + str(len(errors)) + " / Toplam fikstur: " + str(total) +
          ", HT: " + str(ht_n) + "/" + str(ft_n) +
          ". Yazildi: " + out_path, file=sys.stderr)
    if ft_n >= 10 and ht_n < ft_n * 0.9:
        print("[!] UYARI: HT kapsami dusuk — BC/BD (2Y) feed'de eksik veya "
              "df_sur fallback yetersiz. Cikti yine de yazildi.", file=sys.stderr)
        sys.exit(4)
    if total == 0:
        print("[!] Hic fikstur bulunamadi. Olasi nedenler:", file=sys.stderr)
        print("    1) --season-code yanlis (sayisal Flashscore kodu olmali, orn. 184)",
              file=sys.stderr)
        print("    2) --template-id yanlis", file=sys.stderr)
        print("    3) --league-slug verilmedi / sayfa cekilemedi", file=sys.stderr)


if __name__ == "__main__":
    main()
