#!/usr/bin/env python3
"""
Compact season odds (schema_version=2) -> web-friendly markets_json.

Varsayilan (full):
  selection.bookmakers = {bm_id: {opening, current, active}}  # TUM bookmaker'lar
  selection.odds / opening / bookmaker_id = en iyi aktif oran (UI ozeti)
  blob.odds + blob.odds_columns = GitHub .json.gz ile ayni ham satirlar

Slim (--slim import): sadece en iyi oran; bookmakers grid yok (onerilmez).
"""
from __future__ import annotations

import hashlib
import json
import re
from typing import Any

# fetchseason.py ODDS_SCHEMA ile ayni
ODDS_COLUMNS = [
    "bookmaker_id", "betting_type", "betting_scope",
    "side", "opening_odds", "current_odds", "active",
]

SCOPE_LABEL = {
    "FULL_TIME": "",
    "FIRST_HALF": "1st Half",
    "SECOND_HALF": "2nd Half",
}

TYPE_LABEL = {
    "HOME_DRAW_AWAY": "1X2",
    "DOUBLE_CHANCE": "Double Chance",
    "DRAW_NO_BET": "Draw No Bet",
    "BOTH_TEAMS_TO_SCORE": "BTTS",
    "OVER_UNDER": "Over/Under",
    "ASIAN_HANDICAP": "Asian Handicap",
    "EUROPEAN_HANDICAP": "European Handicap",
    "CORRECT_SCORE": "Correct Score",
    "HALF_FULL_TIME": "HT/FT",
    "ODD_OR_EVEN": "Odd/Even",
}

# Flashscore HT/FT winner kodlari
HTFT_LABEL = {
    "1/1": "1/1",
    "1/X": "1/X",
    "1/2": "1/2",
    "X/1": "X/1",
    "X/X": "X/X",
    "X/2": "X/2",
    "2/1": "2/1",
    "2/X": "2/X",
    "2/2": "2/2",
}


def _f(x) -> float | None:
    if x is None or x == "":
        return None
    try:
        return float(x)
    except (TypeError, ValueError):
        return None


def market_title(betting_type: str, scope: str, line: str | None = None) -> str:
    base = TYPE_LABEL.get(betting_type, betting_type.replace("_", " ").title())
    sc = SCOPE_LABEL.get(scope, scope)
    parts = [base]
    if line:
        parts.append(line)
    if sc:
        parts.append(sc)
    return " · ".join(parts)


def normalize_side(side: str, betting_type: str) -> str:
    """Eski scrape H/D/A DC satirlari -> DC:1X/12/X2; BTTS True/False -> YES/NO."""
    if betting_type == "DOUBLE_CHANCE":
        if side in ("H", "DC:1X"):
            return "DC:1X"
        if side in ("A", "DC:X2"):
            return "DC:X2"
        if side in ("D", "DC:12"):
            return "DC:12"
    if betting_type == "BOTH_TEAMS_TO_SCORE":
        if side in ("btts:True", "btts:YES", "True"):
            return "btts:YES"
        if side in ("btts:False", "btts:NO", "False"):
            return "btts:NO"
    if side.startswith("line:") and betting_type == "EUROPEAN_HANDICAP":
        return "D:" + side[5:]
    return side


def selection_label(side: str, home: str, away: str, betting_type: str | None = None) -> str:
    home = home or "Home"
    away = away or "Away"
    bt = betting_type or ""

    if bt == "DOUBLE_CHANCE" or side.startswith("DC:"):
        side = normalize_side(side, "DOUBLE_CHANCE")
        if side == "DC:1X":
            return f"{home} or Draw (1X)"
        if side == "DC:X2":
            return f"Draw or {away} (X2)"
        if side == "DC:12":
            return f"{home} or {away} (12)"

    if side.startswith("htft:"):
        code = side[5:]
        pretty = HTFT_LABEL.get(code, code)
        # 1=home, X=draw, 2=away
        def part(p: str) -> str:
            if p == "1":
                return home
            if p == "2":
                return away
            if p.upper() == "X":
                return "Draw"
            return p
        if "/" in pretty:
            a, b = pretty.split("/", 1)
            return f"{part(a)} / {part(b)} ({pretty})"
        return pretty

    if side in ("btts:YES", "btts:True"):
        return "Yes"
    if side in ("btts:NO", "btts:False"):
        return "No"

    if side == "H":
        return home
    if side == "A":
        return away
    if side == "D":
        return "Draw"
    if side.startswith("score:"):
        return side[6:].replace(":", "-")
    if side.startswith("btts:"):
        return side[5:].replace("_", " ").title()
    if side.startswith("sel:"):
        return side[4:].replace("_", " ").title()
    if side.startswith("OVER:") or side.startswith("UNDER:"):
        kind, _, line = side.partition(":")
        return f"{kind.title()} {line}"
    if side.startswith("H:") or side.startswith("A:"):
        team = home if side[0] == "H" else away
        return f"{team} ({side[2:]})"
    if side.startswith("D:"):
        return f"Draw ({side[2:]})"
    if side.startswith("line:"):
        return f"Draw ({side[5:]})"
    if side.startswith("p:"):
        return side
    return side.replace("_", " ")


def parse_compact_row(row: list) -> dict[str, Any] | None:
    if not isinstance(row, list) or len(row) < 7:
        return None
    return {
        "bookmaker_id": str(row[0]) if row[0] is not None else None,
        "betting_type": row[1],
        "betting_scope": row[2],
        "side": str(row[3]),
        "opening": _f(row[4]),
        "current": _f(row[5]),
        "active": bool(row[6]) if row[6] is not None else True,
    }


def _line_from_side(side: str, betting_type: str) -> str | None:
    if betting_type == "OVER_UNDER":
        if ":" in side and not side.startswith("score:"):
            return side.split(":", 1)[1]
    if betting_type in ("ASIAN_HANDICAP", "EUROPEAN_HANDICAP"):
        if side.startswith(("H:", "A:", "D:", "line:")):
            return side.split(":", 1)[1]
    return None


def build_markets_blob(
    odds_rows: list,
    bookmakers: dict[str, str],
    home_name: str,
    away_name: str,
    *,
    include_raw_odds: bool = True,
) -> tuple[dict[str, Any], str, int]:
    """
    Return (markets_json, hash, selection_count).

    markets_json her zaman selection.bookmakers grid'ini tasir (tum BM).
    include_raw_odds=True ise GitHub gz ile ayni compact `odds` satirlarini da yazar.
    """
    nested: dict[str, dict[str, Any]] = {}
    bm_names = {str(k): v for k, v in (bookmakers or {}).items()}
    raw_odds: list = []

    for raw in odds_rows:
        r = parse_compact_row(raw)
        if not r or not r["betting_type"]:
            continue
        if "EventOddsItemHandicap" in r["side"] or "{'__typename'" in r["side"]:
            continue
        if include_raw_odds and isinstance(raw, list) and len(raw) >= 7:
            # Ham satir: bookmaker_id string tut (JSON anahtarlari ile tutarli)
            row = list(raw)
            if row[0] is not None:
                row[0] = str(row[0]) if not isinstance(row[0], str) else row[0]
            raw_odds.append(row)

        side = normalize_side(r["side"], r["betting_type"])
        line = _line_from_side(side, r["betting_type"])
        mkey = f"{r['betting_type']}:{r['betting_scope']}"
        if line and r["betting_type"] == "OVER_UNDER":
            mkey = f"{r['betting_type']}:{r['betting_scope']}:{line}"

        if mkey not in nested:
            nested[mkey] = {
                "key": mkey,
                "name": market_title(
                    r["betting_type"], r["betting_scope"],
                    line if r["betting_type"] == "OVER_UNDER" else None,
                ),
                "type": r["betting_type"],
                "scope": r["betting_scope"],
                "line": line if r["betting_type"] == "OVER_UNDER" else None,
                "selections": {},
            }

        if r["betting_type"] == "OVER_UNDER" and ":" in side:
            sk = side.split(":", 1)[0]
        else:
            sk = side

        sels = nested[mkey]["selections"]
        if sk not in sels:
            if r["betting_type"] == "OVER_UNDER":
                label = f"{sk.title()}" + (f" {line}" if line else "")
            else:
                label = selection_label(side, home_name, away_name, r["betting_type"])
            sels[sk] = {
                "key": sk,
                "name": label,
                "bookmakers": {},
            }

        if r["bookmaker_id"]:
            sels[sk]["bookmakers"][str(r["bookmaker_id"])] = {
                "opening": r["opening"],
                "current": r["current"],
                "active": r["active"],
            }

    markets = []
    sel_count = 0
    for mkey in sorted(nested.keys()):
        m = nested[mkey]
        selections = []
        for sk in sorted(m["selections"].keys()):
            s = m["selections"][sk]
            bms = s["bookmakers"]
            best = None
            best_opening = None
            best_bm = None
            suspended = True
            for bid, q in bms.items():
                if q.get("active") and q.get("current") is not None:
                    suspended = False
                    if best is None or q["current"] > best:
                        best = q["current"]
                        best_opening = q.get("opening")
                        best_bm = bid
            if best is None:
                for bid, q in bms.items():
                    if q.get("current") is not None:
                        best = q["current"]
                        best_opening = q.get("opening")
                        best_bm = bid
                        break
            selections.append({
                "key": s["key"],
                "name": s["name"],
                "odds": best,
                "opening": best_opening,
                "bookmaker_id": best_bm,
                "bookmaker_name": bm_names.get(str(best_bm)) if best_bm else None,
                "suspended": suspended if best is not None else True,
                # Tum BM ham grid — slim degil
                "bookmakers": bms,
            })
            sel_count += 1
        markets.append({
            "key": m["key"],
            "name": m["name"],
            "type": m["type"],
            "scope": m["scope"],
            "line": m["line"],
            "selections": selections,
        })

    priority = ["HOME_DRAW_AWAY", "DOUBLE_CHANCE", "BOTH_TEAMS_TO_SCORE",
                "OVER_UNDER", "DRAW_NO_BET", "ASIAN_HANDICAP", "HALF_FULL_TIME"]

    def sort_key(m):
        try:
            p = priority.index(m["type"])
        except ValueError:
            p = 99
        return (p, m["scope"] != "FULL_TIME", m["key"])

    markets.sort(key=sort_key)

    blob: dict[str, Any] = {
        "bookmakers": dict(bm_names),
        "markets": markets,
    }
    if include_raw_odds:
        blob["odds_columns"] = list(ODDS_COLUMNS)
        blob["odds"] = raw_odds
    raw = json.dumps(blob, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    digest = hashlib.sha256(raw.encode("utf-8")).hexdigest()
    return blob, digest, sel_count


def kickoff_iso(ts) -> str | None:
    if ts is None or ts == "":
        return None
    try:
        from datetime import datetime, timezone
        return datetime.fromtimestamp(int(ts), tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    except (TypeError, ValueError):
        return str(ts)


def competition_label(league_slug: str | None) -> str:
    if not league_slug:
        return "Unknown"
    leaf = league_slug.strip("/").split("/")[-1]
    m = re.match(r"^(.*)-(\d{4}-\d{4})$", leaf)
    if m:
        name = m.group(1).replace("-", " ").title()
        return f"{name} {m.group(2)}"
    m = re.match(r"^(.*)-(\d{4})$", leaf)
    if m:
        name = m.group(1).replace("-", " ").title()
        return f"{name} {m.group(2)}"
    return leaf.replace("-", " ").title()


def season_label_from_slug(league_slug: str | None) -> str | None:
    if not league_slug:
        return None
    leaf = league_slug.strip("/")
    m = re.search(r"(\d{4}-\d{4})$", leaf)
    if m:
        return m.group(1)
    m = re.search(r"(\d{4})$", leaf)
    return m.group(1) if m else None
