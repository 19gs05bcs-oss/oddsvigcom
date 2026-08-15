#!/usr/bin/env python3
"""
smart_match_report.py — "Akilli Analiz" (benzer mac) raporunu, Railway'de
(odds-web) 283 sezonun TAMAMINI belleğe almak yerine, burada (Koyeb worker)
TEK GECISTE stream ederek hesaplar.

Node tarafindaki src/lib/analysis/smartMatchReport.ts'in birebir Python
karsiligi — mantik degistirilmedi, sadece "tum arsivi listeye topla, sonra
diz" yerine "sezon sezon oku, ayni anda tum biriktiricileri guncelle, sezonu
at" streaming modeline cevrildi. Boylece bellek tavani ~1 sezon + biriktirici
boyutuyla sinirli kalir (283 sezon x sabit degil).

Kullanim: archive_cache_server.py, ArchiveCacheServer.iter_all_archive_matches()
uretecini bu modulun build_smart_match_report()'una verir.
"""
from __future__ import annotations

import re
from typing import Iterable, Iterator

PREFERRED_BM = 16

_P_TOKEN_RE = re.compile(r"^p:([^:]+)(?::(.+))?$")


# ---------------- kucuk yardimcilar (tableRows.ts / smartMatchReport.ts) ----------------

def num(v) -> float | None:
    if v is None or v == "":
        return None
    try:
        n = float(v)
    except (TypeError, ValueError):
        return None
    return n if n >= 1.01 else None


def pct_change(opening: float, closing: float) -> float:
    if opening < 1.01:
        return 0.0
    return ((closing - opening) / opening) * 100.0


def move_kind(opening: float | None, closing: float | None) -> str | None:
    if opening is None or closing is None or opening < 1.01 or closing < 1.01:
        return None
    ch = pct_change(opening, closing)
    if ch <= -2:
        return "shortened"
    if ch >= 2:
        return "lengthened"
    return "stable"


def odds_close(a: float, b: float, tol_pct: float) -> bool:
    if tol_pct <= 0:
        return round(a * 100) == round(b * 100)
    return abs(a - b) / b <= tol_pct


def outcome_1x2(h: float, a: float) -> str:
    if h > a:
        return "H"
    if h < a:
        return "A"
    return "D"


def side_label(side: str) -> str:
    if side == "H":
        return "1"
    if side == "D":
        return "X"
    if side == "A":
        return "2"
    return side


def normalize_side_token(side_tok: str, home_id=None, away_id=None) -> str:
    s = str(side_tok or "").strip()
    m = _P_TOKEN_RE.match(s)
    if not m:
        return s
    pid, line = m.group(1), m.group(2)
    base = None
    if home_id and pid == str(home_id):
        base = "H"
    elif away_id and pid == str(away_id):
        base = "A"
    if not base:
        return s
    return f"{base}:{line}" if line else base


def pick_odds(
    odds_rows: list | None,
    bm_id: int,
    mtype: str,
    scope: str,
    side: str,
    home_id=None,
    away_id=None,
) -> tuple[float | None, float | None]:
    """odds_rows: [[bm_id, btype, bscope, side, opening, current, active], ...]"""
    opening = closing = None
    for row in odds_rows or []:
        if not isinstance(row, (list, tuple)) or len(row) < 7:
            continue
        try:
            if int(row[0]) != bm_id:
                continue
        except (TypeError, ValueError):
            continue
        if row[1] != mtype or row[2] != scope:
            continue
        tok = normalize_side_token(str(row[3]), home_id, away_id)
        if ":" in side:
            if tok != side:
                continue
        else:
            base = tok[0] if tok[:2] in ("H:", "A:", "D:") else tok.split(":")[0]
            if base != side and tok != side:
                continue
        opening = num(row[4])
        closing = num(row[5])
        if closing is None:
            closing = opening
        break
    return opening, closing


def extract_1x2(odds_rows, bm_id: int, home_id=None, away_id=None):
    H = pick_odds(odds_rows, bm_id, "HOME_DRAW_AWAY", "FULL_TIME", "H", home_id, away_id)[1]
    D = pick_odds(odds_rows, bm_id, "HOME_DRAW_AWAY", "FULL_TIME", "D", home_id, away_id)[1]
    A = pick_odds(odds_rows, bm_id, "HOME_DRAW_AWAY", "FULL_TIME", "A", home_id, away_id)[1]
    if H is None or D is None or A is None:
        return None
    return {"H": H, "D": D, "A": A}


def favorite_side(H, D, A) -> str | None:
    vals = [(k, v) for k, v in (("H", H), ("D", D), ("A", A)) if v is not None]
    if not vals:
        return None
    vals.sort(key=lambda kv: kv[1])
    return vals[0][0]


def side_won(side: str, h: float, a: float, mtype: str, line: str | None = None) -> bool | None:
    if mtype == "HOME_DRAW_AWAY":
        return side == outcome_1x2(h, a)
    if mtype == "BOTH_TEAMS_TO_SCORE":
        yes = h > 0 and a > 0
        if re.search(r"YES|True", side, re.I):
            return yes
        if re.search(r"NO|False", side, re.I):
            return not yes
    if mtype == "OVER_UNDER" and line:
        total = h + a
        try:
            ln = float(line)
        except ValueError:
            return None
        over = total > ln
        if side.startswith("OVER"):
            return over
        if side.startswith("UNDER"):
            return not over
    return None


def bm_ids(odds_rows) -> list[int]:
    s: set[int] = set()
    for row in odds_rows or []:
        if not isinstance(row, (list, tuple)):
            continue
        try:
            n = int(row[0])
        except (TypeError, ValueError):
            continue
        if n > 0:
            s.add(n)
    return sorted(s)


def build_bm_grid(odds_rows, bookmakers: dict | None, home_id=None, away_id=None) -> list[dict]:
    bms = bookmakers or {}
    out = []
    for bid in bm_ids(odds_rows):
        pH = pick_odds(odds_rows, bid, "HOME_DRAW_AWAY", "FULL_TIME", "H", home_id, away_id)
        pD = pick_odds(odds_rows, bid, "HOME_DRAW_AWAY", "FULL_TIME", "D", home_id, away_id)
        pA = pick_odds(odds_rows, bid, "HOME_DRAW_AWAY", "FULL_TIME", "A", home_id, away_id)
        out.append({
            "id": str(bid),
            "name": bms.get(str(bid)) or f"#{bid}",
            "H": pH[1], "D": pD[1], "A": pA[1],
            "favorite": favorite_side(pH[1], pD[1], pA[1]),
            "moveH": move_kind(pH[0], pH[1]),
            "moveD": move_kind(pD[0], pD[1]),
            "moveA": move_kind(pA[0], pA[1]),
        })
    return out


def count_outcomes(rows: list[dict]) -> dict:
    stats = {"n": len(rows), "H": 0, "D": 0, "A": 0, "top": None, "topPct": 0.0}
    for r in rows:
        stats[r["outcome"]] += 1
    if not stats["n"]:
        return stats
    entries = sorted((("H", stats["H"]), ("D", stats["D"]), ("A", stats["A"])), key=lambda e: -e[1])
    stats["top"] = entries[0][0]
    stats["topPct"] = (entries[0][1] / stats["n"]) * 100.0
    return stats


# ---------------- streaming rapor ----------------

class _Accumulators:
    """Tek geçişte tüm sezonlar üzerinde güncellenen biriktiriciler."""

    __slots__ = (
        "similar_samples", "similar_limit",
        "profile", "bm", "tol",
        "move_specs", "move_stats",
        "consensus_favorite", "consensus_profile", "consensus_tol",
        "consensus_samples",
        "archive_count",
    )

    def __init__(self):
        self.similar_samples: list[dict] = []
        self.similar_limit = 120
        self.profile = None
        self.bm = PREFERRED_BM
        self.tol = 0.03
        # her hareket adayı için: {key: (mtype, scope, side, opening, closing, move)}
        self.move_specs: dict[str, tuple] = {}
        self.move_stats: dict[str, dict] = {}
        self.consensus_favorite = None
        self.consensus_profile = None
        self.consensus_tol = 0.03
        self.consensus_samples: list[dict] = []
        self.archive_count = 0


def _process_event(acc: _Accumulators, m: dict) -> None:
    """m: {id, season, home, away, kickoff, homeScore, awayScore, odds, bookmakers}"""
    acc.archive_count += 1
    odds = m.get("odds") or []
    h, a = m.get("homeScore"), m.get("awayScore")
    if h is None or a is None:
        return

    # 1) similar1x2
    if acc.profile is not None and len(acc.similar_samples) < acc.similar_limit:
        tri = extract_1x2(odds, acc.bm)
        if tri and odds_close(tri["H"], acc.profile["H"], acc.tol) \
                and odds_close(tri["D"], acc.profile["D"], acc.tol) \
                and odds_close(tri["A"], acc.profile["A"], acc.tol):
            acc.similar_samples.append({
                "id": m["id"], "season": m.get("season"),
                "home": m.get("home"), "away": m.get("away"),
                "kickoff": m.get("kickoff"),
                "score": f"{h}-{a}", "outcome": outcome_1x2(h, a),
                "oddsH": tri["H"], "oddsD": tri["D"], "oddsA": tri["A"],
                "odds": odds,
            })

    # 2) hareket (movement) geçmiş istatistikleri
    for key, (mtype, scope, side, opening, closing, move) in acc.move_specs.items():
        p = pick_odds(odds, acc.bm, mtype, scope, side)
        if p[0] is None or p[1] is None:
            continue
        if not odds_close(p[1], closing, acc.tol):
            continue
        if move_kind(p[0], p[1]) != move:
            continue
        won = side_won(side, h, a, mtype)
        if won is None:
            continue
        st = acc.move_stats.setdefault(key, {"n": 0, "wins": 0, "reversed": 0, "sumImpl": 0.0})
        st["n"] += 1
        if won:
            st["wins"] += 1
        st["sumImpl"] += 1 / p[1]
        implied_fav = move == "shortened"
        if won != implied_fav:
            st["reversed"] += 1

    # 3) konsensüs geçmişi
    if acc.consensus_favorite and acc.consensus_profile:
        grid = build_bm_grid(odds, m.get("bookmakers"))
        if len(grid) >= 10:
            tri = extract_1x2(odds, PREFERRED_BM)
            if tri and odds_close(tri["H"], acc.consensus_profile["H"], acc.consensus_tol * 1.5) \
                    and odds_close(tri["D"], acc.consensus_profile["D"], acc.consensus_tol * 1.5) \
                    and odds_close(tri["A"], acc.consensus_profile["A"], acc.consensus_tol * 1.5):
                fav_count = sum(1 for g in grid if g["favorite"] == acc.consensus_favorite)
                if fav_count / len(grid) >= 0.75:
                    acc.consensus_samples.append({
                        "id": m["id"], "season": m.get("season"),
                        "home": m.get("home"), "away": m.get("away"),
                        "kickoff": m.get("kickoff"),
                        "score": f"{h}-{a}", "outcome": outcome_1x2(h, a),
                        "oddsH": tri["H"], "oddsD": tri["D"], "oddsA": tri["A"],
                    })


def build_smart_match_report(
    fixture: dict,
    archive_iter: Iterable[dict],
    archive_source: str | None = None,
    reference_bm: int | None = None,
    tolerance_pct: float = 0.03,
) -> dict:
    """
    fixture: {match_id, home_name, away_name, kickoff_at, league, odds,
              bookmakers, home_id, away_id}
    archive_iter: her elemanı {id, season, home, away, kickoff, homeScore,
              awayScore, odds, bookmakers} olan, TEK GEÇİŞLİK (streaming)
              iterable — çağıran taraf (archive_cache_server.py) bunu
              sezon sezon üretir, bellekte biriktirmez.
    """
    bm = reference_bm if reference_bm is not None else PREFERRED_BM
    tol = tolerance_pct if tolerance_pct is not None else 0.03
    home = fixture.get("home_name") or "Home"
    away = fixture.get("away_name") or "Away"
    f_odds = fixture.get("odds") or []
    f_bms = fixture.get("bookmakers") or {}
    home_id = fixture.get("home_id")
    away_id = fixture.get("away_id")

    profile = extract_1x2(f_odds, bm, home_id, away_id)
    grid = build_bm_grid(f_odds, f_bms, home_id, away_id)

    fav_counts = {"H": 0, "D": 0, "A": 0}
    for g in grid:
        if g["favorite"]:
            fav_counts[g["favorite"]] += 1
    total_bm = len(grid)
    top_fav = max(fav_counts, key=lambda k: fav_counts[k]) if total_bm else None
    aligned_count = fav_counts.get(top_fav, 0) if top_fav else 0
    aligned = total_bm > 0 and (aligned_count / total_bm) >= 0.7

    key_markets = [
        ("HOME_DRAW_AWAY", "FULL_TIME", "H", None),
        ("HOME_DRAW_AWAY", "FULL_TIME", "D", None),
        ("HOME_DRAW_AWAY", "FULL_TIME", "A", None),
        ("BOTH_TEAMS_TO_SCORE", "FULL_TIME", "btts:YES", None),
        ("BOTH_TEAMS_TO_SCORE", "FULL_TIME", "btts:NO", None),
        ("OVER_UNDER", "FULL_TIME", "OVER", "2.5"),
        ("OVER_UNDER", "FULL_TIME", "UNDER", "2.5"),
    ]

    acc = _Accumulators()
    acc.bm = bm
    acc.tol = tol
    acc.profile = profile
    acc.similar_limit = 120
    acc.consensus_tol = tol
    if profile and aligned and top_fav:
        acc.consensus_favorite = top_fav
        acc.consensus_profile = profile

    movements_pre = []
    for mtype, scope, side, line in key_markets:
        side_tok = f"{side}:{line}" if (line and mtype == "OVER_UNDER") else side
        p = pick_odds(f_odds, bm, mtype, scope, side_tok, home_id, away_id)
        if p[0] is None or p[1] is None:
            continue
        move = move_kind(p[0], p[1])
        if not move or move == "stable":
            continue
        key = f"{mtype}|{scope}|{side_tok}"
        acc.move_specs[key] = (mtype, scope, side_tok, p[0], p[1], move)
        movements_pre.append((key, mtype, scope, side_tok, side, p[0], p[1], move))

    # ---- TEK GEÇİŞ: tüm sezonları stream et, tüm biriktiricileri güncelle ----
    for m in archive_iter:
        _process_event(acc, m)

    similar1x2 = {**count_outcomes(acc.similar_samples), "samples": acc.similar_samples[:40]}

    consensus_hist = count_outcomes(acc.consensus_samples) if acc.consensus_samples and len(acc.consensus_samples) >= 10 else None

    movements = []
    for key, mtype, scope, side_tok, side, opening, closing, move in movements_pre:
        st = acc.move_stats.get(key)
        historical = None
        if st and st["n"] >= 8:
            win_pct = st["wins"] / st["n"]
            implied_pct = st["sumImpl"] / st["n"]
            reversed_pct = st["reversed"] / st["n"]
            note = (
                f"Oran düştü (steam) — geçmişte bu seçim {win_pct*100:.0f}% isabet; tersine {reversed_pct*100:.0f}%"
                if move == "shortened" else
                f"Oran uzadı (drift) — geçmişte {win_pct*100:.0f}% isabet; beklenenin tersi {reversed_pct*100:.0f}%"
            )
            historical = {"n": st["n"], "winPct": win_pct, "impliedPct": implied_pct, "reversedPct": reversed_pct, "note": note}
        ch = pct_change(opening, closing)
        movements.append({
            "market": mtype, "scope": scope, "side": side_tok,
            "sideLabel": side_label(side.split(":")[0]),
            "bookmakerId": str(bm), "bookmakerName": f_bms.get(str(bm)) or f"#{bm}",
            "opening": opening, "closing": closing,
            "changePct": round(ch * 10) / 10, "move": move,
            "historical": historical,
        })
    movements.sort(key=lambda mv: -abs(mv["changePct"]))

    summary = []
    if profile and similar1x2["n"] >= 5:
        summary.append(
            f"MS 1X2 profiline ±{tol*100:.0f}% uyan {similar1x2['n']} maçta en sık sonuç "
            f"MS {side_label(similar1x2['top'] or 'D')} (%{similar1x2['topPct']:.0f})."
        )
    elif profile:
        summary.append(f"MS 1X2 profiline uyan yeterli arşiv maçı yok (n={similar1x2['n']}). Toleransı artırın.")
    if aligned and total_bm:
        extra = f" Geçmişte aynı BM uyumunda MS {side_label((consensus_hist or {}).get('top') or top_fav)} %{consensus_hist['topPct']:.0f}." if consensus_hist else ""
        summary.append(f"{aligned_count}/{total_bm} bookmaker favoriyi MS {side_label(top_fav)} olarak gösteriyor.{extra}")
    elif total_bm:
        summary.append(f"Bookmaker'lar dağılmış — tek yönde güçlü konsensüs yok (en çok MS {side_label(top_fav)}: {aligned_count}/{total_bm}).")
    if movements and movements[0].get("historical"):
        summary.append(movements[0]["historical"]["note"])

    return {
        "home": home, "away": away, "kickoff": fixture.get("kickoff_at"),
        "league": fixture.get("league"), "referenceBm": bm, "tolerancePct": tol,
        "archiveMatches": acc.archive_count, "archiveSource": archive_source,
        "profile1x2": profile, "similar1x2": similar1x2,
        "movements": movements, "bookmakerGrid": grid,
        "consensus": {
            "favorite": top_fav if total_bm else None,
            "counts": fav_counts, "aligned": aligned,
            "alignedPct": (aligned_count / total_bm * 100.0) if total_bm else 0.0,
            "historical": consensus_hist,
        },
        "summary": summary,
    }
