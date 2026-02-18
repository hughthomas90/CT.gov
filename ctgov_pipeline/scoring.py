from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from typing import Any, Dict, List, Tuple


def _parse_iso_date(s: Any) -> date | None:
    if not s or not isinstance(s, str):
        return None
    try:
        return datetime.fromisoformat(s).date()
    except Exception:
        return None


def _normalize_phase(phases: List[str]) -> str:
    # CT.gov uses values like PHASE1, PHASE2, PHASE3, PHASE4, EARLY_PHASE1, NA, etc.
    if not phases:
        return "UNKNOWN"
    p = [str(x).upper() for x in phases if x]
    # Prefer the highest/most-advanced phase if multiple.
    order = [
        "PHASE4",
        "PHASE3",
        "PHASE2",
        "PHASE1",
        "EARLY_PHASE1",
    ]
    for o in order:
        if o in p:
            return o
    # handle combined strings like PHASE2/PHASE3
    joined = ",".join(p)
    for o in order:
        if o in joined:
            return o
    return p[0]


def score_urgency(
    primary_completion_date_iso: str | None,
    *,
    has_results: bool,
    pubmed_count: int = 0,
    today: date | None = None,
) -> Tuple[int, List[str], int | None]:
    """Score how urgent a trial is for commissioning.

    Returns (score_0_100, reasons, days_to_primary_completion).
    """
    reasons: List[str] = []
    today = today or date.today()
    d = _parse_iso_date(primary_completion_date_iso)
    if not d:
        return 0, ["No primary completion date available"], None

    delta_days = (d - today).days

    # "Soon" window (0..180): closer = higher
    if 0 <= delta_days <= 180:
        score = int(100 - (delta_days / 180) * 80)  # 100 -> 20
        reasons.append(f"Primary completion in {delta_days} days")
        return max(0, min(100, score)), reasons, delta_days

    # Recently completed (past 180 days): prioritize if no results/papers
    if -180 <= delta_days < 0:
        score = int(70 - (abs(delta_days) / 180) * 40)  # 70 -> 30
        reasons.append(f"Primary completion {abs(delta_days)} days ago")
        if not has_results:
            score += 15
            reasons.append("No posted results on CT.gov")
        if pubmed_count == 0:
            score += 15
            reasons.append("No linked PubMed citations found (yet)")
        return max(0, min(100, score)), reasons, delta_days

    # Far future or long-past: low urgency
    if delta_days > 180:
        return 0, [f"Primary completion is >180 days away ({delta_days} days)"], delta_days
    return 0, [f"Primary completion is >180 days ago ({abs(delta_days)} days ago)"], delta_days


def score_major(
    *,
    phases: List[str],
    enrollment: Any,
    sponsor_class: str | None,
    study_type: str | None,
    oversight_has_dmc: bool | None,
    is_fda_regulated_drug: bool | None,
    is_fda_regulated_device: bool | None,
) -> Tuple[int, List[str]]:
    reasons: List[str] = []
    score = 0

    phase_norm = _normalize_phase(phases)
    if phase_norm in {"PHASE3", "PHASE4"}:
        score += 40
        reasons.append(f"Phase {phase_norm.replace('PHASE', '')}")
    elif phase_norm == "PHASE2":
        score += 25
        reasons.append("Phase 2")
    elif phase_norm == "PHASE1":
        score += 10
        reasons.append("Phase 1")
    else:
        score += 5
        reasons.append(f"Phase: {phase_norm}")

    # Enrollment (size)
    try:
        n = int(enrollment) if enrollment is not None else None
    except Exception:
        n = None

    if n is not None:
        if n >= 2000:
            score += 35
            reasons.append(f"Large enrollment (n={n})")
        elif n >= 1000:
            score += 30
            reasons.append(f"Large enrollment (n={n})")
        elif n >= 500:
            score += 25
            reasons.append(f"Moderate-large enrollment (n={n})")
        elif n >= 200:
            score += 18
            reasons.append(f"Moderate enrollment (n={n})")
        elif n >= 100:
            score += 12
            reasons.append(f"Enrollment (n={n})")
        else:
            score += 5
            reasons.append(f"Small enrollment (n={n})")
    else:
        reasons.append("Enrollment unknown")

    # Sponsor class
    sc = (sponsor_class or "").upper().strip()
    if sc == "INDUSTRY":
        score += 20
        reasons.append("Industry-sponsored")
    elif sc == "NIH":
        score += 18
        reasons.append("NIH-sponsored")
    elif sc:
        score += 10
        reasons.append(f"Sponsor class: {sc}")
    else:
        score += 5
        reasons.append("Sponsor class unknown")

    st = (study_type or "").upper().strip()
    if st == "INTERVENTIONAL":
        score += 8
        reasons.append("Interventional study")
    elif st:
        score += 3
        reasons.append(f"Study type: {st}")

    if oversight_has_dmc is True:
        score += 5
        reasons.append("Has DMC/DSM board (oversightHasDmc=true)")

    if is_fda_regulated_drug is True:
        score += 3
        reasons.append("FDA-regulated drug")
    if is_fda_regulated_device is True:
        score += 3
        reasons.append("FDA-regulated device")

    return max(0, min(100, score)), reasons


DEFAULT_INTEREST_KEYWORDS = [
    ("first-in-human", 6),
    ("randomized", 4),
    ("double-blind", 4),
    ("platform", 4),
    ("adaptive", 4),
    ("pragmatic", 3),
    ("mRNA", 8),
    ("CRISPR", 8),
    ("gene therapy", 8),
    ("cell therapy", 7),
    ("CAR-T", 7),
    ("ADC", 7),
    ("bispecific", 6),
    ("AI", 5),
]


def score_interesting(
    record_text: str,
    interesting_keywords: List[Dict[str, Any]] | None = None,
) -> Tuple[int, List[str]]:
    """Keyword-driven interest score (configurable)."""
    reasons: List[str] = []
    score = 0
    text = record_text.lower()

    # topic-specific keywords from config
    if interesting_keywords:
        for item in interesting_keywords:
            if not isinstance(item, dict):
                continue
            kw = str(item.get("keyword", "")).strip()
            if not kw:
                continue
            w = int(item.get("weight", 5))
            if kw.lower() in text:
                score += w
                reasons.append(f"Keyword match: {kw} (+{w})")

    # generic keywords
    for kw, w in DEFAULT_INTEREST_KEYWORDS:
        if kw.lower() in text:
            score += w
            reasons.append(f"Signal term: {kw} (+{w})")

    # cap and normalize
    score = max(0, min(100, score))
    if not reasons:
        reasons.append("No interest keywords matched")
    return score, reasons


def total_score(major: int, urgency: int, interesting: int) -> int:
    # Weighted toward 'major' + 'urgency' for commissioning workflow
    return int(round(0.4 * major + 0.4 * urgency + 0.2 * interesting))


def score_trial(
    record: Dict[str, Any],
    *,
    interesting_keywords: List[Dict[str, Any]] | None = None,
    pubmed_count: int = 0,
    today: date | None = None,
) -> Dict[str, Any]:
    """Compute all scores and reasons for a normalized trial record."""
    today = today or date.today()
    urgency, urg_reasons, days_to_pc = score_urgency(
        record.get("primary_completion_date_parsed"),
        has_results=bool(record.get("has_results")),
        pubmed_count=pubmed_count,
        today=today,
    )
    major, major_reasons = score_major(
        phases=record.get("phases") or [],
        enrollment=record.get("enrollment"),
        sponsor_class=record.get("lead_sponsor_class"),
        study_type=record.get("study_type"),
        oversight_has_dmc=record.get("oversight_has_dmc"),
        is_fda_regulated_drug=record.get("is_fda_regulated_drug"),
        is_fda_regulated_device=record.get("is_fda_regulated_device"),
    )

    record_text = " ".join(
        [
            str(record.get("brief_title", "")),
            str(record.get("official_title", "")),
            " ".join(record.get("conditions") or []),
            " ".join(record.get("interventions") or []),
        ]
    )
    interesting, int_reasons = score_interesting(record_text, interesting_keywords=interesting_keywords)

    total = total_score(major, urgency, interesting)

    return {
        "urgency": urgency,
        "major": major,
        "interesting": interesting,
        "total": total,
        "days_to_primary_completion": days_to_pc,
        "reasons": {
            "urgency": urg_reasons,
            "major": major_reasons,
            "interesting": int_reasons,
        },
    }
