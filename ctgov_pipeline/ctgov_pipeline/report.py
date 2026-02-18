from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List

import pandas as pd


def _loads(x: Any, default: Any) -> Any:
    if x is None:
        return default
    if isinstance(x, (list, dict)):
        return x
    if not isinstance(x, str):
        return default
    try:
        return json.loads(x)
    except Exception:
        return default


def _utcnow() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def trial_url(nct_id: str) -> str:
    return f"https://clinicaltrials.gov/study/{nct_id}"


def _first_email(contacts_json: Any) -> str | None:
    c = _loads(contacts_json, {})
    if not isinstance(c, dict):
        return None
    for cc in c.get("central_contacts", []) or []:
        if isinstance(cc, dict) and cc.get("email"):
            return str(cc.get("email"))
    return None


def write_digest_markdown(rows: Iterable[Dict[str, Any]], out_path: Path) -> None:
    rows = list(rows)
    lines: List[str] = []
    lines.append(f"# CT.gov Trial Watch Digest")
    lines.append("")
    lines.append(f"_Generated: {_utcnow()}_")
    lines.append("")
    lines.append(f"Total actionable trials: **{len(rows)}**")
    lines.append("")
    # group by topic tag
    by_topic: Dict[str, List[Dict[str, Any]]] = {}
    for r in rows:
        tags = _loads(r.get("topic_tags_json"), [])
        if not tags:
            tags = ["(untagged)"]
        for t in tags:
            by_topic.setdefault(str(t), []).append(r)

    # stable ordering: alphabetical, but move untagged to end
    topic_names = sorted([t for t in by_topic.keys() if t != "(untagged)"]) + (["(untagged)"] if "(untagged)" in by_topic else [])

    for topic in topic_names:
        trs = sorted(by_topic[topic], key=lambda x: (-(x.get("total_score") or 0), x.get("primary_completion_date_parsed") or "9999-12-31"))
        lines.append(f"## {topic}")
        lines.append("")
        for r in trs[:25]:
            nct = r.get("nct_id")
            if not nct:
                continue
            title = (r.get("brief_title") or "").strip()
            phase = r.get("phase") or ""
            modality = r.get("modality") or ""
            sponsor = (r.get("lead_sponsor_name") or "").strip()
            status = (r.get("overall_status") or "").strip()
            pc = r.get("primary_completion_date") or r.get("primary_completion_date_parsed") or ""
            d2 = r.get("days_to_primary_completion")
            has_results = bool(r.get("has_results"))
            pubmed_count = r.get("pubmed_count") or 0
            score = r.get("total_score") or 0
            email = _first_email(r.get("contacts_json"))
            url = trial_url(str(nct))

            lines.append(f"### {nct}: {title}")
            lines.append("")
            lines.append(f"- **Total score:** {score}  |  **Phase:** {phase}  |  **Modality:** {modality}")
            lines.append(f"- **Sponsor:** {sponsor}")
            lines.append(f"- **Status:** {status}")
            lines.append(f"- **Primary completion:** {pc}  |  **Days to readout:** {d2}")
            lines.append(f"- **CT.gov results posted:** {'Yes' if has_results else 'No'}  |  **PubMed papers:** {pubmed_count}")
            if email:
                lines.append(f"- **Central contact email:** {email}")
            lines.append(f"- **Link:** {url}")
            # short reasons
            reasons = _loads(r.get("score_reasons_json"), {})
            if isinstance(reasons, dict):
                urg = reasons.get("urgency") or []
                maj = reasons.get("major") or []
                lines.append(f"- **Why flagged:** {', '.join([str(x) for x in (urg[:2] + maj[:2]) if x])}")
            lines.append("")

    out_path.write_text("\n".join(lines), encoding="utf-8")


def export_table(rows: Iterable[Dict[str, Any]], *, csv_path: Path | None = None, xlsx_path: Path | None = None) -> None:
    rows = list(rows)
    if not rows:
        return
    # Flatten JSON columns we care about
    flat: List[Dict[str, Any]] = []
    for r in rows:
        item = dict(r)
        item["conditions"] = ", ".join(_loads(item.get("conditions_json"), []))
        item["interventions"] = ", ".join(_loads(item.get("interventions_json"), []))
        item["intervention_types"] = ", ".join(_loads(item.get("intervention_types_json"), []))
        item["topic_tags"] = ", ".join(_loads(item.get("topic_tags_json"), []))
        item["contact_email"] = _first_email(item.get("contacts_json")) or ""
        item["ctgov_url"] = trial_url(item.get("nct_id"))
        flat.append(item)

    df = pd.DataFrame(flat)
    # Put the most useful columns first
    preferred = [
        "nct_id",
        "brief_title",
        "phase",
        "modality",
        "overall_status",
        "lead_sponsor_name",
        "lead_sponsor_class",
        "primary_completion_date",
        "primary_completion_date_parsed",
        "days_to_primary_completion",
        "has_results",
        "pubmed_count",
        "total_score",
        "major_score",
        "urgency_score",
        "interesting_score",
        "topic_tags",
        "contact_email",
        "ctgov_url",
    ]
    cols = [c for c in preferred if c in df.columns] + [c for c in df.columns if c not in preferred]
    df = df[cols]

    if csv_path:
        df.to_csv(csv_path, index=False)
    if xlsx_path:
        df.to_excel(xlsx_path, index=False)
