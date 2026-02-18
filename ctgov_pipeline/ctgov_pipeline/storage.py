from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple


def utcnow_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def connect(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    return conn


def init_db(conn: sqlite3.Connection) -> None:
    cur = conn.cursor()

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS trials (
            nct_id TEXT PRIMARY KEY,
            brief_title TEXT,
            official_title TEXT,
            acronym TEXT,
            overall_status TEXT,
            study_type TEXT,
            phase TEXT,
            phases_json TEXT,
            modality TEXT,
            enrollment INTEGER,
            lead_sponsor_name TEXT,
            lead_sponsor_class TEXT,
            has_results INTEGER,
            start_date TEXT,
            primary_completion_date TEXT,
            primary_completion_date_parsed TEXT,
            completion_date_parsed TEXT,
            last_update_post_date_parsed TEXT,
            results_first_post_date_parsed TEXT,
            conditions_json TEXT,
            interventions_json TEXT,
            intervention_types_json TEXT,
            contacts_json TEXT,
            location_count INTEGER,
            topic_tags_json TEXT,
            urgency_score INTEGER,
            major_score INTEGER,
            interesting_score INTEGER,
            total_score INTEGER,
            days_to_primary_completion INTEGER,
            score_reasons_json TEXT,
            pubmed_count INTEGER DEFAULT 0,
            pubmed_latest_date TEXT,
            last_pubmed_check_utc TEXT,
            last_synced_utc TEXT,
            raw_json TEXT
        )
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS pubmed_citations (
            nct_id TEXT NOT NULL,
            pmid TEXT NOT NULL,
            title TEXT,
            source TEXT,
            pub_date TEXT,
            doi TEXT,
            last_seen_utc TEXT,
            PRIMARY KEY (nct_id, pmid)
        )
        """
    )

    cur.execute("CREATE INDEX IF NOT EXISTS idx_trials_total_score ON trials(total_score DESC)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_trials_primary_completion ON trials(primary_completion_date_parsed)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_trials_last_update ON trials(last_update_post_date_parsed)")
    conn.commit()


def _json(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=False, sort_keys=False, default=str)


def _safe_int(x: Any) -> Optional[int]:
    if x is None:
        return None
    try:
        return int(x)
    except Exception:
        return None


def upsert_trial(
    conn: sqlite3.Connection,
    record: Dict[str, Any],
    *,
    topic_name: str,
    scores: Dict[str, Any],
    raw_json: Optional[Dict[str, Any]] = None,
) -> None:
    nct_id = record.get("nct_id")
    if not nct_id:
        return

    # merge topic tags
    cur = conn.cursor()
    cur.execute("SELECT topic_tags_json FROM trials WHERE nct_id = ?", (nct_id,))
    row = cur.fetchone()
    existing_tags: List[str] = []
    if row and row["topic_tags_json"]:
        try:
            existing_tags = json.loads(row["topic_tags_json"]) or []
        except Exception:
            existing_tags = []
    tags = list(dict.fromkeys(existing_tags + [topic_name]))  # dedup preserving order

    phase = None
    phases = record.get("phases") or []
    if isinstance(phases, list) and phases:
        phase = str(phases[0])

    payload = {
        "nct_id": nct_id,
        "brief_title": record.get("brief_title"),
        "official_title": record.get("official_title"),
        "acronym": record.get("acronym"),
        "overall_status": record.get("overall_status"),
        "study_type": record.get("study_type"),
        "phase": phase,
        "phases_json": _json(record.get("phases") or []),
        "modality": record.get("modality"),
        "enrollment": _safe_int(record.get("enrollment")),
        "lead_sponsor_name": record.get("lead_sponsor_name"),
        "lead_sponsor_class": record.get("lead_sponsor_class"),
        "has_results": 1 if record.get("has_results") else 0,
        "start_date": record.get("start_date"),
        "primary_completion_date": record.get("primary_completion_date"),
        "primary_completion_date_parsed": record.get("primary_completion_date_parsed"),
        "completion_date_parsed": record.get("completion_date_parsed"),
        "last_update_post_date_parsed": record.get("last_update_post_date_parsed"),
        "results_first_post_date_parsed": record.get("results_first_post_date_parsed"),
        "conditions_json": _json(record.get("conditions") or []),
        "interventions_json": _json(record.get("interventions") or []),
        "intervention_types_json": _json(record.get("intervention_types") or []),
        "contacts_json": _json(record.get("contacts") or {}),
        "location_count": _safe_int(record.get("location_count")),
        "topic_tags_json": _json(tags),
        "urgency_score": _safe_int(scores.get("urgency")),
        "major_score": _safe_int(scores.get("major")),
        "interesting_score": _safe_int(scores.get("interesting")),
        "total_score": _safe_int(scores.get("total")),
        "days_to_primary_completion": _safe_int(scores.get("days_to_primary_completion")),
        "score_reasons_json": _json(scores.get("reasons") or {}),
        "last_synced_utc": utcnow_iso(),
        "raw_json": _json(raw_json) if raw_json is not None else None,
    }

    cols = ", ".join(payload.keys())
    placeholders = ", ".join(["?"] * len(payload))
    updates = ", ".join([f"{k}=excluded.{k}" for k in payload.keys() if k != "nct_id"])

    cur.execute(
        f"""
        INSERT INTO trials ({cols})
        VALUES ({placeholders})
        ON CONFLICT(nct_id) DO UPDATE SET
          {updates}
        """,
        tuple(payload.values()),
    )
    conn.commit()


def update_pubmed_summary(
    conn: sqlite3.Connection,
    nct_id: str,
    pubmed_count: int,
    pubmed_latest_date: Optional[str],
) -> None:
    cur = conn.cursor()
    cur.execute(
        """
        UPDATE trials
        SET pubmed_count = ?,
            pubmed_latest_date = ?,
            last_pubmed_check_utc = ?
        WHERE nct_id = ?
        """,
        (int(pubmed_count), pubmed_latest_date, utcnow_iso(), nct_id),
    )
    conn.commit()


def upsert_pubmed_citations(
    conn: sqlite3.Connection,
    nct_id: str,
    citations: Iterable[Dict[str, Any]],
) -> None:
    cur = conn.cursor()
    now = utcnow_iso()
    for c in citations:
        pmid = str(c.get("pmid", "")).strip()
        if not pmid:
            continue
        cur.execute(
            """
            INSERT INTO pubmed_citations (nct_id, pmid, title, source, pub_date, doi, last_seen_utc)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(nct_id, pmid) DO UPDATE SET
              title=excluded.title,
              source=excluded.source,
              pub_date=excluded.pub_date,
              doi=excluded.doi,
              last_seen_utc=excluded.last_seen_utc
            """,
            (
                nct_id,
                pmid,
                c.get("title"),
                c.get("source"),
                c.get("pub_date"),
                c.get("doi"),
                now,
            ),
        )
    conn.commit()


def fetch_trials_for_digest(
    conn: sqlite3.Connection,
    *,
    readout_window_days: int,
    recently_completed_days: int,
) -> List[sqlite3.Row]:
    """Return actionable trials for a digest report."""
    cur = conn.cursor()
    # Filter by primary completion date proximity if available.
    # We pre-compute days_to_primary_completion during scoring.
    cur.execute(
        """
        SELECT *
        FROM trials
        WHERE days_to_primary_completion IS NOT NULL
          AND (
            (days_to_primary_completion BETWEEN 0 AND ?)
            OR (days_to_primary_completion BETWEEN -? AND -1)
          )
        ORDER BY total_score DESC, primary_completion_date_parsed ASC
        """,
        (int(readout_window_days), int(recently_completed_days)),
    )
    return list(cur.fetchall())


def fetch_actionable_nct_ids(
    conn: sqlite3.Connection,
    *,
    readout_window_days: int,
    recently_completed_days: int,
    limit: int,
) -> List[str]:
    cur = conn.cursor()
    cur.execute(
        """
        SELECT nct_id
        FROM trials
        WHERE days_to_primary_completion IS NOT NULL
          AND (
            (days_to_primary_completion BETWEEN 0 AND ?)
            OR (days_to_primary_completion BETWEEN -? AND -1)
          )
        ORDER BY total_score DESC
        LIMIT ?
        """,
        (int(readout_window_days), int(recently_completed_days), int(limit)),
    )
    return [r[0] for r in cur.fetchall()]
