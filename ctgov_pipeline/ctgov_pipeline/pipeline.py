from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from typing import Iterable, List, Optional

from .config import AppConfig, TopicConfig
from .ctgov import CTGovClient
from .parsing import extract_trial_record
from .pubmed import PubMedClient
from .report import export_table, write_digest_markdown
from .scoring import score_trial
from .storage import (
    connect,
    fetch_actionable_nct_ids,
    fetch_trials_for_digest,
    init_db,
    upsert_pubmed_citations,
    upsert_trial,
    update_pubmed_summary,
)


def _topic_text_match(record: dict, keywords: List[str]) -> bool:
    if not keywords:
        return True
    hay = " ".join(
        [
            str(record.get("brief_title") or ""),
            str(record.get("official_title") or ""),
            " ".join(record.get("conditions") or []),
            " ".join(record.get("interventions") or []),
        ]
    ).lower()
    for kw in keywords:
        if str(kw).lower() in hay:
            return True
    return False


def _get_existing_pubmed_count(conn, nct_id: str) -> int:
    cur = conn.cursor()
    cur.execute("SELECT pubmed_count FROM trials WHERE nct_id = ?", (nct_id,))
    row = cur.fetchone()
    if not row:
        return 0
    try:
        return int(row[0] or 0)
    except Exception:
        return 0


def sync_ctgov(
    cfg: AppConfig,
    db_path: Path,
    *,
    topic_names: Optional[List[str]] = None,
    max_pages: Optional[int] = None,
) -> None:
    conn = connect(db_path)
    init_db(conn)

    client = CTGovClient(sleep_seconds=cfg.pipeline.ctgov_sleep_seconds)

    selected_topics: List[TopicConfig] = []
    if topic_names:
        wanted = {t.strip() for t in topic_names if t and t.strip()}
        selected_topics = [t for t in cfg.topics if t.name in wanted]
    else:
        selected_topics = list(cfg.topics)

    for topic in selected_topics:
        params = dict(topic.ctgov_params)
        page_size = int(params.get("pageSize") or 200)
        # The client also sets defaults; we keep these explicit.
        params["pageSize"] = page_size
        params.setdefault("format", "json")

        topic_max_pages = max_pages if max_pages is not None else cfg.pipeline.max_pages_per_topic

        print(f"[sync] Topic: {topic.name} | pageSize={page_size} | max_pages={topic_max_pages}")
        count = 0
        kept = 0
        for study in client.iter_studies(params, page_size=page_size, max_pages=topic_max_pages):
            count += 1
            record = extract_trial_record(study)
            nct_id = record.get("nct_id")
            if not nct_id:
                continue

            # Optional: apply tag keyword matching as an extra safety filter
            if topic.tag_keywords and not _topic_text_match(record, topic.tag_keywords):
                # Still keep it (because it matched the API query), but you could flip this
                pass

            existing_pubmed = _get_existing_pubmed_count(conn, str(nct_id))
            scores = score_trial(
                record,
                interesting_keywords=topic.interesting_keywords,
                pubmed_count=existing_pubmed,
                today=date.today(),
            )

            # Store without raw JSON by default to keep DB smaller.
            upsert_trial(conn, record, topic_name=topic.name, scores=scores, raw_json=None)
            kept += 1

            if kept % 200 == 0:
                print(f"  processed {kept} trials (topic={topic.name})")

        print(f"[sync] Topic: {topic.name} | received={count} | stored={kept}")

    conn.close()
    print(f"[sync] Done. DB: {db_path}")


def generate_digest(cfg: AppConfig, db_path: Path, out_path: Path, *, days: Optional[int] = None) -> None:
    conn = connect(db_path)
    init_db(conn)

    window_days = int(days) if days is not None else cfg.pipeline.readout_window_days
    rows = fetch_trials_for_digest(
        conn,
        readout_window_days=window_days,
        recently_completed_days=cfg.pipeline.recently_completed_days,
    )
    # rows are sqlite3.Row; convert to dict for report helpers
    dict_rows = [dict(r) for r in rows]

    out_path.parent.mkdir(parents=True, exist_ok=True)
    write_digest_markdown(dict_rows, out_path)

    # optional exports alongside digest
    if cfg.pipeline.export_csv:
        export_table(dict_rows, csv_path=out_path.with_suffix(".csv"), xlsx_path=None)
    if cfg.pipeline.export_excel:
        export_table(dict_rows, csv_path=None, xlsx_path=out_path.with_suffix(".xlsx"))

    conn.close()
    print(f"[digest] Wrote {out_path}")
    if cfg.pipeline.export_csv:
        print(f"[digest] Wrote {out_path.with_suffix('.csv')}")
    if cfg.pipeline.export_excel:
        print(f"[digest] Wrote {out_path.with_suffix('.xlsx')}")


def link_pubmed(cfg: AppConfig, db_path: Path, *, max_trials: Optional[int] = None) -> None:
    if not cfg.pubmed.enabled:
        print("[pubmed] Disabled in config.")
        return

    conn = connect(db_path)
    init_db(conn)

    limit = int(max_trials) if max_trials is not None else cfg.pubmed.max_trials_per_run

    if cfg.pubmed.actionable_only:
        nct_ids = fetch_actionable_nct_ids(
            conn,
            readout_window_days=cfg.pipeline.readout_window_days,
            recently_completed_days=cfg.pipeline.recently_completed_days,
            limit=limit,
        )
    else:
        cur = conn.cursor()
        cur.execute("SELECT nct_id FROM trials ORDER BY total_score DESC LIMIT ?", (limit,))
        nct_ids = [r[0] for r in cur.fetchall()]

    print(f"[pubmed] Checking PubMed for {len(nct_ids)} trials (limit={limit})")

    client = PubMedClient(
        tool=cfg.pubmed.tool,
        email=cfg.pubmed.email,
        sleep_seconds=cfg.pubmed.sleep_seconds,
    )

    for i, nct in enumerate(nct_ids, 1):
        try:
            citations = client.citations_for_nct(nct)
        except Exception as e:
            print(f"[pubmed] {nct}: error: {e}")
            continue

        upsert_pubmed_citations(conn, nct, citations)

        # Summarize
        pubmed_count = len(citations)
        latest = None
        # pub_date strings are not guaranteed ISO; keep the latest lexicographically as a weak heuristic
        pub_dates = [c.get("pub_date") for c in citations if c.get("pub_date")]
        if pub_dates:
            latest = sorted([str(x) for x in pub_dates])[-1]

        update_pubmed_summary(conn, nct, pubmed_count=pubmed_count, pubmed_latest_date=latest)

        if i % 25 == 0:
            print(f"[pubmed] processed {i}/{len(nct_ids)}")

    conn.close()
    print("[pubmed] Done.")
