from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Tuple

import pandas as pd
import streamlit as st

from ctgov_pipeline.config import load_config
from ctgov_pipeline.pipeline import generate_digest, link_pubmed, sync_ctgov


@dataclass
class AppPaths:
    config_path: Path
    db_path: Path
    digest_path: Path


def _paths() -> AppPaths:
    """Resolve config/db paths.

    Streamlit Community Cloud: use st.secrets for paths if provided.
    Local dev: defaults to files in the repo root.
    """

    cfg = Path(str(st.secrets.get("CONFIG_PATH", "config.yaml")))
    if not cfg.exists():
        cfg = Path("config.example.yaml")

    db = Path(str(st.secrets.get("DB_PATH", "ctgov.sqlite")))
    digest = Path(str(st.secrets.get("DIGEST_PATH", "digest.md")))
    return AppPaths(config_path=cfg, db_path=db, digest_path=digest)


def _safe_json_loads(x: Any, default: Any) -> Any:
    if x is None:
        return default
    try:
        return json.loads(x)
    except Exception:
        return default


@st.cache_data(ttl=600)
def load_trials(db_path_str: str) -> pd.DataFrame:
    """Load the trials table into a DataFrame.

    Cached to keep the app snappy; cache invalidates automatically after ttl.
    """

    db_path = Path(db_path_str)
    if not db_path.exists():
        return pd.DataFrame()

    conn = sqlite3.connect(str(db_path))
    try:
        df = pd.read_sql_query(
            """
            SELECT
              nct_id,
              brief_title,
              official_title,
              acronym,
              overall_status,
              study_type,
              phase,
              modality,
              enrollment,
              lead_sponsor_name,
              lead_sponsor_class,
              has_results,
              primary_completion_date_parsed,
              last_update_post_date_parsed,
              results_first_post_date_parsed,
              days_to_primary_completion,
              urgency_score,
              major_score,
              interesting_score,
              total_score,
              topic_tags_json,
              conditions_json,
              interventions_json,
              intervention_types_json,
              contacts_json,
              score_reasons_json,
              pubmed_count,
              pubmed_latest_date
            FROM trials
            """,
            conn,
        )
    finally:
        conn.close()

    # Parse JSON-ish columns into python objects for easier filtering/preview.
    json_cols = [
        "topic_tags_json",
        "conditions_json",
        "interventions_json",
        "intervention_types_json",
        "contacts_json",
        "score_reasons_json",
    ]
    for col in json_cols:
        if col in df.columns:
            if col.endswith("_json") and col not in {"contacts_json", "score_reasons_json"}:
                df[col] = df[col].apply(lambda v: _safe_json_loads(v, default=[]))
            else:
                df[col] = df[col].apply(lambda v: _safe_json_loads(v, default={}))

    # Ensure numeric where possible
    for col in [
        "enrollment",
        "days_to_primary_completion",
        "urgency_score",
        "major_score",
        "interesting_score",
        "total_score",
        "pubmed_count",
    ]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    return df


@st.cache_data(ttl=600)
def load_pubmed_citations(db_path_str: str, nct_id: str) -> pd.DataFrame:
    db_path = Path(db_path_str)
    if not db_path.exists() or not nct_id:
        return pd.DataFrame()
    conn = sqlite3.connect(str(db_path))
    try:
        df = pd.read_sql_query(
            """
            SELECT pmid, title, source, pub_date, doi
            FROM pubmed_citations
            WHERE nct_id = ?
            ORDER BY pub_date DESC
            """,
            conn,
            params=(nct_id,),
        )
    finally:
        conn.close()
    return df


def _all_topics(df: pd.DataFrame) -> List[str]:
    topics: List[str] = []
    if "topic_tags_json" not in df.columns or df.empty:
        return topics
    for tags in df["topic_tags_json"].tolist():
        if isinstance(tags, list):
            topics.extend([str(t) for t in tags if t])
    return sorted(list(dict.fromkeys(topics)))


def _token_haystack(row: pd.Series) -> str:
    parts: List[str] = []
    for c in [
        "brief_title",
        "official_title",
        "acronym",
        "lead_sponsor_name",
        "overall_status",
        "study_type",
        "phase",
        "modality",
    ]:
        v = row.get(c)
        if v:
            parts.append(str(v))
    for c in ["conditions_json", "interventions_json", "intervention_types_json"]:
        v = row.get(c)
        if isinstance(v, list) and v:
            parts.extend([str(x) for x in v])
    return " ".join(parts).lower()


def _filter_df(df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict[str, object]]:
    """Apply sidebar filters. Returns (filtered_df, filter_state)."""

    st.sidebar.header("Filters")

    if df.empty:
        return df, {}

    topics = _all_topics(df)
    selected_topics = st.sidebar.multiselect("Topics", topics, default=topics)

    phase_values = sorted([p for p in df["phase"].dropna().unique().tolist() if str(p).strip()])
    selected_phases = st.sidebar.multiselect("Phase", phase_values, default=phase_values)

    modality_values = sorted([m for m in df["modality"].dropna().unique().tolist() if str(m).strip()])
    selected_modalities = st.sidebar.multiselect("Modality", modality_values, default=modality_values)

    sponsor_class_values = sorted(
        [s for s in df["lead_sponsor_class"].dropna().unique().tolist() if str(s).strip()]
    )
    selected_sponsor_classes = st.sidebar.multiselect("Sponsor class", sponsor_class_values, default=sponsor_class_values)

    status_values = sorted([s for s in df["overall_status"].dropna().unique().tolist() if str(s).strip()])
    selected_statuses = st.sidebar.multiselect("Status", status_values, default=status_values)

    # Days-to-primary-completion slider (readout radar)
    days = df["days_to_primary_completion"].dropna()
    if len(days) > 0:
        dmin = int(days.min())
        dmax = int(days.max())
        default_lo = max(dmin, -180)
        default_hi = min(dmax, 180)
        days_range = st.sidebar.slider(
            "Days to primary completion",
            min_value=dmin,
            max_value=dmax,
            value=(default_lo, default_hi),
            help="Negative = primary completion already passed (recently completed).",
        )
    else:
        days_range = None

    min_total = st.sidebar.slider(
        "Minimum total score",
        min_value=0,
        max_value=100,
        value=0,
    )

    only_actionable = st.sidebar.checkbox(
        "Actionable only (±180 days)",
        value=True,
        help="Keeps the focus on soon-to-readout and recently completed trials.",
    )

    q = st.sidebar.text_input(
        "Search",
        value="",
        placeholder="e.g., GLP-1, mortality, bispecific",
    ).strip()

    out = df.copy()

    if selected_topics and "topic_tags_json" in out.columns:
        out = out[out["topic_tags_json"].apply(lambda tags: any(t in (tags or []) for t in selected_topics))]

    if selected_phases:
        out = out[out["phase"].isin(selected_phases)]

    if selected_modalities:
        out = out[out["modality"].isin(selected_modalities)]

    if selected_sponsor_classes:
        out = out[out["lead_sponsor_class"].isin(selected_sponsor_classes)]

    if selected_statuses:
        out = out[out["overall_status"].isin(selected_statuses)]

    if days_range is not None:
        lo, hi = days_range
        out = out[
            (out["days_to_primary_completion"].notna())
            & (out["days_to_primary_completion"] >= lo)
            & (out["days_to_primary_completion"] <= hi)
        ]

    out = out[(out["total_score"].fillna(0) >= float(min_total))]

    if only_actionable:
        out = out[
            (out["days_to_primary_completion"].notna())
            & (out["days_to_primary_completion"] >= -180)
            & (out["days_to_primary_completion"] <= 180)
        ]

    if q:
        out = out[out.apply(_token_haystack, axis=1).str.contains(q.lower(), na=False)]

    out = out.sort_values(["total_score", "days_to_primary_completion"], ascending=[False, True])

    state = {
        "selected_topics": selected_topics,
        "selected_phases": selected_phases,
        "selected_modalities": selected_modalities,
        "selected_sponsor_classes": selected_sponsor_classes,
        "selected_statuses": selected_statuses,
        "days_range": days_range,
        "min_total": min_total,
        "only_actionable": only_actionable,
        "q": q,
    }
    return out, state


def _trial_detail(df: pd.DataFrame, db_path: Path) -> None:
    if df.empty:
        st.info("No trials match the current filters.")
        return

    options = df[["nct_id", "brief_title"]].fillna("")
    labels = options.apply(lambda r: f"{r['nct_id']} — {str(r['brief_title'])[:90]}".strip(" —"), axis=1).tolist()
    nct_to_label = dict(zip(df["nct_id"].tolist(), labels))
    selected = st.selectbox(
        "Select a trial to view details",
        options=df["nct_id"].tolist(),
        format_func=lambda x: nct_to_label.get(x, x),
        index=0,
    )
    row = df[df["nct_id"] == selected].iloc[0]

    st.subheader(row.get("brief_title") or row.get("nct_id"))
    ctgov_url = f"https://clinicaltrials.gov/study/{row.get('nct_id')}"
    st.markdown(f"**ClinicalTrials.gov:** {ctgov_url}")

    cols = st.columns(4)
    cols[0].metric("Total score", int(row.get("total_score") or 0))
    cols[1].metric("Urgency", int(row.get("urgency_score") or 0))
    cols[2].metric("Major", int(row.get("major_score") or 0))
    cols[3].metric("Interesting", int(row.get("interesting_score") or 0))

    meta_left, meta_right = st.columns(2)
    with meta_left:
        st.write(
            {
                "NCT": row.get("nct_id"),
                "Phase": row.get("phase"),
                "Status": row.get("overall_status"),
                "Modality": row.get("modality"),
                "Study type": row.get("study_type"),
                "Enrollment": int(row.get("enrollment")) if pd.notna(row.get("enrollment")) else None,
                "Sponsor": row.get("lead_sponsor_name"),
                "Sponsor class": row.get("lead_sponsor_class"),
            }
        )
    with meta_right:
        st.write(
            {
                "Primary completion": row.get("primary_completion_date_parsed"),
                "Days to primary completion": int(row.get("days_to_primary_completion"))
                if pd.notna(row.get("days_to_primary_completion"))
                else None,
                "Last update": row.get("last_update_post_date_parsed"),
                "Results first posted": row.get("results_first_post_date_parsed"),
                "Has CT.gov results": bool(row.get("has_results")),
                "PubMed papers": int(row.get("pubmed_count") or 0),
                "Latest PubMed date": row.get("pubmed_latest_date"),
            }
        )

    reasons = row.get("score_reasons_json")
    if isinstance(reasons, dict) and reasons:
        with st.expander("Why this was flagged (score reasons)"):
            st.json(reasons)

    conditions = row.get("conditions_json")
    if isinstance(conditions, list) and conditions:
        with st.expander("Conditions"):
            st.write(conditions)

    interventions = row.get("interventions_json")
    if isinstance(interventions, list) and interventions:
        with st.expander("Interventions"):
            st.write(interventions)

    contacts = row.get("contacts_json")
    if isinstance(contacts, dict) and contacts:
        with st.expander("Contacts (best-effort extraction)"):
            st.json(contacts)

    citations = load_pubmed_citations(str(db_path), str(row.get("nct_id")))
    if not citations.empty:
        with st.expander("PubMed citations linked to this NCT"):
            citations = citations.copy()
            citations["PubMed"] = citations["pmid"].apply(lambda x: f"https://pubmed.ncbi.nlm.nih.gov/{x}/")
            st.dataframe(citations[["pmid", "PubMed", "title", "source", "pub_date", "doi"]], use_container_width=True, hide_index=True)


def main() -> None:
    st.set_page_config(page_title="CT.gov Trial Watch", layout="wide")
    paths = _paths()
    cfg = load_config(paths.config_path)

    st.title("CT.gov Trial Watch")
    st.caption(
        "Editorial intelligence dashboard: soon-to-readout, major, and interesting trials — powered by the ClinicalTrials.gov API v2."
    )

    with st.expander("Data controls", expanded=False):
        st.write({"Config": str(paths.config_path), "Database": str(paths.db_path)})

        allow_manual_sync = bool(st.secrets.get("ALLOW_MANUAL_SYNC", False))
        if allow_manual_sync:
            c1, c2, c3 = st.columns(3)
            with c1:
                if st.button("Sync CT.gov now"):
                    sync_ctgov(cfg, paths.db_path)
                    st.cache_data.clear()
                    st.success("Sync complete.")
            with c2:
                if st.button("Link PubMed now"):
                    link_pubmed(cfg, paths.db_path)
                    st.cache_data.clear()
                    st.success("PubMed linking complete.")
            with c3:
                if st.button("Generate digest now"):
                    generate_digest(cfg, paths.db_path, paths.digest_path)
                    st.success("Digest generated.")
        else:
            st.info(
                "Manual syncing is disabled by default. "
                "On Streamlit Community Cloud, prefer a scheduled GitHub Action to refresh `ctgov.sqlite`, "
                "and keep the app read-only."
            )

    df = load_trials(str(paths.db_path))
    if df.empty:
        st.warning(
            "No data found yet. Create a database by running `python -m ctgov_pipeline sync` locally, "
            "or set up a scheduled GitHub Action to generate `ctgov.sqlite` and commit it to the repo."
        )
        st.stop()

    filtered, _ = _filter_df(df)

    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Trials in DB", int(len(df)))
    k2.metric("Matching filters", int(len(filtered)))
    k3.metric(">= Phase 3", int(filtered["phase"].fillna("").str.contains("PHASE3|PHASE4", case=False, na=False).sum()))
    k4.metric("Industry sponsored", int((filtered["lead_sponsor_class"].fillna("") == "INDUSTRY").sum()))

    st.subheader("Ranked trials")
    view_cols = [
        "nct_id",
        "brief_title",
        "phase",
        "modality",
        "lead_sponsor_class",
        "enrollment",
        "primary_completion_date_parsed",
        "days_to_primary_completion",
        "urgency_score",
        "major_score",
        "interesting_score",
        "total_score",
        "pubmed_count",
    ]
    table = filtered[view_cols].copy()
    table = table.rename(
        columns={
            "lead_sponsor_class": "sponsor_class",
            "primary_completion_date_parsed": "primary_completion",
            "days_to_primary_completion": "days_to_pc",
            "pubmed_count": "pubmed",
        }
    )
    st.dataframe(table, use_container_width=True, hide_index=True)

    st.divider()
    st.subheader("Trial details")
    _trial_detail(filtered, paths.db_path)


if __name__ == "__main__":
    main()
