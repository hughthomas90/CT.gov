# CT.gov Trial Watch (MVP)

A lightweight, editor-friendly pipeline for **tracking ClinicalTrials.gov trials that are nearing readout**, scoring them for:
- **Readout urgency** (soon-to-readout / recently completed but unpublished)
- **“Major trial” likelihood** (phase, size, sponsor class, etc.)
- **Interestingness** (keyword-driven, configurable)

…and optionally linking each NCT to **PubMed papers** via NCBI E-utilities.

This project intentionally uses the **official ClinicalTrials.gov API v2** (not screen scraping).

---

## What you get (today)

- **Sync job**: pulls trials from ClinicalTrials.gov (by topic queries you define), stores them to a local SQLite database.
- **Scoring**: produces transparent scores + “reasons” per trial so editors can understand why something is flagged.
- **Digest report**: generates a ranked markdown report of soon-to-readout trials (and optionally an Excel/CSV export).
- **PubMed linking (optional)**: searches PubMed for citations containing an NCT identifier in the Secondary Source ID (SI) field.

---

## Quickstart

### 1) Install

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 2) Configure topics

Copy and edit:

```bash
cp config.example.yaml config.yaml
```

Edit topics under `topics:`. Each topic contains:
- `ctgov_params`: parameters passed to `GET https://clinicaltrials.gov/api/v2/studies`
- `tag_keywords`: optional keywords for topic tagging
- `interesting_keywords`: optional keywords that boost the “interesting” score

### 3) Run a sync

```bash
python -m ctgov_pipeline sync --config config.yaml --db ctgov.sqlite
```

### 4) Generate a digest

```bash
python -m ctgov_pipeline digest --config config.yaml --db ctgov.sqlite --out digest.md
```

### 5) (Optional) Link PubMed papers

```bash
python -m ctgov_pipeline pubmed --config config.yaml --db ctgov.sqlite
```

---

## Notes

- **Rate limiting**: The pipeline includes polite throttling. You can tune it in `config.yaml`.
- **Editor workflow**: The SQLite DB makes it easy to build a front-end later (Streamlit / FastAPI + React).
- **Security**: No secrets are required for ClinicalTrials.gov or PubMed E-utilities. If you add email/Slack alerts, use environment variables for credentials.

---

## Roadmap suggestions (next)

1. **“Commissioning queue”**: add a table for editor notes, assignments, status (contacted/in progress).
2. **Dashboard**: Streamlit view with filters (topic, phase, modality, sponsor, readout window).
3. **Diffing**: store snapshots and highlight changes (status changes, date shifts, enrollment changes).
4. **Enrichment**:
   - trial → sponsor pipelines (company mapping)
   - trial → conference readouts (if available)
   - preprints / Crossref / Europe PMC feeds

---

## Disclaimer

This tool is for editorial intelligence and internal planning. It does not provide medical advice.

---

## Streamlit hosting (recommended dashboard option)

This repo includes a basic Streamlit dashboard (`app.py`). You can run it locally:

```bash
streamlit run app.py
```

### Deploy on Streamlit Community Cloud

1. Push this repo to GitHub (public or private).
2. Create an app on Streamlit Community Cloud pointing at `app.py`.
3. Keep the app read-only and refresh data via a scheduled job.

### Keep data fresh

Streamlit Community Cloud apps can sleep when idle, so **do not rely on the app process for scheduled updates**.

The repo includes a GitHub Actions workflow at:

```
.github/workflows/update_data.yml
```

It runs daily, regenerates `ctgov.sqlite`, and commits the updated artifacts (`digest.md`, `digest.csv`, `digest.xlsx`).

> Tip: If your DB grows large, move storage to Postgres instead of committing a SQLite file.

