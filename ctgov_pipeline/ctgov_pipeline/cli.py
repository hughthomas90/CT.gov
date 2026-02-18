from __future__ import annotations

import argparse
from pathlib import Path

from .config import load_config
from .pipeline import sync_ctgov, generate_digest, link_pubmed


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="ctgov-trial-watch",
        description="Track ClinicalTrials.gov trials nearing readout; score + generate digests.",
    )
    p.add_argument("--config", required=True, help="Path to config YAML")
    p.add_argument("--db", required=True, help="Path to SQLite database file")

    sub = p.add_subparsers(dest="cmd", required=True)

    s_sync = sub.add_parser("sync", help="Sync trials from ClinicalTrials.gov into the local DB")
    s_sync.add_argument("--topics", nargs="*", default=None, help="Optional subset of topic names to sync")
    s_sync.add_argument("--max-pages", type=int, default=None, help="Override max pages per topic")

    s_digest = sub.add_parser("digest", help="Generate a markdown digest from the DB")
    s_digest.add_argument("--out", required=True, help="Output markdown file path")
    s_digest.add_argument("--days", type=int, default=None, help="Override readout window days")

    s_pubmed = sub.add_parser("pubmed", help="Link trials to PubMed papers (stores PMIDs/citations in DB)")
    s_pubmed.add_argument("--max-trials", type=int, default=None, help="Override max trials per run")

    return p


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    cfg = load_config(Path(args.config))
    db_path = Path(args.db)

    if args.cmd == "sync":
        sync_ctgov(cfg, db_path, topic_names=args.topics, max_pages=args.max_pages)
        return 0

    if args.cmd == "digest":
        out_path = Path(args.out)
        generate_digest(cfg, db_path, out_path, days=args.days)
        return 0

    if args.cmd == "pubmed":
        link_pubmed(cfg, db_path, max_trials=args.max_trials)
        return 0

    parser.error("Unknown command")
    return 2
