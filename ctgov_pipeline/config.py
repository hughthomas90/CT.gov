from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml


@dataclass(frozen=True)
class PubMedConfig:
    enabled: bool = True
    tool: str = "ctgov-trial-watch"
    email: str = ""
    sleep_seconds: float = 0.4
    actionable_only: bool = True
    max_trials_per_run: int = 200


@dataclass(frozen=True)
class PipelineConfig:
    readout_window_days: int = 180
    recently_completed_days: int = 120
    max_pages_per_topic: int = 10
    ctgov_sleep_seconds: float = 0.25
    export_excel: bool = True
    export_csv: bool = True


@dataclass(frozen=True)
class TopicConfig:
    name: str
    ctgov_params: dict[str, Any]
    tag_keywords: list[str]
    interesting_keywords: list[dict[str, Any]]


@dataclass(frozen=True)
class AppConfig:
    pipeline: PipelineConfig
    pubmed: PubMedConfig
    topics: list[TopicConfig]


def _as_bool(x: Any, default: bool) -> bool:
    if x is None:
        return default
    if isinstance(x, bool):
        return x
    if isinstance(x, str):
        return x.strip().lower() in {"1", "true", "yes", "y", "on"}
    return bool(x)


def load_config(path: Path) -> AppConfig:
    raw = yaml.safe_load(path.read_text(encoding="utf-8"))
    raw = raw or {}

    p_raw = raw.get("pipeline", {}) or {}
    pipeline = PipelineConfig(
        readout_window_days=int(p_raw.get("readout_window_days", 180)),
        recently_completed_days=int(p_raw.get("recently_completed_days", 120)),
        max_pages_per_topic=int(p_raw.get("max_pages_per_topic", 10)),
        ctgov_sleep_seconds=float(p_raw.get("ctgov_sleep_seconds", 0.25)),
        export_excel=_as_bool(p_raw.get("export_excel"), True),
        export_csv=_as_bool(p_raw.get("export_csv"), True),
    )

    pm_raw = raw.get("pubmed", {}) or {}
    pubmed = PubMedConfig(
        enabled=_as_bool(pm_raw.get("enabled"), True),
        tool=str(pm_raw.get("tool", "ctgov-trial-watch")),
        email=str(pm_raw.get("email", "")),
        sleep_seconds=float(pm_raw.get("sleep_seconds", 0.4)),
        actionable_only=_as_bool(pm_raw.get("actionable_only"), True),
        max_trials_per_run=int(pm_raw.get("max_trials_per_run", 200)),
    )

    topics_raw = raw.get("topics", []) or []
    topics: list[TopicConfig] = []
    for t in topics_raw:
        if not isinstance(t, dict) or "name" not in t:
            continue
        topics.append(
            TopicConfig(
                name=str(t["name"]),
                ctgov_params=dict(t.get("ctgov_params", {}) or {}),
                tag_keywords=list(t.get("tag_keywords", []) or []),
                interesting_keywords=list(t.get("interesting_keywords", []) or []),
            )
        )

    if not topics:
        raise ValueError("Config must include at least one topic under `topics:`")

    return AppConfig(pipeline=pipeline, pubmed=pubmed, topics=topics)
