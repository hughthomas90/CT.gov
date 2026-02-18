from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any, Dict, Iterator, Optional

import requests


@dataclass
class CTGovClient:
    """Very small client for ClinicalTrials.gov API v2."""

    base_url: str = "https://clinicaltrials.gov/api/v2"
    sleep_seconds: float = 0.25
    timeout_seconds: float = 30.0
    user_agent: str = "ctgov-trial-watch/0.1 (+https://example.org)"

    def __post_init__(self) -> None:
        self._session = requests.Session()
        self._session.headers.update({"User-Agent": self.user_agent})

    def _get(self, path: str, params: Optional[Dict[str, Any]] = None) -> requests.Response:
        url = self.base_url.rstrip("/") + "/" + path.lstrip("/")
        resp = self._session.get(url, params=params or {}, timeout=self.timeout_seconds)
        return resp

    def version(self) -> Dict[str, Any]:
        resp = self._get("version")
        resp.raise_for_status()
        return resp.json()

    def get_study(self, nct_id: str) -> Dict[str, Any]:
        resp = self._get(f"studies/{nct_id}")
        resp.raise_for_status()
        return resp.json()

    def iter_studies(
        self,
        params: Dict[str, Any],
        *,
        page_size: int = 200,
        max_pages: Optional[int] = None,
    ) -> Iterator[Dict[str, Any]]:
        """Iterate over study records from GET /studies with pagination.

        The API returns study records page-by-page. If response includes `nextPageToken`,
        pass it back as `pageToken` to retrieve the next page.
        """
        p = dict(params)
        # Default to JSON; API supports CSV too but this pipeline expects JSON.
        p.setdefault("format", "json")
        p.setdefault("pageSize", page_size)

        page = 0
        page_token: Optional[str] = None

        while True:
            page += 1
            if max_pages is not None and page > max_pages:
                break

            if page_token:
                p["pageToken"] = page_token
            else:
                p.pop("pageToken", None)

            resp = self._get("studies", params=p)
            if resp.status_code != 200:
                raise RuntimeError(f"CT.gov API error {resp.status_code}: {resp.text[:500]}")

            data = resp.json()

            studies = data.get("studies") or []
            for s in studies:
                if isinstance(s, dict):
                    yield s

            # Pagination token can be either in JSON or (sometimes) in headers.
            page_token = data.get("nextPageToken") or resp.headers.get("x-next-page-token") or resp.headers.get("X-Next-Page-Token")
            if not page_token:
                break

            # polite throttling
            if self.sleep_seconds:
                time.sleep(self.sleep_seconds)
