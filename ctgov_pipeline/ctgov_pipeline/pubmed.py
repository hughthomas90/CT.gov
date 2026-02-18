from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import requests


@dataclass
class PubMedClient:
    """Small PubMed E-utilities client for linking NCT IDs to PMIDs."""

    tool: str
    email: str
    sleep_seconds: float = 0.4
    timeout_seconds: float = 30.0
    base_url: str = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils"

    def __post_init__(self) -> None:
        self._session = requests.Session()
        self._session.headers.update({"User-Agent": f"{self.tool} (mailto:{self.email})" if self.email else self.tool})

    def _get(self, endpoint: str, params: Dict[str, Any]) -> requests.Response:
        url = self.base_url.rstrip("/") + "/" + endpoint.lstrip("/")
        return self._session.get(url, params=params, timeout=self.timeout_seconds)

    def search_pmids_for_nct(self, nct_id: str, *, retmax: int = 200) -> List[str]:
        # NLM notes that ClinicalTrials.gov identifiers appear in PubMed's Secondary Source ID (SI) field
        # as ClinicalTrials.gov/NCT########. In practice, searching either form can help.
        term = f'("ClinicalTrials.gov/{nct_id}"[SI] OR "{nct_id}"[SI])'
        params = {
            "db": "pubmed",
            "term": term,
            "retmode": "json",
            "retmax": str(retmax),
            "tool": self.tool,
        }
        if self.email:
            params["email"] = self.email

        resp = self._get("esearch.fcgi", params=params)
        if resp.status_code != 200:
            raise RuntimeError(f"PubMed ESearch error {resp.status_code}: {resp.text[:500]}")
        data = resp.json()
        ids = data.get("esearchresult", {}).get("idlist", []) or []
        if self.sleep_seconds:
            time.sleep(self.sleep_seconds)
        return [str(x) for x in ids]

    def summary(self, pmids: List[str]) -> Dict[str, Any]:
        if not pmids:
            return {}
        params = {
            "db": "pubmed",
            "id": ",".join(pmids),
            "retmode": "json",
            "tool": self.tool,
        }
        if self.email:
            params["email"] = self.email
        resp = self._get("esummary.fcgi", params=params)
        if resp.status_code != 200:
            raise RuntimeError(f"PubMed ESummary error {resp.status_code}: {resp.text[:500]}")
        data = resp.json()
        if self.sleep_seconds:
            time.sleep(self.sleep_seconds)
        return data

    def citations_for_nct(self, nct_id: str, *, retmax: int = 200) -> List[Dict[str, Any]]:
        pmids = self.search_pmids_for_nct(nct_id, retmax=retmax)
        if not pmids:
            return []
        summ = self.summary(pmids)
        result = summ.get("result", {}) or {}
        out: List[Dict[str, Any]] = []
        for pmid in pmids:
            item = result.get(str(pmid))
            if not isinstance(item, dict):
                continue
            # DOI can appear in elocationid (often 'doi: ...') or articleids list
            doi = None
            eloc = item.get("elocationid")
            if isinstance(eloc, str) and "doi" in eloc.lower():
                doi = eloc.replace("doi:", "").strip()
            # articleids sometimes is like [{'idtype':'doi','value':'...'}]
            for aid in item.get("articleids", []) or []:
                if isinstance(aid, dict) and aid.get("idtype") == "doi":
                    doi = aid.get("value")
                    break

            out.append(
                {
                    "pmid": str(pmid),
                    "title": item.get("title"),
                    "source": item.get("fulljournalname") or item.get("source"),
                    "pub_date": item.get("pubdate"),
                    "doi": doi,
                }
            )
        return out
