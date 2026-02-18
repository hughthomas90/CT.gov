from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Any, Dict, Iterable, List, Optional, Tuple


def get_nested(obj: Dict[str, Any], path: str, default: Any = None) -> Any:
    """Safely get a nested value from a dict using a dot-delimited path."""
    cur: Any = obj
    for part in path.split("."):
        if not isinstance(cur, dict):
            return default
        if part not in cur:
            return default
        cur = cur[part]
    return cur


@dataclass(frozen=True)
class ParsedDate:
    raw: str
    value: Optional[date]
    precision: str  # DAY | MONTH | YEAR | NONE


def parse_partial_date(raw: Any) -> ParsedDate:
    """Parse date strings like YYYY, YYYY-MM, YYYY-MM-DD into a date.

    The CT.gov API sometimes returns month-precision strings (e.g., '2024-09').
    We convert them to a usable date for sorting, and store a precision label.
    """
    if raw is None:
        return ParsedDate(raw="", value=None, precision="NONE")

    if isinstance(raw, dict) and "date" in raw:
        raw = raw.get("date")

    if not isinstance(raw, str):
        raw = str(raw)

    s = raw.strip()
    if not s:
        return ParsedDate(raw="", value=None, precision="NONE")

    parts = s.split("-")
    try:
        y = int(parts[0])
        if len(parts) == 1:
            # year precision → mid-year for ordering
            return ParsedDate(raw=s, value=date(y, 7, 1), precision="YEAR")
        if len(parts) == 2:
            m = int(parts[1])
            # month precision → mid-month for ordering
            return ParsedDate(raw=s, value=date(y, m, 15), precision="MONTH")
        if len(parts) >= 3:
            m = int(parts[1])
            d = int(parts[2])
            return ParsedDate(raw=s, value=date(y, m, d), precision="DAY")
    except Exception:
        return ParsedDate(raw=s, value=None, precision="NONE")

    return ParsedDate(raw=s, value=None, precision="NONE")


def _extract_interventions(study: Dict[str, Any]) -> Tuple[List[str], List[str]]:
    interventions = get_nested(study, "protocolSection.armsInterventionsModule.interventions", []) or []
    names: List[str] = []
    types: List[str] = []
    if isinstance(interventions, list):
        for it in interventions:
            if not isinstance(it, dict):
                continue
            n = it.get("name")
            t = it.get("type")
            if isinstance(n, str) and n.strip():
                names.append(n.strip())
            if isinstance(t, str) and t.strip():
                types.append(t.strip())
    # de-duplicate while preserving order
    def dedup(seq: Iterable[str]) -> List[str]:
        seen = set()
        out = []
        for x in seq:
            if x not in seen:
                seen.add(x)
                out.append(x)
        return out

    return dedup(names), dedup(types)


def _extract_contacts(study: Dict[str, Any]) -> Dict[str, Any]:
    """Best-effort extraction of investigator/contact info."""
    module = get_nested(study, "protocolSection.contactsLocationsModule", {}) or {}
    out: Dict[str, Any] = {"central_contacts": [], "overall_officials": []}

    central = module.get("centralContacts") if isinstance(module, dict) else None
    if isinstance(central, list):
        for c in central:
            if not isinstance(c, dict):
                continue
            out["central_contacts"].append(
                {
                    "name": c.get("name"),
                    "role": c.get("role"),
                    "phone": c.get("phone"),
                    "email": c.get("email"),
                }
            )

    officials = module.get("overallOfficials") if isinstance(module, dict) else None
    if isinstance(officials, list):
        for o in officials:
            if not isinstance(o, dict):
                continue
            out["overall_officials"].append(
                {
                    "name": o.get("name"),
                    "affiliation": o.get("affiliation"),
                    "role": o.get("role"),
                }
            )

    return out


def infer_modality(intervention_types: List[str]) -> str:
    """Map CT.gov intervention types into editor-friendly buckets."""
    tset = {t.upper() for t in intervention_types if isinstance(t, str)}

    if any(t in tset for t in {"DRUG", "BIOLOGICAL", "GENETIC", "GENE_TRANSFER", "CELL_THERAPY"}):
        return "drug/biologic"
    if "DEVICE" in tset:
        return "device"
    if any(t in tset for t in {"PROCEDURE", "SURGERY"}):
        return "procedure/surgery"
    if "RADIATION" in tset:
        return "radiation"
    if "DIAGNOSTIC_TEST" in tset:
        return "diagnostic"
    if "BEHAVIORAL" in tset:
        return "behavioral"
    return "other"


def extract_trial_record(study: Dict[str, Any]) -> Dict[str, Any]:
    """Normalize a CT.gov study JSON object into a compact dict for scoring/storage."""
    nct_id = get_nested(study, "protocolSection.identificationModule.nctId")
    if not nct_id:
        # some endpoints may return 'id' at top level
        nct_id = study.get("id")

    brief_title = get_nested(study, "protocolSection.identificationModule.briefTitle", "")
    official_title = get_nested(study, "protocolSection.identificationModule.officialTitle", "")
    acronym = get_nested(study, "protocolSection.identificationModule.acronym", "")

    overall_status = get_nested(study, "protocolSection.statusModule.overallStatus", "")
    study_type = get_nested(study, "protocolSection.designModule.studyType", "")

    phases = get_nested(study, "protocolSection.designModule.phases", []) or []
    if not isinstance(phases, list):
        phases = [str(phases)]

    enrollment = get_nested(study, "protocolSection.designModule.enrollmentInfo.count")
    enrollment_type = get_nested(study, "protocolSection.designModule.enrollmentInfo.type", "")

    lead_sponsor_name = get_nested(study, "protocolSection.sponsorCollaboratorsModule.leadSponsor.name", "") or get_nested(
        study, "protocolSection.identificationModule.organization.fullName", ""
    )
    lead_sponsor_class = get_nested(study, "protocolSection.sponsorCollaboratorsModule.leadSponsor.class", "") or get_nested(
        study, "protocolSection.identificationModule.organization.class", ""
    )

    is_fda_drug = get_nested(study, "protocolSection.oversightModule.isFdaRegulatedDrug")
    is_fda_device = get_nested(study, "protocolSection.oversightModule.isFdaRegulatedDevice")
    has_dmc = get_nested(study, "protocolSection.oversightModule.oversightHasDmc")

    # Dates
    start_date_struct = get_nested(study, "protocolSection.statusModule.startDateStruct")
    primary_comp_struct = get_nested(study, "protocolSection.statusModule.primaryCompletionDateStruct")
    completion_struct = get_nested(study, "protocolSection.statusModule.completionDateStruct")
    last_update_struct = get_nested(study, "protocolSection.statusModule.lastUpdatePostDateStruct")
    results_first_post_struct = get_nested(study, "protocolSection.statusModule.resultsFirstPostDateStruct")

    start_date = parse_partial_date(start_date_struct)
    primary_completion_date = parse_partial_date(primary_comp_struct)
    completion_date = parse_partial_date(completion_struct)
    last_update_post_date = parse_partial_date(last_update_struct)
    results_first_post_date = parse_partial_date(results_first_post_struct)

    primary_completion_type = None
    if isinstance(primary_comp_struct, dict):
        primary_completion_type = primary_comp_struct.get("type")

    completion_type = None
    if isinstance(completion_struct, dict):
        completion_type = completion_struct.get("type")

    conditions = get_nested(study, "protocolSection.conditionsModule.conditions", []) or []
    if not isinstance(conditions, list):
        conditions = [str(conditions)]

    interventions, intervention_types = _extract_interventions(study)
    modality = infer_modality(intervention_types)

    # Locations count (best effort)
    locs = (
        get_nested(study, "protocolSection.contactsLocationsModule.locations")
        or get_nested(study, "protocolSection.locationsModule.locations")
        or []
    )
    location_count = len(locs) if isinstance(locs, list) else None

    contacts = _extract_contacts(study)

    # Results flag exists on /studies search results; single-study endpoint might not include it.
    has_results = study.get("hasResults")
    if has_results is None:
        # If results were posted, resultsFirstPostDate usually exists.
        has_results = bool(results_first_post_date.raw)

    record = {
        "nct_id": nct_id,
        "brief_title": brief_title,
        "official_title": official_title,
        "acronym": acronym,
        "overall_status": overall_status,
        "study_type": study_type,
        "phases": phases,
        "enrollment": enrollment,
        "enrollment_type": enrollment_type,
        "lead_sponsor_name": lead_sponsor_name,
        "lead_sponsor_class": lead_sponsor_class,
        "is_fda_regulated_drug": bool(is_fda_drug) if is_fda_drug is not None else None,
        "is_fda_regulated_device": bool(is_fda_device) if is_fda_device is not None else None,
        "oversight_has_dmc": bool(has_dmc) if has_dmc is not None else None,
        "conditions": conditions,
        "interventions": interventions,
        "intervention_types": intervention_types,
        "modality": modality,
        "location_count": location_count,
        "has_results": bool(has_results),
        "start_date": start_date.raw,
        "start_date_parsed": start_date.value.isoformat() if start_date.value else None,
        "start_date_precision": start_date.precision,
        "primary_completion_date": primary_completion_date.raw,
        "primary_completion_date_parsed": primary_completion_date.value.isoformat() if primary_completion_date.value else None,
        "primary_completion_date_precision": primary_completion_date.precision,
        "primary_completion_date_type": primary_completion_type,
        "completion_date": completion_date.raw,
        "completion_date_parsed": completion_date.value.isoformat() if completion_date.value else None,
        "completion_date_precision": completion_date.precision,
        "completion_date_type": completion_type,
        "last_update_post_date": last_update_post_date.raw,
        "last_update_post_date_parsed": last_update_post_date.value.isoformat() if last_update_post_date.value else None,
        "results_first_post_date": results_first_post_date.raw,
        "results_first_post_date_parsed": results_first_post_date.value.isoformat() if results_first_post_date.value else None,
        "contacts": contacts,
    }
    return record
