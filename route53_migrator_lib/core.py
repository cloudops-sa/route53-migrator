import datetime
import json
import os
import sys
from typing import Any, Dict, Iterable, List, Optional, Tuple

from .aws_clients import route53_client, s3_client


def utc_now_iso() -> str:
    return datetime.datetime.now(datetime.timezone.utc).isoformat()


def eprint(msg: str) -> None:
    print(msg, file=sys.stderr)


def prompt_if_missing(value: Optional[str], prompt: str) -> str:
    if value is not None and value != "":
        return value
    return input(prompt).strip()


def write_json(path: str, obj: Any) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        json.dump(obj, f, indent=2)


def read_json(path: str) -> Any:
    with open(path, "r") as f:
        return json.load(f)


def s3_put_json(profile_name: str, bucket: str, key: str, obj: Any) -> None:
    client = s3_client(profile_name)
    body = json.dumps(obj, indent=2).encode("utf-8")
    client.put_object(Bucket=bucket, Key=key, Body=body, ContentType="application/json")


def s3_get_json(profile_name: str, bucket: str, key: str) -> Any:
    client = s3_client(profile_name)
    resp = client.get_object(Bucket=bucket, Key=key)
    data = resp["Body"].read()
    return json.loads(data.decode("utf-8"))


def iter_record_sets(client, hosted_zone_id: str) -> Iterable[Dict[str, Any]]:
    kwargs: Dict[str, Any] = {"HostedZoneId": hosted_zone_id}

    while True:
        resp = client.list_resource_record_sets(**kwargs)
        for rrset in resp.get("ResourceRecordSets", []):
            yield rrset

        if not resp.get("IsTruncated"):
            break

        kwargs["StartRecordName"] = resp.get("NextRecordName")
        kwargs["StartRecordType"] = resp.get("NextRecordType")
        if resp.get("NextRecordIdentifier"):
            kwargs["StartRecordIdentifier"] = resp.get("NextRecordIdentifier")
        else:
            kwargs.pop("StartRecordIdentifier", None)


def export_raw_recordsets(source_profile: str, source_zone_id: str, exported_at: str) -> Dict[str, Any]:
    client = route53_client(source_profile)
    record_sets = list(iter_record_sets(client, source_zone_id))
    return {
        "source_hosted_zone_id": source_zone_id,
        "exported_at": exported_at,
        "record_sets": record_sets,
    }


def record_key(rrset: Dict[str, Any]) -> Tuple[str, str, str]:
    name = rrset.get("Name", "")
    rtype = rrset.get("Type", "")
    set_id = rrset.get("SetIdentifier", "")
    return name, rtype, set_id


def _sorted_resource_records(rrset: Dict[str, Any]) -> List[Dict[str, Any]]:
    rrs = rrset.get("ResourceRecords")
    if not isinstance(rrs, list):
        return []
    cleaned: List[Dict[str, Any]] = []
    for item in rrs:
        if isinstance(item, dict) and "Value" in item:
            cleaned.append({"Value": str(item["Value"])})
    cleaned.sort(key=lambda x: x.get("Value", ""))
    return cleaned


def normalize_rrset(rrset: Dict[str, Any]) -> Dict[str, Any]:
    """Create a comparable representation of a record set.

    This intentionally ignores fields that do not affect desired state comparisons.
    """

    out: Dict[str, Any] = {}
    for k in (
        "Name",
        "Type",
        "SetIdentifier",
        "Weight",
        "Region",
        "Failover",
        "GeoLocation",
        "MultiValueAnswer",
        "HealthCheckId",
    ):
        if k in rrset:
            out[k] = rrset[k]

    if "AliasTarget" in rrset:
        out["AliasTarget"] = rrset.get("AliasTarget")
    else:
        if "TTL" in rrset:
            out["TTL"] = rrset.get("TTL")
        out["ResourceRecords"] = _sorted_resource_records(rrset)

    return out


def rrset_equivalent(a: Dict[str, Any], b: Dict[str, Any]) -> bool:
    return normalize_rrset(a) == normalize_rrset(b)


def classify_change(
    action: str,
    desired_rrset: Dict[str, Any],
    target_index: Dict[Tuple[str, str, str], Dict[str, Any]],
) -> str:
    """Return one of: create, update, noop.

    - For CREATE: existing records are treated as update (will likely fail on apply).
    - For UPSERT: if equivalent, treated as noop.
    """

    key = record_key(desired_rrset)
    existing = target_index.get(key)
    if existing is None:
        return "create"

    if action == "UPSERT" and rrset_equivalent(existing, desired_rrset):
        return "noop"
    return "update"


def fetch_target_index(target_profile: str, target_zone_id: str) -> Dict[Tuple[str, str, str], Dict[str, Any]]:
    client = route53_client(target_profile)
    index: Dict[Tuple[str, str, str], Dict[str, Any]] = {}
    for rrset in iter_record_sets(client, target_zone_id):
        index[record_key(rrset)] = rrset
    return index


def apply_change_batches(target_profile: str, target_zone_id: str, batches: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    client = route53_client(target_profile)
    results: List[Dict[str, Any]] = []

    for idx, batch in enumerate(batches, start=1):
        resp = client.change_resource_record_sets(HostedZoneId=target_zone_id, ChangeBatch=batch)
        change_info = resp.get("ChangeInfo")
        results.append({"batch": idx, "change_info": change_info})
        print(f"Applied batch {idx}/{len(batches)}: {change_info}")

    return results


def transform_recordsets_to_change_batches(
    raw_export: Dict[str, Any],
    exclude_types: Iterable[str],
    action: str,
    batch_size: int,
    transformed_at: str,
) -> Dict[str, Any]:
    exclude = {t.strip().upper() for t in exclude_types if t.strip()}

    changes: List[Dict[str, Any]] = []
    for rrset in raw_export.get("record_sets", []):
        rtype = str(rrset.get("Type", "")).upper()
        if rtype in exclude:
            continue

        changes.append({"Action": action, "ResourceRecordSet": dict(rrset)})

    batches: List[Dict[str, Any]] = []
    for i in range(0, len(changes), batch_size):
        batches.append({"Changes": changes[i : i + batch_size]})

    return {
        "source_hosted_zone_id": raw_export.get("source_hosted_zone_id"),
        "transformed_at": transformed_at,
        "action": action,
        "exclude_types": sorted(list(exclude)),
        "batch_size": batch_size,
        "batches": batches,
    }


def summarize_changes(batches_doc: Dict[str, Any]) -> Dict[str, Any]:
    batches = batches_doc.get("batches", []) or []

    total_changes = 0
    by_type: Dict[str, int] = {}
    sample_names: List[str] = []

    for batch in batches:
        for ch in batch.get("Changes", []) or []:
            total_changes += 1
            rrset = ch.get("ResourceRecordSet", {}) or {}
            rtype = str(rrset.get("Type", "UNKNOWN"))
            by_type[rtype] = by_type.get(rtype, 0) + 1
            if len(sample_names) < 10:
                nm = rrset.get("Name")
                if nm:
                    sample_names.append(nm)

    return {
        "total_changes": total_changes,
        "total_batches": len(batches),
        "by_type": dict(sorted(by_type.items(), key=lambda kv: kv[0])),
        "sample_names": sample_names,
        "action": batches_doc.get("action"),
    }


def estimate_diff_against_target(
    batches_doc: Dict[str, Any],
    target_index: Dict[Tuple[str, str, str], Dict[str, Any]],
) -> Dict[str, int]:
    create_count = 0
    update_count = 0
    noop_count = 0

    default_action = str(batches_doc.get("action") or "").upper() or "UPSERT"

    for batch in batches_doc.get("batches", []) or []:
        for ch in batch.get("Changes", []) or []:
            rrset = ch.get("ResourceRecordSet", {}) or {}
            action = str(ch.get("Action") or default_action).upper()
            cls = classify_change(action, rrset, target_index)
            if cls == "create":
                create_count += 1
            elif cls == "noop":
                noop_count += 1
            else:
                update_count += 1

    return {
        "would_create_new": create_count,
        "would_update_existing": update_count,
        "would_be_noop": noop_count,
    }


def filter_noop_upserts(
    batches_doc: Dict[str, Any],
    target_index: Dict[Tuple[str, str, str], Dict[str, Any]],
) -> Dict[str, Any]:
    """Return a copy of batches_doc with no-op UPSERT changes removed."""

    default_action = str(batches_doc.get("action") or "").upper() or "UPSERT"
    new_doc = dict(batches_doc)
    new_batches: List[Dict[str, Any]] = []

    for batch in batches_doc.get("batches", []) or []:
        changes_out: List[Dict[str, Any]] = []
        for ch in batch.get("Changes", []) or []:
            rrset = ch.get("ResourceRecordSet", {}) or {}
            action = str(ch.get("Action") or default_action).upper()
            if action == "UPSERT" and classify_change(action, rrset, target_index) == "noop":
                continue
            changes_out.append(ch)
        if changes_out:
            new_batches.append({"Changes": changes_out})

    new_doc["batches"] = new_batches
    return new_doc
