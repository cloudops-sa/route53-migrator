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
    new_count = 0
    existing_count = 0

    for batch in batches_doc.get("batches", []) or []:
        for ch in batch.get("Changes", []) or []:
            rrset = ch.get("ResourceRecordSet", {}) or {}
            key = record_key(rrset)
            if key in target_index:
                existing_count += 1
            else:
                new_count += 1

    return {"would_create_new": new_count, "would_touch_existing": existing_count}
