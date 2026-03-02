from route53_migrator_lib.core import (
    classify_change,
    estimate_diff_against_target,
    filter_noop_upserts,
    record_key,
)


def test_classify_change_noop_upsert() -> None:
    existing = {
        "Name": "example.com.",
        "Type": "A",
        "TTL": 300,
        "ResourceRecords": [{"Value": "1.2.3.4"}],
    }
    desired = {
        "Name": "example.com.",
        "Type": "A",
        "TTL": 300,
        "ResourceRecords": [{"Value": "1.2.3.4"}],
    }
    idx = {record_key(existing): existing}
    assert classify_change("UPSERT", desired, idx) == "noop"


def test_filter_noop_upserts_removes_equivalent_changes() -> None:
    existing = {
        "Name": "example.com.",
        "Type": "A",
        "TTL": 300,
        "ResourceRecords": [{"Value": "1.2.3.4"}],
    }
    idx = {record_key(existing): existing}

    batches_doc = {
        "action": "UPSERT",
        "batches": [
            {
                "Changes": [
                    {"Action": "UPSERT", "ResourceRecordSet": dict(existing)},
                    {
                        "Action": "UPSERT",
                        "ResourceRecordSet": {
                            "Name": "new.example.com.",
                            "Type": "CNAME",
                            "TTL": 300,
                            "ResourceRecords": [{"Value": "target.example.com."}],
                        },
                    },
                ]
            }
        ],
    }

    filtered = filter_noop_upserts(batches_doc, idx)
    assert len(filtered["batches"]) == 1
    assert len(filtered["batches"][0]["Changes"]) == 1


def test_estimate_diff_reports_noop() -> None:
    existing = {
        "Name": "example.com.",
        "Type": "A",
        "TTL": 300,
        "ResourceRecords": [{"Value": "1.2.3.4"}],
    }
    idx = {record_key(existing): existing}
    batches_doc = {
        "action": "UPSERT",
        "batches": [{"Changes": [{"Action": "UPSERT", "ResourceRecordSet": dict(existing)}]}],
    }

    diff = estimate_diff_against_target(batches_doc, idx)
    assert diff["would_be_noop"] == 1
