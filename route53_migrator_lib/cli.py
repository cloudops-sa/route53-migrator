import argparse
import datetime
import json
import os
from typing import Any, Dict, List, Optional

from botocore.exceptions import BotoCoreError, ClientError

from .aws_clients import assert_profile_usable
from .core import (
    apply_change_batches,
    estimate_diff_against_target,
    export_raw_recordsets,
    fetch_target_index,
    prompt_if_missing,
    read_json,
    s3_get_json,
    s3_put_json,
    summarize_changes,
    transform_recordsets_to_change_batches,
    utc_now_iso,
    write_json,
    eprint,
)


DEFAULT_EXCLUDE_TYPES = {"NS", "SOA"}
DEFAULT_BATCH_SIZE = 100


def _load_transformed_from_args(args: argparse.Namespace) -> Dict[str, Any]:
    if args.in_file:
        return read_json(args.in_file)

    bucket = args.s3_bucket
    key = args.s3_key

    if not bucket:
        bucket = prompt_if_missing(None, "S3 bucket in target account: ")
    if not key:
        key = prompt_if_missing(None, "S3 key for change-batches.json: ")

    assert_profile_usable(args.target_profile)
    return s3_get_json(args.target_profile, bucket, key)


def cmd_export(args: argparse.Namespace) -> int:
    out_dir = args.out_dir
    timestamp = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    raw_path = os.path.join(out_dir, "raw-recordsets.json")
    transformed_path = os.path.join(out_dir, "change-batches.json")

    raw = export_raw_recordsets(args.source_profile, args.source_zone_id, exported_at=utc_now_iso())

    if args.export_raw:
        write_json(raw_path, raw)
        print(f"Wrote raw export: {raw_path}")

    transformed: Optional[Dict[str, Any]] = None
    if args.export_transformed:
        transformed = transform_recordsets_to_change_batches(
            raw_export=raw,
            exclude_types=args.exclude_types.split(",") if args.exclude_types else sorted(list(DEFAULT_EXCLUDE_TYPES)),
            action=args.import_action,
            batch_size=args.batch_size,
            transformed_at=utc_now_iso(),
        )
        write_json(transformed_path, transformed)
        print(f"Wrote transformed export: {transformed_path}")

    if args.upload_to_s3 or args.s3_bucket:
        bucket = args.s3_bucket
        if not bucket:
            bucket = prompt_if_missing(None, "S3 bucket in target account for storing exports: ")

        target_profile = args.target_profile
        if not target_profile:
            target_profile = prompt_if_missing(None, "Target AWS profile (for S3 upload): ")
        ident = assert_profile_usable(target_profile)
        print(f"Using target profile identity for S3 upload: {ident.get('Arn')}")

        prefix = args.s3_prefix
        if not prefix:
            prefix = prompt_if_missing(
                None,
                f"S3 prefix (folder) [default route53-migrator/{args.source_zone_id}/{timestamp}/]: ",
            )
        if not prefix:
            prefix = f"route53-migrator/{args.source_zone_id}/{timestamp}/"
        if not prefix.endswith("/"):
            prefix = prefix + "/"

        if args.export_raw:
            s3_put_json(target_profile, bucket, prefix + "raw-recordsets.json", raw)
            print(f"Uploaded raw export to s3://{bucket}/{prefix}raw-recordsets.json")

        if args.export_transformed and transformed is not None:
            s3_put_json(target_profile, bucket, prefix + "change-batches.json", transformed)
            print(f"Uploaded transformed export to s3://{bucket}/{prefix}change-batches.json")

    return 0


def cmd_import(args: argparse.Namespace) -> int:
    ident = assert_profile_usable(args.target_profile)
    print(f"Using target profile identity: {ident.get('Arn')}")

    force_preview_only = bool(args.dry_run and args.yes and not args.apply)

    if not args.apply and not args.yes:
        ready = prompt_if_missing(None, "Are you ready to import into target account? (yes/no): ").lower()
        if ready not in {"y", "yes"}:
            print("Import canceled.")
            return 0

    batches_doc = _load_transformed_from_args(args)
    summary = summarize_changes(batches_doc)

    print("Dry-run summary:")
    print(json.dumps(summary, indent=2))

    if args.diff_against_target:
        try:
            target_index = fetch_target_index(args.target_profile, args.target_zone_id)
            diff = estimate_diff_against_target(batches_doc, target_index)
            print("Best-effort diff against target hosted zone:")
            print(json.dumps(diff, indent=2))
        except (BotoCoreError, ClientError) as e:
            eprint(f"Unable to compute diff against target hosted zone: {e}")

    if force_preview_only:
        print("Dry-run only (--dry-run with --yes). No changes applied.")
        return 0

    if not args.apply and not args.yes:
        confirm = prompt_if_missing(None, "Proceed to apply these changes? (yes/no): ").lower()
        if confirm not in {"y", "yes"}:
            print("Import canceled.")
            return 0

    if not args.apply and args.yes:
        print("Applying changes (--yes).")

    results = apply_change_batches(args.target_profile, args.target_zone_id, batches_doc.get("batches", []) or [])

    print("Import completed.")
    print("Post-validation steps:")
    print("1) Wait for Route53 changes to reach INSYNC (optional: check via get_change for each change id).")
    print("2) Compare record counts (excluding NS/SOA) between source and target zones.")
    print("3) Validate resolution using dig against authoritative name servers and public resolvers.")
    print("Cutover guidance:")
    print("- If delegated from a parent hosted zone: update parent zone NS records to the new zone's name servers.")
    print("- If using a registrar: update registrar name servers to the new zone's name servers.")

    if args.print_results:
        print(json.dumps(results, indent=2))

    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="route53_migrator")
    sub = parser.add_subparsers(dest="command", required=True)

    p_export = sub.add_parser("export")
    p_export.add_argument("--source-profile", required=True)
    p_export.add_argument("--source-zone-id", required=True)
    p_export.add_argument("--out-dir", default="./artifacts")
    p_export.add_argument("--exclude-types", default=",".join(sorted(DEFAULT_EXCLUDE_TYPES)))
    p_export.add_argument("--export-raw", action=argparse.BooleanOptionalAction, default=True)
    p_export.add_argument("--export-transformed", action=argparse.BooleanOptionalAction, default=True)
    p_export.add_argument("--import-action", choices=["CREATE", "UPSERT"], default="UPSERT")
    p_export.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE)
    p_export.add_argument("--upload-to-s3", action="store_true", default=False)
    p_export.add_argument("--s3-bucket")
    p_export.add_argument("--s3-prefix")
    p_export.add_argument("--target-profile")
    p_export.set_defaults(func=cmd_export)

    p_import = sub.add_parser("import")
    p_import.add_argument("--target-profile", required=True)
    p_import.add_argument("--target-zone-id", required=True)
    p_import.add_argument("--in-file")
    p_import.add_argument("--s3-bucket")
    p_import.add_argument("--s3-key")
    p_import.add_argument("--diff-against-target", action=argparse.BooleanOptionalAction, default=True)
    p_import.add_argument("--dry-run", action=argparse.BooleanOptionalAction, default=True)
    p_import.add_argument("--apply", action="store_true", default=False)
    p_import.add_argument("--yes", action="store_true", default=False)
    p_import.add_argument("--print-results", action="store_true", default=False)
    p_import.set_defaults(func=cmd_import)

    return parser


def main(argv: Optional[List[str]] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        return int(args.func(args))
    except RuntimeError as e:
        eprint(str(e))
        return 2
    except (BotoCoreError, ClientError) as e:
        eprint(str(e))
        return 3


if __name__ == "__main__":
    raise SystemExit(main())
