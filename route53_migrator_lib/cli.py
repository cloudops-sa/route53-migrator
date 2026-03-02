import argparse
import datetime
import json
import os
import sys
import re
from typing import Any, Dict, List, Optional

from botocore.exceptions import BotoCoreError, ClientError

from .aws_clients import assert_profile_usable
from .core import (
    apply_change_batches,
    estimate_diff_against_target,
    export_raw_recordsets,
    fetch_target_index,
    filter_noop_upserts,
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


DEFAULT_CONFIG_PATH = "./route53-migrator.config.json"


def _default_change_batches_key(prefix: Optional[str]) -> str:
    p = (prefix or "").strip()
    if not p:
        return "change-batches.json"
    p = p.strip("/")
    return f"{p}/change-batches.json"


def _print_section(title: str) -> None:
    print("\n" + title)
    print("-" * len(title))


def _ask_str(prompt: str, default: Optional[str] = None, required: bool = True) -> str:
    while True:
        suffix = f" [default {default}]" if default not in (None, "") else ""
        val = prompt_if_missing(None, f"{prompt}{suffix}: ").strip()
        if val == "" and default is not None:
            val = default
        if not required or val != "":
            return val
        eprint("Value required.")


def _ask_int(prompt: str, default: int) -> int:
    while True:
        raw = _ask_str(prompt, default=str(default), required=True)
        try:
            return int(raw)
        except ValueError:
            eprint("Please enter a valid integer.")


def _ask_yes_no(prompt: str, default_yes: bool = True) -> bool:
    default = "yes" if default_yes else "no"
    while True:
        raw = _ask_str(prompt + " (yes/no)", default=default, required=True).lower()
        if raw in {"y", "yes"}:
            return True
        if raw in {"n", "no"}:
            return False
        eprint("Please enter 'yes' or 'no'.")


def _ask_zone_id(prompt: str) -> str:
    while True:
        raw = _ask_str(prompt, required=True)
        m = re.search(r"\b(Z[0-9A-Z]+)\b", raw.strip().upper())
        if m:
            return m.group(1)
        eprint("Hosted zone id should look like 'Z...'. You can paste just the id or a JSON line containing it.")


def _load_config(path: str) -> Dict[str, Any]:
    if not path:
        return {}
    try:
        return read_json(path) or {}
    except FileNotFoundError:
        return {}


def _cfg_get(cfg: Dict[str, Any], dotted: str, default: Any = None) -> Any:
    cur: Any = cfg
    for part in dotted.split("."):
        if not isinstance(cur, dict) or part not in cur:
            return default
        cur = cur[part]
    return cur


def _wizard_write_config() -> int:
    print("Interactive setup (-i)")

    _print_section("Step 1: Choose what to configure")
    print("Choose one:")
    print("- export: save source zone records to artifacts (and optionally upload to S3)")
    print("- import: apply artifacts into a target hosted zone")
    print("- both: configure both export + import")
    mode = _ask_str("Configure [export/import/both]", default="both", required=True).strip().lower()
    if mode not in {"export", "import", "both"}:
        eprint("Invalid choice. Use export, import, or both.")
        return 2

    cfg: Dict[str, Any] = {"version": 1, "export": {}, "import": {}, "s3": {}}

    if mode in {"export", "both"}:
        _print_section("Step 2: Export settings (source account)")
        cfg["export"]["source_profile"] = _ask_str("Source AWS profile")
        cfg["export"]["source_zone_id"] = _ask_zone_id("Source hosted zone id")
        cfg["export"]["out_dir"] = _ask_str("Output directory", default="./artifacts", required=True)
        cfg["export"]["exclude_types"] = _ask_str(
            "Exclude record types (comma-separated)",
            default=",".join(sorted(DEFAULT_EXCLUDE_TYPES)),
            required=True,
        )

        action = _ask_str("Import action for transformed output", default="UPSERT", required=True).upper()
        if action not in {"CREATE", "UPSERT"}:
            eprint("Invalid action. Using UPSERT.")
            action = "UPSERT"
        cfg["export"]["import_action"] = action
        cfg["export"]["batch_size"] = _ask_int("Batch size", default=DEFAULT_BATCH_SIZE)

        cfg["export"]["upload_to_s3"] = _ask_yes_no("Upload export artifacts to target S3", default_yes=True)

        if cfg["export"]["upload_to_s3"]:
            _print_section("Step 3: S3 settings (target account)")
            cfg["s3"]["bucket"] = _ask_str("Target S3 bucket")
            cfg["s3"]["prefix"] = _ask_str("Target S3 prefix (folder)", default="", required=False)
            cfg["export"]["target_profile"] = _ask_str("Target AWS profile (for S3 upload)")

    if mode in {"import", "both"}:
        _print_section("Step 4: Import settings (target hosted zone)")
        cfg["import"]["target_profile"] = _ask_str("Target AWS profile")
        cfg["import"]["target_zone_id"] = _ask_zone_id("Target hosted zone id")
        cfg["import"]["from_s3"] = _ask_yes_no("Import from S3", default_yes=True)
        if cfg["import"]["from_s3"]:
            if not cfg["s3"].get("bucket"):
                cfg["s3"]["bucket"] = _ask_str("S3 bucket (target account)")
            default_key = _default_change_batches_key(cfg["s3"].get("prefix"))
            cfg["import"]["s3_key"] = _ask_str("S3 key for change-batches.json", default=default_key, required=True)

        cfg["import"]["diff_against_target"] = True
        cfg["import"]["skip_noop_upserts"] = True

    _print_section("Step 5: Review")
    print(json.dumps(cfg, indent=2))
    if not _ask_yes_no("Write this config file", default_yes=True):
        print("Canceled. Config not written.")
        return 0

    out_path = _ask_str("Write config file path", default=DEFAULT_CONFIG_PATH, required=True)
    write_json(out_path, cfg)
    print(f"Wrote config: {out_path}")
    return 0


def _load_transformed_from_args(args: argparse.Namespace) -> Dict[str, Any]:
    if args.in_file:
        return read_json(args.in_file)

    cfg: Dict[str, Any] = getattr(args, "_config", {}) or {}
    bucket = args.s3_bucket or _cfg_get(cfg, "s3.bucket")
    key = args.s3_key or _cfg_get(cfg, "import.s3_key")

    if not bucket:
        bucket = prompt_if_missing(None, "S3 bucket in target account: ")

    if not key:
        default_key = _default_change_batches_key(_cfg_get(cfg, "s3.prefix"))
        raw = prompt_if_missing(None, f"S3 key for change-batches.json [default {default_key}]: ")
        key = raw.strip() or default_key

    while not str(key).strip():
        key = prompt_if_missing(None, "S3 key for change-batches.json (cannot be empty): ").strip()

    assert_profile_usable(args.target_profile)
    return s3_get_json(args.target_profile, bucket, key)


def cmd_export(args: argparse.Namespace) -> int:
    cfg: Dict[str, Any] = getattr(args, "_config", {}) or {}
    source_profile = args.source_profile or _cfg_get(cfg, "export.source_profile")
    source_zone_id = args.source_zone_id or _cfg_get(cfg, "export.source_zone_id")
    if not source_profile or not source_zone_id:
        raise RuntimeError("Missing required export inputs: source_profile and source_zone_id. Provide flags or --config.")

    out_dir = args.out_dir or _cfg_get(cfg, "export.out_dir") or "./artifacts"
    timestamp = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    raw_path = os.path.join(out_dir, "raw-recordsets.json")
    transformed_path = os.path.join(out_dir, "change-batches.json")

    raw = export_raw_recordsets(source_profile, source_zone_id, exported_at=utc_now_iso())

    if args.export_raw:
        write_json(raw_path, raw)
        print(f"Wrote raw export: {raw_path}")

    transformed: Optional[Dict[str, Any]] = None
    if args.export_transformed:
        transformed = transform_recordsets_to_change_batches(
            raw_export=raw,
            exclude_types=(args.exclude_types or _cfg_get(cfg, "export.exclude_types") or ",".join(sorted(DEFAULT_EXCLUDE_TYPES))).split(","),
            action=(args.import_action or _cfg_get(cfg, "export.import_action") or "UPSERT"),
            batch_size=(args.batch_size or int(_cfg_get(cfg, "export.batch_size") or DEFAULT_BATCH_SIZE)),
            transformed_at=utc_now_iso(),
        )
        write_json(transformed_path, transformed)
        print(f"Wrote transformed export: {transformed_path}")

    upload_to_s3 = bool(args.upload_to_s3)
    if not upload_to_s3:
        upload_to_s3 = bool(_cfg_get(cfg, "export.upload_to_s3", False))

    if upload_to_s3 or args.s3_bucket or _cfg_get(cfg, "s3.bucket"):
        bucket = args.s3_bucket or _cfg_get(cfg, "s3.bucket")
        if not bucket:
            bucket = prompt_if_missing(None, "S3 bucket in target account for storing exports: ")

        target_profile = args.target_profile or _cfg_get(cfg, "export.target_profile")
        if not target_profile:
            target_profile = prompt_if_missing(None, "Target AWS profile (for S3 upload): ")
        ident = assert_profile_usable(target_profile)
        print(f"Using target profile identity for S3 upload: {ident.get('Arn')}")

        prefix = args.s3_prefix or _cfg_get(cfg, "s3.prefix")
        if not prefix:
            prefix = prompt_if_missing(
                None,
                f"S3 prefix (folder) [default route53-migrator/{args.source_zone_id}/{timestamp}/]: ",
            )
        if not prefix:
            prefix = f"route53-migrator/{source_zone_id}/{timestamp}/"
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
    cfg: Dict[str, Any] = getattr(args, "_config", {}) or {}
    target_profile = args.target_profile or _cfg_get(cfg, "import.target_profile")
    target_zone_id = args.target_zone_id or _cfg_get(cfg, "import.target_zone_id")
    if not target_profile or not target_zone_id:
        raise RuntimeError("Missing required import inputs: target_profile and target_zone_id. Provide flags or --config.")

    args.target_profile = target_profile
    args.target_zone_id = target_zone_id

    ident = assert_profile_usable(target_profile)
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

    target_index = None
    if args.diff_against_target or args.skip_noop_upserts:
        try:
            target_index = fetch_target_index(target_profile, target_zone_id)

            if args.diff_against_target:
                diff = estimate_diff_against_target(batches_doc, target_index)
                print("Best-effort diff against target hosted zone:")
                print(json.dumps(diff, indent=2))

            if args.skip_noop_upserts:
                before = summarize_changes(batches_doc)
                batches_doc = filter_noop_upserts(batches_doc, target_index)
                after = summarize_changes(batches_doc)
                print("No-op UPSERT filtering:")
                print(
                    json.dumps(
                        {
                            "before_total_changes": before.get("total_changes"),
                            "after_total_changes": after.get("total_changes"),
                        },
                        indent=2,
                    )
                )
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

    results = apply_change_batches(target_profile, target_zone_id, batches_doc.get("batches", []) or [])

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
    parser.add_argument("-i", "--interactive", action="store_true", default=False)
    parser.add_argument("--config", default=None)

    sub = parser.add_subparsers(dest="command", required=False)

    p_export = sub.add_parser("export")
    p_export.add_argument("--source-profile", required=False)
    p_export.add_argument("--source-zone-id", required=False)
    p_export.add_argument("--out-dir", default=None)
    p_export.add_argument("--exclude-types", default=None)
    p_export.add_argument("--export-raw", action=argparse.BooleanOptionalAction, default=True)
    p_export.add_argument("--export-transformed", action=argparse.BooleanOptionalAction, default=True)
    p_export.add_argument("--import-action", choices=["CREATE", "UPSERT"], default=None)
    p_export.add_argument("--batch-size", type=int, default=None)
    p_export.add_argument("--upload-to-s3", action="store_true", default=False)
    p_export.add_argument("--s3-bucket")
    p_export.add_argument("--s3-prefix")
    p_export.add_argument("--target-profile")
    p_export.set_defaults(func=cmd_export)

    p_import = sub.add_parser("import")
    p_import.add_argument("--target-profile", required=False)
    p_import.add_argument("--target-zone-id", required=False)
    p_import.add_argument("--in-file")
    p_import.add_argument("--s3-bucket")
    p_import.add_argument("--s3-key")
    p_import.add_argument("--diff-against-target", action=argparse.BooleanOptionalAction, default=True)
    p_import.add_argument("--skip-noop-upserts", action=argparse.BooleanOptionalAction, default=True)
    p_import.add_argument("--dry-run", action=argparse.BooleanOptionalAction, default=True)
    p_import.add_argument("--apply", action="store_true", default=False)
    p_import.add_argument("--yes", action="store_true", default=False)
    p_import.add_argument("--print-results", action="store_true", default=False)
    p_import.set_defaults(func=cmd_import)

    return parser


def main(argv: Optional[List[str]] = None) -> int:
    parser = build_parser()

    if argv is None:
        argv = sys.argv[1:]

    args, remaining = parser.parse_known_args(argv)

    if args.interactive:
        return _wizard_write_config()

    cfg = _load_config(args.config or "") if args.config else {}

    global_argv: List[str] = []
    if args.config:
        global_argv.extend(["--config", args.config])

    if args.command is None:
        if sys.stdin.isatty():
            choice = prompt_if_missing(None, "Choose command [export/import]: ").strip().lower()
            if choice in {"e", "export"}:
                argv = global_argv + ["export"] + remaining
            elif choice in {"i", "import"}:
                argv = global_argv + ["import"] + remaining
            else:
                eprint("Invalid choice. Use 'export' or 'import'.")
                return 2
        else:
            parser.print_help()
            return 2

    args = parser.parse_args(argv)
    setattr(args, "_config", cfg)
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
