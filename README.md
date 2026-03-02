# Route53 Migrator

CLI to export Route53 hosted zone record sets from a source account and import them into a target hosted zone, with optional S3 storage of artifacts in the target account.

## Prerequisites

- `uv`
- AWS profiles configured (typically in `~/.aws/config` and `~/.aws/credentials`)

## Install / Setup

```bash
uv sync
```

## Usage

Show help:

```bash
uv run route53-migrator --help
uv run route53-migrator export --help
uv run route53-migrator import --help
```

If you run without a command, it will prompt:

```bash
uv run route53-migrator
```

If you pass `--config` without a command, it will still prompt and preserve your config:

```bash
uv run route53-migrator --config ./route53-migrator.config.json
```

Interactive init (creates a reusable config file):

```bash
uv run route53-migrator -i
```

The wizard is step-based (mode -> export -> s3 -> import -> review) and validates common inputs.

Then run with config defaults:

```bash
uv run route53-migrator --config ./route53-migrator.config.json export
uv run route53-migrator --config ./route53-migrator.config.json import
```

### Export

Export to local artifacts:

```bash
uv run route53-migrator export \
  --source-profile <source_profile> \
  --source-zone-id <Z_SOURCE_ZONE_ID> \
  --out-dir ./artifacts
```

Export using config defaults:

```bash
uv run route53-migrator --config ./route53-migrator.config.json export
```

Export and upload artifacts to S3 in the target account:

```bash
uv run route53-migrator export \
  --source-profile <source_profile> \
  --source-zone-id <Z_SOURCE_ZONE_ID> \
  --upload-to-s3 \
  --target-profile <target_profile>
```

Artifacts created:

- `raw-recordsets.json` (raw record sets from source)
- `change-batches.json` (import-ready change batches)

### Import

Import from local transformed artifacts:

```bash
uv run route53-migrator import \
  --target-profile <target_profile> \
  --target-zone-id <Z_TARGET_ZONE_ID> \
  --in-file ./artifacts/change-batches.json
```

Import using config defaults:

```bash
uv run route53-migrator --config ./route53-migrator.config.json import
```

Import from S3:

```bash
uv run route53-migrator import \
  --target-profile <target_profile> \
  --target-zone-id <Z_TARGET_ZONE_ID> \
  --s3-bucket <bucket> \
  --s3-key <prefix>/change-batches.json
```

Dry-run only (non-interactive):

```bash
uv run route53-migrator import \
  --target-profile <target_profile> \
  --target-zone-id <Z_TARGET_ZONE_ID> \
  --s3-bucket <bucket> \
  --s3-key <prefix>/change-batches.json \
  --yes --dry-run
```

Apply without prompts:

```bash
uv run route53-migrator import \
  --target-profile <target_profile> \
  --target-zone-id <Z_TARGET_ZONE_ID> \
  --s3-bucket <bucket> \
  --s3-key <prefix>/change-batches.json \
  --yes --no-dry-run
```

## Dry-run diff and skipping no-op UPSERTs

During import, the tool computes a best-effort diff against the target hosted zone and reports:

- create vs update vs noop (for UPSERT)

By default, it also filters out **no-op UPSERTs** to keep imports smaller and safer.

Flags:

- `--diff-against-target/--no-diff-against-target`
- `--skip-noop-upserts/--no-skip-noop-upserts`

## Troubleshooting

- If you see `ModuleNotFoundError: botocore`, run `uv sync`.
- Ensure your AWS profiles have permissions to list and change Route53 record sets.
