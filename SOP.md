# SOP: Route53 Hosted Zone Migration (Export  Import  DNS Cutover)

## 1) Purpose
Migrate DNS records from a **source AWS account** to a **target AWS account** using `route53-migrator`, then perform a controlled DNS cutover. Optionally transfer domain registration and decommission the legacy account.

## 2) Scope / Assumptions
- Public hosted zone migrations (internet-facing DNS).
- Target hosted zone exists before import.
- `NS`/`SOA` are excluded from import (zone-owned).
- Artifacts can be stored locally and/or in S3.

## 3) Roles
- Operator: runs export/import, performs validation and cutover.
- Approver: confirms change window, approves cutover.

## 4) Inputs
- Source AWS profile (e.g. `ca-dev`).
- Target AWS profile (e.g. `ca-devops`).
- Source hosted zone id (e.g. `Z...`).
- Target hosted zone id (e.g. `Z...`).
- (Optional) Target S3 bucket + prefix to store exported artifacts.

## 5) Pre-checks (Before)
### 5.1 Confirm identities and permissions
```bash
aws --profile <source_profile> sts get-caller-identity
aws --profile <target_profile> sts get-caller-identity
```

### 5.2 Ensure target hosted zone exists
- Route53 Console (target account)  Hosted zones  Create hosted zone (Public)
- Record target `HostedZoneId`.

### 5.3 Determine cutover type
- **Registrar cutover**: domain is registered at a registrar (Route53 Domains or external)
- **Parent-zone delegation**: subdomain delegated via an `NS` record in a parent hosted zone

### 5.4 Optional: reduce TTLs ahead of cutover
- Reduce TTLs to 60300 seconds for key records if possible.

### 5.5 DNSSEC check (recommended)
```bash
aws --profile <registrar_profile> route53domains get-domain-detail \
  --domain-name <domain> --query "DnsSec"
```

## 6) Configure (Wizard)
Create a reusable config:
```bash
uv run route53-migrator -i
```
Default output:
- `./route53-migrator.config.json`

## 7) Export (During)
Run export:
```bash
uv run route53-migrator --config ./route53-migrator.config.json export
```
Outputs:
- `./artifacts/raw-recordsets.json`
- `./artifacts/change-batches.json`
- optional S3:
  - `s3://<bucket>/<prefix>/raw-recordsets.json`
  - `s3://<bucket>/<prefix>/change-batches.json`

## 8) Import (During)
Run import (includes dry-run + confirmation):
```bash
uv run route53-migrator --config ./route53-migrator.config.json import
```
Wait for INSYNC:
```bash
aws --profile <target_profile> route53 get-change --id /change/XXXXXXXX
```

## 9) Validate after Import (Before Cutover)
### 9.1 Confirm records exist in target zone
```bash
aws --profile <target_profile> route53 list-resource-record-sets \
  --hosted-zone-id <TARGET_ZONE_ID> \
  --query "ResourceRecordSets[?Type!='NS' && Type!='SOA'] | length(@)"
```

### 9.2 Spot-check critical names
```bash
aws --profile <target_profile> route53 list-resource-record-sets \
  --hosted-zone-id <TARGET_ZONE_ID> \
  --query "ResourceRecordSets[?Name=='web.<domain>.' && Type=='A']"
```

## 10) DNS Cutover (During)
### 10.1 Registrar cutover (registered domain)
1) Get target name servers:
```bash
aws --profile <target_profile> route53 get-hosted-zone --id <TARGET_ZONE_ID> \
  --query "DelegationSet.NameServers" --output text
```
2) Update registrar name servers to that set.

### 10.2 Parent-zone delegation cutover (subdomain)
- UPSERT the `NS` record for the subdomain in the parent hosted zone to match the target hosted zones name servers.

## 11) Post-cutover validation (After)
### 11.1 Confirm delegation
```bash
dig <domain> NS +short @8.8.8.8
dig <domain> NS +short @1.1.1.1
```

### 11.2 Confirm key records via public resolvers
```bash
dig web.<domain> A @8.8.8.8
dig api.<domain> A @1.1.1.1
```

### 11.3 If you need authoritative validation
```bash
dig web.<domain> A @<one_of_target_nameservers>
```

## 12) Rollback SOP
### 12.1 Registrar rollback
- Change registrar name servers back to old name servers.

### 12.2 Parent-zone rollback
- UPSERT parent-zone NS record back to old name servers.

## 13) Legacy account decommission (Optional)
### 13.1 Keep old hosted zone temporarily
- Keep for 2472 hours after cutover (or longer for production) for rollback safety.

### 13.2 Transfer registration out of legacy account (if applicable)
- Transfer the domain registration to the target AWS account or to an external registrar before closing the legacy account.

### 13.3 Cleanup
- Archive artifacts (local/S3) for audit.
- Delete old hosted zone only after registration and cutover are confirmed stable.
