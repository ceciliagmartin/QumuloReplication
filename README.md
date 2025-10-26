# Qumulo Replication Helper

Automate Qumulo replication setup and management with intelligent load balancing and flexible reporting.

## What It Does

- **Create replications** automatically from source directories to destination cluster
- **Selective replication** with include/exclude filters (replicate only prod, skip test/temp)
- **Custom destination paths** for organized backup hierarchies (e.g., `/dr/backups/...`)
- **Accept pending replications** in bulk on destination cluster
- **Clean replications** on source and remove ENDED relationships on destination
- **Balance load** intelligently across destination floating IPs
- **View status** with multiple display formats (table, card, CSV)
- **Export to CSV** for analysis in Excel/Google Sheets

## Quick Start

### Actions

| Action | What it does | Required Arguments |
|--------|-------------|-------------------|
| `summary` | Show replication status (supports --format and --csv) | `--src_host`, `--src_user` |
| `create` | Create new replications from source to destination | `--src_host`, `--src_user`, `--dst_host`, `--dst_user`, `--basepath` |
| `accept` | Accept pending replications on destination | `--dst_host`, `--dst_user` |
| `clean` | Delete replications (source-side) and/or clean ENDED relationships (destination-side) | Source: `--src_host`, `--src_user` / Destination: `--dst_host`, `--dst_user` (can use both) |

### View Replication Status

```bash
# Source cluster only (table format)
python3 replication.py \
  --src_host src.cluster.com \
  --src_user admin \
  --action summary

# Source + Destination (both clusters)
python3 replication.py \
  --src_host src.cluster.com \
  --src_user admin \
  --dst_host dst.cluster.com \
  --dst_user admin \
  --action summary

# Card format (easier to read for health checks)
python3 replication.py \
  --src_host src.cluster.com \
  --src_user admin \
  --action summary \
  --format card

# Export to CSV
python3 replication.py \
  --src_host src.cluster.com \
  --src_user admin \
  --action summary \
  --csv status.csv
```

### Accept Pending Replications

```bash
python3 replication.py \
  --dst_host dst.cluster.com \
  --dst_user admin \
  --action accept
```

### Create Replications

```bash
# Basic: replicate all directories under /data
python3 replication.py \
  --src_host src.cluster.com \
  --src_user admin \
  --dst_host dst.cluster.com \
  --dst_user admin \
  --basepath /data \
  --action create

# Advanced: replicate only production to DR backup path
python3 replication.py \
  --src_host src.cluster.com \
  --src_user admin \
  --dst_host dst.cluster.com \
  --dst_user admin \
  --basepath /data \
  --dst_path /dr/backups \
  --filteri "prod" \
  --action create
```

## Installation

```bash
pip install qumulo-api
git clone <repo-url>
cd qrepli
```

## Display Formats

### Table Format (Default)

Fixed-width columns, truncated paths for readability:

```
==================================================================================================================================
Source Cluster Summary:
==================================================================================================================================
Cluster Name: qtest
Cluster ID: fb9119f3-9ecd-4110-b6c4-44f65ecec3...
Total Relationships: 2

Source Path                         | Target Path                         | State           | Target Cluster      | ID (truncated)
------------------------------------------------------------------------------------------------------------------------------------
/snapz/                             | /snapz/                             | ESTABLISHED     | qwhat               | 93874ed3-9c03-49
/Users/                             | /Users/                             | ESTABLISHED     | qwho                | b3b7d559-e89c-43

State                     | Count
--------------------------------------
ESTABLISHED               | 2
```

### Card Format (Best for Health Checks)

Visual indicators, prominent error display:

```
====================================================================================================
Source Cluster Summary:
====================================================================================================
Cluster: qtest (fb9119f3-9ecd-41...)
Total Relationships: 2

▸ /snapz/ → qwhat:/snapz/
  State: ESTABLISHED ✗
  ID: 93874ed3-9c03-49...
  ⚠ Error: Target cluster error: Snapshot limit of 40000 has been reached.
  Recovery Point: 2025-09-25 13:10:00
  Queued Snapshots: 2102
  Mode: Snapshot Policy

▸ /Users/ → qwho:/Users/
  State: ESTABLISHED ✓
  ID: b3b7d559-e89c-43...
  Recovery Point: 2025-10-21 06:40:30
  Mode: Continuous

State Summary:
  ✓ ESTABLISHED: 2
```

**Card Format Icons:**
- `✓` = Healthy (ESTABLISHED, no errors)
- `⟳` = In progress (REPLICATING, CREATING)
- `✗` = Problem (errors, DISCONNECTED)
- `⚠` = Warning (other states)

### CSV Export

Full data with no truncation, ready for Excel/analysis:

```csv
cluster_type,cluster_name,cluster_id,source_path,target_path,remote_cluster,state,replication_id,error,recovery_point,queued_snapshots,replication_mode
Source,qtest,fb9119f3-9ecd-4110-b6c4-44f65ecec31f,/snapz/,/snapz/,qwhat,ESTABLISHED,93874ed3-9c03-49a4-a628-778b4b5d831d,Target cluster error: Snapshot limit,2025-09-25T13:10:00.000439663Z,2102,REPLICATION_SNAPSHOT_POLICY
Source,qtest,fb9119f3-9ecd-4110-b6c4-44f65ecec31f,/Users/,/Users/,qwho,ESTABLISHED,b3b7d559-e89c-4323-a408-efeb38f60eb6,,2025-10-21T06:40:30.804453983Z,0,REPLICATION_CONTINUOUS
```

## Usage

### Actions

| Action | What it does | Required Arguments |
|--------|-------------|-------------------|
| `summary` | Show replication status (supports --format and --csv) | `--src_host`, `--src_user` |
| `create` | Create new replications from source to destination | `--src_host`, `--src_user`, `--dst_host`, `--dst_user`, `--basepath` |
| `accept` | Accept pending replications on destination | `--dst_host`, `--dst_user` |
| `clean` | Delete replications (source-side) and/or clean ENDED relationships (destination-side) | Source: `--src_host`, `--src_user` / Destination: `--dst_host`, `--dst_user` (can use both) |

### Summary Options

```bash
--format {table,card}      Display format (default: table)
                          table = Fixed-width columns, compact
                          card  = Readable blocks, good for health checks

--csv FILEPATH            Save summary to CSV file
                          Example: --csv status.csv

--csv-only                Export ONLY to CSV, no screen output
                          (requires --csv, good for automation)
```

### Connection Options

```
Source cluster (for summary/create/clean):
  --src_host HOST        Cluster hostname or IP
  --src_user USER        Username
  --src_password PASS    Password (prompts if not provided)

Destination cluster (for create/accept, optional for summary):
  --dst_host HOST        Cluster hostname or IP
  --dst_user USER        Username
  --dst_password PASS    Password (prompts if not provided)
```

### Create Options

```
  --basepath PATH              Directory to replicate (default: /)
  --dst_path PATH              Prepend path to destination targets (default: empty)
                               Example: /data/proj with --dst_path /backup → /backup/data/proj
  --dst_network_id ID          Network ID for FIPs (default: 1)
  --dst IP [IP ...]            Use specific destination IPs
  --allow_non_empty_dir        Allow replication to non-empty directories
  --confirm                    Prompt before accepting replications
```

### Filtering Options (Create and Clean Actions Only)

Filter which directories to replicate (create) or delete (clean). **Cannot use both filters together.**

**Supported actions:** `create`, `clean`
**Not supported:** `summary`, `accept` (these show/accept all existing replications)

```
  --filteri STRING [STRING ...]      Include ONLY directories containing these strings
                                     Example: --filteri "prod" "staging"
                                     Use case: Replicate only production/staging dirs

  --filtere STRING [STRING ...]      Exclude directories containing these strings
                                     Example: --filtere "test" "temp" "cache"
                                     Use case: Skip test/temporary directories

  No filter (default)                Replicate ALL subdirectories under basepath
```

**String matching (simple substring search):**
- `"prod"` matches `prod-db`, `prod-app`, `production`, etc.
- `"test"` matches `test-db`, `testing`, `my-test`, etc.
- Case-sensitive matching
- Matches directory names only, not full paths

## Examples

### Quick Health Check (Card Format)

```bash
python3 replication.py \
  --src_host src.cluster.com \
  --src_user admin \
  --action summary \
  --format card
```

Shows errors prominently with ⚠ indicators.

### Export Status to CSV

```bash
# Display on screen AND save to CSV
python3 replication.py \
  --src_host src.cluster.com \
  --src_user admin \
  --action summary \
  --csv replication_status.csv

# Export ONLY (no screen output, good for cron jobs)
python3 replication.py \
  --src_host src.cluster.com \
  --src_user admin \
  --action summary \
  --csv status.csv \
  --csv-only
```

### Compare Source and Destination

```bash
python3 replication.py \
  --src_host src.cluster.com \
  --src_user admin \
  --dst_host dst.cluster.com \
  --dst_user admin \
  --action summary \
  --format card
```

Shows both clusters in readable card format.

### Accept with Confirmation

```bash
python3 replication.py \
  --dst_host dst.cluster.com \
  --dst_user admin \
  --action accept \
  --confirm
```

Shows:
```
Found 3 pending replication relationship(s):
  ID: abc-123
  Source Cluster: production
  Source Path: /data/critical/
  Target Path: /data/critical/
  State: AWAITING_AUTHORIZATION

Accept all 3 replication(s)? (yes/no): yes

Successfully accepted 3 of 3 replication relationship(s)
```

### Create with Specific IPs

```bash
python3 replication.py \
  --src_host src.cluster.com \
  --src_user admin \
  --dst_host dst.cluster.com \
  --dst_user admin \
  --basepath /data/project \
  --dst 10.1.1.20 10.1.1.21 \
  --action create
```

Creates replications using only the specified destination IPs.

### Disaster Recovery: Replicate to Backup Path

Use `--dst_path` to prepend a path on the destination cluster, creating a dedicated backup directory structure.

```bash
# Source /data/prod-db1, /data/prod-db2
# Destination: /dr/backups/data/prod-db1, /dr/backups/data/prod-db2

python3 replication.py \
  --src_host src.cluster.com \
  --src_user admin \
  --dst_host dst.cluster.com \
  --dst_user admin \
  --basepath /data \
  --dst_path /dr/backups \
  --action create
```

**Result:** All subdirectories under `/data` are replicated to `/dr/backups/data/*` on the destination.

### Selective Replication: Include Only Production

Use `--filteri` to replicate ONLY directories containing specific strings.

```bash
# Only replicate directories containing "prod" or "critical"
# Skips: test-db, dev-app, staging-cache, etc.

python3 replication.py \
  --src_host src.cluster.com \
  --src_user admin \
  --dst_host dst.cluster.com \
  --dst_user admin \
  --basepath /data \
  --filteri "prod" "critical" \
  --action create
```

**Real-world scenario:** You have `/data/prod-db1`, `/data/prod-db2`, `/data/test-db`, `/data/dev-app`. Only `prod-db1` and `prod-db2` are replicated (they contain "prod").

### Selective Replication: Exclude Test/Temp Directories

Use `--filtere` to skip directories you don't want to replicate.

```bash
# Replicate everything EXCEPT directories containing test, temp, or cache

python3 replication.py \
  --src_host src.cluster.com \
  --src_user admin \
  --dst_host dst.cluster.com \
  --dst_user admin \
  --basepath /data \
  --filtere "test" "temp" "cache" \
  --action create
```

**Real-world scenario:** Exclude test databases, temporary files, and cache directories from replication to save bandwidth and storage.

### Combined: Backup Production to DR Site

Combine `--dst_path` and `--filteri` for targeted disaster recovery.

```bash
# Replicate ONLY production databases to dedicated DR backup path
# Source: /data/prod-db1, /data/prod-db2
# Destination: /dr-site/backups/data/prod-db1, /dr-site/backups/data/prod-db2

python3 replication.py \
  --src_host production.cluster.com \
  --src_user admin \
  --dst_host dr.cluster.com \
  --dst_user admin \
  --basepath /data \
  --dst_path /dr-site/backups \
  --filteri "prod" \
  --action create
```

**Benefits:**
- Only production data replicated (saves time/bandwidth)
- Organized under dedicated DR path on destination
- Test/dev/staging directories automatically excluded

### Clean Replications

The `clean` action intelligently handles cleanup based on which cluster credentials you provide:

**Source-side cleanup:** Deletes replication relationships from the source cluster under the specified basepath.

**Destination-side cleanup:** Removes ENDED relationships from the destination cluster. These are remnant relationships that remain in ENDED state after being deleted from the source.

**What are ENDED relationships?**
When you delete a replication from the source cluster, the corresponding relationship on the destination cluster transitions to ENDED state. These ENDED relationships persist on the destination and should be cleaned up to maintain cluster hygiene.

```bash
# Clean source-side replications only
python3 replication.py \
  --src_host src.cluster.com \
  --src_user admin \
  --basepath /data/project \
  --action clean

# Clean destination-side ENDED relationships only
python3 replication.py \
  --dst_host dst.cluster.com \
  --dst_user admin \
  --basepath /data/project \
  --action clean

# Clean both source and destination at once
python3 replication.py \
  --src_host src.cluster.com \
  --src_user admin \
  --dst_host dst.cluster.com \
  --dst_user admin \
  --basepath /data/project \
  --action clean
```

**Typical workflow:**
1. Delete replications from source: `--action clean` with `--src_host`
2. Clean up ENDED relationships on destination: `--action clean` with `--dst_host`
3. Or do both in one command by providing both source and destination credentials

## How Load Balancing Works

The tool automatically balances replications across destination floating IPs:

1. Queries destination cluster for available floating IPs
2. Tracks how many replications each IP already has
3. Assigns new replications to the least-used IP
4. Results in even distribution across nodes

Example: With 3 destination IPs and 10 replications, each IP gets 3-4 replications.

## Security

**Passwords:** If you don't provide `--*_password`, you'll be prompted securely:
```
Enter password for admin@dst.cluster.com: ****
```

This is safer than putting passwords in command lines (which show up in shell history).

For automation, you can still provide passwords via CLI arguments.

## Common Use Cases

### Production-Only Disaster Recovery

Replicate only production databases to a dedicated DR site, excluding test/dev:

```bash
# Replicate directories containing "prod" to /dr-site/backups
python3 replication.py \
  --src_host production.cluster.com \
  --src_user admin \
  --dst_host dr.cluster.com \
  --dst_user admin \
  --basepath /databases \
  --dst_path /dr-site/backups \
  --filteri "prod" \
  --action create
```

### Multi-Environment Selective Replication

Replicate both production and staging, but exclude test environments:

```bash
# Include directories containing "prod" or "staging"
python3 replication.py \
  --src_host src.cluster.com \
  --src_user admin \
  --dst_host dst.cluster.com \
  --dst_user admin \
  --basepath /apps \
  --filteri "prod" "staging" \
  --action create
```

### Exclude Temporary/Cache Directories

Replicate everything except temporary and cache directories:

```bash
# Skip directories containing temp, cache, or test
python3 replication.py \
  --src_host src.cluster.com \
  --src_user admin \
  --dst_host dst.cluster.com \
  --dst_user admin \
  --basepath /data \
  --filtere "temp" "cache" "test" \
  --action create
```

### Daily Health Check

```bash
# Check for errors/warnings in card format
python3 replication.py \
  --src_host src.cluster.com \
  --src_user admin \
  --action summary \
  --format card | grep -E "✗|⚠"
```

### Weekly Status Report

```bash
# Export to CSV for analysis
python3 replication.py \
  --src_host src.cluster.com \
  --src_user admin \
  --dst_host dst.cluster.com \
  --dst_user admin \
  --action summary \
  --csv weekly_status_$(date +%Y%m%d).csv
```

### Automation/Monitoring

```bash
# Export only (no terminal output)
python3 replication.py \
  --src_host src.cluster.com \
  --src_user admin \
  --action summary \
  --csv /var/log/replication/status.csv \
  --csv-only
```

Then parse the CSV with your monitoring tool.

## Common Issues

**"Cluster configuration returned default values"**
- Warning appears when cluster name/ID can't be retrieved
- Usually harmless, but check API permissions if persistent
- Code uses "Unknown" as fallback and continues working

**"Target directory not empty"**
```bash
# Add --allow_non_empty_dir flag
--action accept --allow_non_empty_dir
```

**"Network ID not found"**
```bash
# Specify correct network ID
--dst_network_id 2
```

**Long paths truncated in table**
- Table format uses fixed widths (35 chars for paths)
- Use `--csv` to get full paths without truncation
- Or use `--format card` for full path display

## Output Format Comparison

| Format | Best For | Pros | Cons |
|--------|----------|------|------|
| **Table** (default) | Terminal viewing, quick scan | Compact, predictable width (~130 chars) | Paths/IDs truncated |
| **Card** | Health checks, error spotting | Visual indicators, full context, errors prominent | More vertical space |
| **CSV** | Analysis, automation, archival | Full data, no truncation, Excel-ready | Requires separate viewer |

## Requirements

- Python 3.8+
- `qumulo-api` package
- Access to Qumulo clusters with appropriate permissions

## License

MIT License - use freely, modify as needed.

## Questions?

Open an issue on GitHub or check Qumulo documentation for more information.
