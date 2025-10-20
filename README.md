# Qumulo Replication Helper

Automate Qumulo replication setup and management with intelligent load balancing.

## What It Does

- **Create replications** automatically from source directories to destination cluster
- **Accept pending replications** in bulk on destination cluster
- **Balance load** intelligently across destination floating IPs
- **View status** of existing replication relationships

## Quick Start

### Accept Pending Replications

```bash
python3 generate_replications.py \
  --dst_host dst.cluster.com \
  --dst_user admin \
  --action accept
```

### Create Replications

```bash
python3 generate_replications.py \
  --src_host src.cluster.com \
  --src_user admin \
  --dst_host dst.cluster.com \
  --dst_user admin \
  --basepath /data \
  --action create
```

### View Status

```bash
python3 generate_replications.py \
  --src_host src.cluster.com \
  --src_user admin \
  --action summary
```

## Installation

```bash
pip install qumulo-api
git clone <repo-url>
cd qrepli
```

## Usage

### Actions

| Action | What it does | Needs |
|--------|-------------|-------|
| `accept` | Accept pending replications on destination | `--dst_host`, `--dst_user` |
| `create` | Create new replications from source to destination | `--src_host`, `--src_user`, `--dst_host`, `--dst_user` |
| `summary` | Show replication status on source | `--src_host`, `--src_user` |
| `clean` | Delete replications under path | `--src_host`, `--src_user`, `--basepath` |

### Options

```
Source cluster (for summary/create/clean):
  --src_host HOST        Cluster hostname or IP
  --src_user USER        Username
  --src_password PASS    Password (prompts if not provided)

Destination cluster (for create/accept):
  --dst_host HOST        Cluster hostname or IP
  --dst_user USER        Username
  --dst_password PASS    Password (prompts if not provided)

Other:
  --basepath PATH        Directory to replicate (default: /)
  --dst_network_id ID    Network ID for FIPs (default: 1)
  --dst IP [IP ...]      Use specific destination IPs
  --allow_non_empty_dir  Allow replication to non-empty directories
  --confirm              Prompt before accepting replications
```

## Examples

### Accept with Confirmation

```bash
python3 generate_replications.py \
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

### Accept Non-Empty Directories

```bash
python3 generate_replications.py \
  --dst_host dst.cluster.com \
  --dst_user admin \
  --action accept \
  --allow_non_empty_dir
```

### Use Specific Destination IPs

```bash
python3 generate_replications.py \
  --src_host src.cluster.com \
  --src_user admin \
  --dst_host dst.cluster.com \
  --dst_user admin \
  --basepath /data/project \
  --dst 10.1.1.20 10.1.1.21 \
  --action create
```

### View Summary Table

```bash
python3 generate_replications.py \
  --src_host src.cluster.com \
  --src_user admin \
  --action summary
```

Shows:
```
Source Cluster Replication Summary:
========================================================
Path              | Target IP    | File ID      | Replication ID
--------------------------------------------------------
/data/project1/   | 10.1.1.20   | 846415...   | abc-123...
/data/project2/   | 10.1.1.21   | 846415...   | def-456...

Target IP       | Replication Count
--------------------------------------
10.1.1.20       | 5
10.1.1.21       | 3
```

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

## Common Issues

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

## Requirements

- Python 3.8+
- `qumulo-api` package
- Access to Qumulo clusters

## License

MIT License - use freely, modify as needed.

## Questions?

Open an issue on GitHub or check Qumulo documentation at https://care.qumulo.com
