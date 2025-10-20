# Feature: Enhanced Summary with Destination Cluster Information

## Summary
Added ability to show destination cluster information when running `--action summary` with optional `--dst_host` parameter.

## What Changed

### New Functionality
1. **Optional Destination Info in Summary**: Users can now provide `--dst_host` and `--dst_user` with the `summary` action to see both source AND destination cluster information.

2. **New Method**: `ReplicationManager.get_destination_info()`
   - Retrieves cluster name, cluster ID, and target relationships from destination cluster
   - Returns structured dictionary for display

3. **Enhanced Display**: `ReplicationManager.display_status(dst_info=None)`
   - Now accepts optional `dst_info` parameter
   - Displays destination cluster summary including:
     - Cluster name and ID
     - All target replication relationships
     - Relationship states (ESTABLISHED, AWAITING_AUTHORIZATION, REPLICATING, etc.)
     - Source/target path mappings
     - State count summary

4. **Error Handling**: If connection to destination fails, tool gracefully falls back to showing only source info with error message.

## Usage

### Before (source only)
```bash
python3 generate_replications.py \
  --src_host df64.eng.qumulo.com \
  --src_user admin \
  --action summary
```

### After (source + destination)
```bash
python3 generate_replications.py \
  --basepath /test-qwalk-parent/ \
  --src_host df64.eng.qumulo.com \
  --src_user admin \
  --dst_host qwho.eng.qumulo.com \
  --dst_user admin \
  --action summary
```

## Example Output

```
Source Cluster Replication Summary:
==================================================================
Path                                | Target IP       | File ID   ...
------------------------------------------------------------------
/test-qwalk-parent/dirs/            | 10.120.3.55     | 846415...
/test-qwalk-parent/hub/             | 10.120.3.55     | 846415...

Target IP       | Replication Count
--------------------------------------
10.120.3.54     | 5
10.120.3.55     | 5


====================================================================================================
Destination Cluster Summary:
====================================================================================================
Cluster Name: qwho-cluster
Cluster ID: abc-123-def
Total Target Relationships: 12

Source Path                         | Target Path                         | State         | Source Cluster  | Replication ID
----------------------------------------------------------------------------------------------------------------------------
/test-qwalk-parent/dirs/            | /test-qwalk-parent/dirs/            | ESTABLISHED   | df64-cluster    | 83b2c21f-1989-48e6-a019-5efeaa9c5b74
/test-qwalk-parent/hub/             | /test-qwalk-parent/hub/             | REPLICATING   | df64-cluster    | 0a09c414-750d-4249-8e3e-ab5718a9b34c

State                     | Count
--------------------------------------
ESTABLISHED               | 10
REPLICATING              | 2
```

## Tests
- All new functionality is covered by unit tests in `test_replication_summary.py`
- 8 new tests added following TDD approach
- All existing tests continue to pass (backward compatible)
- Test coverage includes:
  - Summary without destination (backward compatibility)
  - Summary with destination info
  - Various relationship states
  - Empty relationships handling
  - Error handling

## Design Principles (KISS)
- **Minimal code changes**: Reused existing patterns and classes
- **Optional feature**: Works with or without `--dst_host`
- **Backward compatible**: Existing behavior unchanged when dst_host not provided
- **Fail gracefully**: Connection errors don't crash, show clear error messages
- **Separation of concerns**: Source display vs destination display logic separated
