#!/usr/bin/env python3
"""
MIT License

Copyright (c) 2025 Cecilia Martin

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.

Discover subfolders with max-depth1 and generate replication relationships
"""

import argparse
import logging
import sys
from dataclasses import dataclass
from datetime import datetime
from typing import List, Dict, Optional, Any, Tuple
from qqbase import Client, RestClient, create_credentials
from qqutils import display_table, export_csv

# Generate timestamped log filename in current directory
log_filename = f"replication_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

# Configure logging to both console and file
logger = logging.getLogger(__name__)

# Create formatter
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")

# Console handler
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(formatter)

# File handler (always created in current directory)
file_handler = logging.FileHandler(log_filename, mode="a")
file_handler.setLevel(logging.INFO)
file_handler.setFormatter(formatter)

# Configure root logger with both handlers
root_logger = logging.getLogger()
root_logger.setLevel(logging.INFO)
# Clear any existing handlers to prevent duplicates
root_logger.handlers.clear()
root_logger.addHandler(console_handler)
root_logger.addHandler(file_handler)

# Log startup message with file location
logger.info(f"Logging to file: {log_filename}")


@dataclass
class CreatedRelationship:
    source_path: str
    dst_address: str
    dst_path: str
    replication_id: str


class TargetCluster:
    def __init__(
        self,
        client: RestClient,
        user_provided_ips: Optional[List[str]] = None,
        network_name: str = "Default",
    ) -> None:
        self.client = client.rc
        self.dst_load = {}
        self.available_ips = []
        self.network_name = network_name

        if user_provided_ips:
            self.available_ips = self._validate_ips(user_provided_ips)
        else:
            self.available_ips = self.get_dst_ips()

        for ip in self.available_ips:
            self.dst_load[ip] = 0

        if len(self.available_ips) == 1:
            logger.warning(
                f"Only one IP ({self.available_ips[0]}) configured. Replications may be unbalanced."
            )

    def get_dst_ips(self) -> List[str]:
        """Retrieve all floating IPs from destination cluster by network name"""
        fip_data = []

        for node_status in self.client.network.list_network_status_v2():
            network_list = node_status["network_statuses"]

            # Find network by name
            matching_network = None
            for network in network_list:
                if network.get("name") == self.network_name:
                    matching_network = network
                    break

            if not matching_network:
                available = [n.get("name") for n in network_list]
                raise ValueError(
                    f"Network '{self.network_name}' not found on node {node_status.get('node_name', 'unknown')}. "
                    f"Available networks: {', '.join(available)}"
                )

            floating_addresses = matching_network.get("floating_addresses", [])
            # Extend fip_data with all FIPs from this node's network
            fip_data.extend(floating_addresses)

        logger.info(
            f'Using network "{self.network_name}" - Dst cluster FIPs: {fip_data}'
        )

        if not fip_data:
            raise ValueError(
                f"Network '{self.network_name}' has no Floating IPs available"
            )

        return fip_data

    def _validate_ips(self, user_ips: List[str]) -> List[str]:
        """Validate that user-provided IPs exist in cluster FIPs"""
        cluster_fips = self.get_dst_ips()
        invalid_ips = [ip for ip in user_ips if ip not in cluster_fips]

        if invalid_ips:
            raise ValueError(
                f"Invalid destination IPs not found in cluster FIPs: {invalid_ips}"
            )

        logger.info(f"Validated {len(user_ips)} destination IP(s): {user_ips}")
        return user_ips

    def get_next_dst_ip(self) -> str:
        """Get the IP with the minimum load (least-used strategy)"""
        if not self.dst_load:
            raise ValueError("No destination IPs available for load balancing")
        # Find the IP with the minimum load
        min_ip = min(self.dst_load, key=self.dst_load.get)
        # Increment its load counter
        self.dst_load[min_ip] += 1
        return min_ip

    def get_destination_info(self) -> Dict[str, Any]:
        """
        Retrieve destination cluster information including target relationships.

        Raises:
            Exception: If cluster configuration or relationships cannot be retrieved
        """
        cluster_name = self.client.cluster.get_cluster_conf()["cluster_name"]

        target_relationships = (
            self.client.replication.list_target_relationship_statuses()
        )

        return {
            "cluster_name": cluster_name,
            "relationships": target_relationships,
        }

    def clean_ended_replications(self, basepath: str = "/") -> int:
        """
        Clean up ENDED replication relationships on destination cluster.

        ENDED relationships are left behind when source-side relationships are deleted.
        This method removes them from the destination cluster.

        Args:
            basepath: Only clean relationships under this path (default: /)

        Returns:
            Number of relationships deleted
        """
        # Get target relationships
        target_relationships = (
            self.client.replication.list_target_relationship_statuses()
        )

        deleted_count = 0
        ended_relationships = []

        # Find ENDED relationships under basepath
        for rel in target_relationships:
            target_path = rel.get("target_root_path", "")
            state = rel.get("state", "")
            rel_id = rel.get("id", "")
            source_cluster = rel.get("source_cluster_name", "")

            if state == "ENDED" and target_path.startswith(basepath):
                ended_relationships.append(
                    {
                        "id": rel_id,
                        "path": target_path,
                        "source_cluster": source_cluster,
                    }
                )

        if not ended_relationships:
            logger.info(f"No ENDED relationships found under {basepath}")
            return 0

        logger.info(f"Found {len(ended_relationships)} ENDED relationship(s) to delete")

        # Delete each ENDED relationship
        failed_count = 0
        for rel in ended_relationships:
            logger.info(
                f"Deleting ENDED relationship: {rel['id']} (path: {rel['path']}, source: {rel['source_cluster']})"
            )
            try:
                self.client.replication.delete_target_relationship(rel["id"])
                deleted_count += 1
                logger.info(f"Successfully deleted relationship {rel['id']}")
            except Exception as e:
                logger.error(f"Failed to delete relationship {rel['id']}: {e}")
                failed_count += 1

        if failed_count:
            logger.warning(
                f"Failed to delete {failed_count} ENDED relationship(s) — check logs above for details"
            )
        logger.info(
            f"Deleted {deleted_count} of {len(ended_relationships)} ENDED relationship(s)"
        )
        return deleted_count

    def accept_pending_replications(
        self,
        allow_non_empty_dir: bool = False,
        confirm: bool = False,
        filteri: Optional[List[str]] = None,
        filtere: Optional[List[str]] = None,
    ) -> List[Dict]:
        """
        Accept pending replication relationships on destination cluster.

        Args:
            allow_non_empty_dir: Allow replication into non-empty directories (default: False)
            confirm: Require user confirmation before accepting (default: False)
            filteri: Include only relationships whose target path contains these strings
            filtere: Exclude relationships whose target path contains these strings

        Returns:
            List of accepted relationship details
        """
        logger.info(
            "Querying destination cluster for pending replication relationships..."
        )

        # Get all target relationship statuses
        target_relationships = (
            self.client.replication.list_target_relationship_statuses()
        )

        # Filter for pending/unauthorized relationships
        pending_relationships = []
        for rel in target_relationships:
            # Relationships waiting for authorization will have state or status field
            state = rel.get("state", "")
            relationship_state = rel.get("relationship_state", "")

            # Check both possible state fields
            if (
                state.upper() == "AWAITING_AUTHORIZATION"
                or state.upper() == "PENDING"
                or relationship_state.upper() == "AWAITING_AUTHORIZATION"
                or relationship_state.upper() == "PENDING"
            ):
                pending_relationships.append(rel)

        # Apply path filters on target_root_path directory name
        if filteri or filtere:
            filtered = []
            for rel in pending_relationships:
                dir_name = rel.get("target_root_path", "").rstrip("/").split("/")[-1]
                if filteri:
                    if not any(pattern in dir_name for pattern in filteri):
                        logger.info(
                            f"Skipping {rel.get('target_root_path')} - does not match include filter"
                        )
                        continue
                if filtere:
                    if any(pattern in dir_name for pattern in filtere):
                        logger.info(
                            f"Skipping {rel.get('target_root_path')} - matches exclude filter"
                        )
                        continue
                filtered.append(rel)
            pending_relationships = filtered

        if not pending_relationships:
            logger.info(
                "No pending replication relationships found on destination cluster."
            )
            return []

        # Display pending relationships
        print(
            f"\nFound {len(pending_relationships)} pending replication relationship(s):"
        )
        print("-" * 100)
        for rel in pending_relationships:
            rel_id = rel.get("id", "N/A")
            source_cluster = rel.get("source_cluster_name", "N/A")
            source_path = rel.get("source_root_path", "N/A")
            target_path = rel.get("target_root_path", "N/A")
            state = rel.get("state", "N/A")
            print(f"  ID: {rel_id}")
            print(f"  Source Cluster: {source_cluster}")
            print(f"  Source Path: {source_path}")
            print(f"  Target Path: {target_path}")
            print(f"  State: {state}")
            print("-" * 100)

        # Confirm if required
        if confirm:
            response = input(
                f"\nAccept all {len(pending_relationships)} replication(s)? (yes/no): "
            )
            if response.lower() not in ["yes", "y"]:
                logger.info("Acceptance cancelled by user.")
                return []

        # Accept each pending relationship
        accepted = []
        for rel in pending_relationships:
            rel_id = rel.get("id")
            target_path = rel.get("target_root_path", "N/A")
            source_cluster = rel.get("source_cluster_name", "N/A")

            try:
                logger.info(f"Accepting replication {rel_id} for path {target_path}...")
                # Call authorize - RestClient already authenticated
                self.client.replication.authorize(
                    rel_id,
                    allow_non_empty_directory=allow_non_empty_dir,
                    allow_fs_path_create=True,
                )
                logger.info(f"Successfully accepted replication {rel_id}")
                accepted.append(
                    {
                        "id": rel_id,
                        "target_path": target_path,
                        "source_cluster": source_cluster,
                    }
                )
            except Exception as e:
                error_msg = str(e)
                # Extract just the error message, not the full backtrace
                if "Error 400:" in error_msg:
                    # Parse out just the error type and message
                    error_parts = error_msg.split("\n")[0]  # First line only
                    if "not_empty" in error_msg.lower() and not allow_non_empty_dir:
                        logger.error(
                            f"Failed to accept {rel_id}: Target directory not empty. Use --allow_non_empty_dir to override."
                        )
                    else:
                        logger.error(f"Failed to accept {rel_id}: {error_parts}")
                else:
                    logger.error(f"Failed to accept replication {rel_id}: {error_msg}")

        # Summary
        print(
            f"\nSuccessfully accepted {len(accepted)} of {len(pending_relationships)} replication relationship(s)"
        )
        if accepted:
            print("\nAccepted Replications:")
            print("=" * 100)
            for acc in accepted:
                print(f"  Path: {acc['target_path']}")
                print(f"  Source Cluster: {acc['source_cluster']}")
                print(f"  ID: {acc['id']}")
                print("-" * 100)

        return accepted


class ReplicationManager:
    def __init__(self, client: RestClient) -> None:
        self.client = client.rc
        self.repli_paths = {}
        self.created_replications = {}

    def get_source_info(self) -> Dict[str, Any]:
        """
        Retrieve source cluster information including source relationships.

        Returns:
            Dict containing cluster_name, cluster_id, and list of relationships

        Raises:
            Exception: If cluster configuration or relationships cannot be retrieved
        """
        cluster_name = self.client.cluster.get_cluster_conf()["cluster_name"]

        source_relationships = (
            self.client.replication.list_source_relationship_statuses()
        )

        return {
            "cluster_name": cluster_name,
            "relationships": source_relationships,
        }

    def populate_replication_cache(self) -> None:
        """
        Populate replication paths cache from source cluster.
        Uses get_source_info() internally for DRY.
        """
        source_info = self.get_source_info()
        relationships = source_info.get("relationships", [])

        for repli in relationships:
            repli_path = repli.get("source_root_path")
            if not repli_path:
                logger.warning(
                    f"Skipping malformed relationship (missing source_root_path): {repli.get('id', 'unknown')}"
                )
                continue
            self.repli_paths[repli_path] = {
                "fid": repli.get("source_root_id"),
                "replid": repli.get("id"),
                "dst": repli.get("target_address"),
            }

    def preflight_check(self, basepath: str) -> None:
        """
        Verify source cluster is reachable and basepath exists before running create.

        Raises:
            ValueError: If cluster is unreachable or basepath does not exist
        """
        try:
            self.client.cluster.get_cluster_conf()
        except Exception as e:
            raise ValueError(f"Cannot reach source cluster: {e}") from e

        try:
            self.client.fs.get_file_attr(path=basepath)
        except Exception as e:
            raise ValueError(
                f"Basepath '{basepath}' not found on source cluster: {e}"
            ) from e

        logger.info(
            f"Pre-flight checks passed: source cluster reachable, basepath '{basepath}' exists"
        )

    def _transform_relationships_to_table_data(
        self,
        cluster_info: Dict[str, Any],
        cluster_type: str = "Source",
        state_filter: Optional[List[str]] = None,
    ) -> List[Dict[str, Any]]:
        """
        Transform relationship data to flat list of dicts for display/export.

        Args:
            cluster_info: Dict with cluster_name, relationships
            cluster_type: Either "Source" or "Destination"
            state_filter: Optional list of state strings to include (e.g. ["ERROR", "REPLICATING"])

        Returns:
            List of flattened relationship dicts ready for display/CSV
        """
        cluster_name = cluster_info.get("cluster_name", "Unknown")
        cluster_id = cluster_info.get("cluster_id", "")
        relationships = cluster_info.get("relationships", [])

        if state_filter:
            upper_states = {s.upper() for s in state_filter}
            relationships = [
                r for r in relationships if r.get("state", "").upper() in upper_states
            ]

        # Determine field names based on cluster type
        if cluster_type == "Source":
            remote_cluster_field = "target_cluster_name"
        else:  # Destination
            remote_cluster_field = "source_cluster_name"

        table_data = []
        for rel in sorted(relationships, key=lambda x: x.get("source_root_path", "")):
            row = {
                "cluster_type": cluster_type,
                "cluster_name": cluster_name,
                "cluster_id": cluster_id,
                "source_path": rel.get("source_root_path", ""),
                "target_path": rel.get("target_root_path", ""),
                "remote_cluster": rel.get(remote_cluster_field, ""),
                "state": rel.get("state", ""),
                "replication_id": rel.get("id", ""),
                "error": rel.get("error_from_last_job", ""),
                "recovery_point": rel.get("recovery_point", ""),
                "queued_snapshots": rel.get("queued_snapshot_count", "")
                if cluster_type == "Source"
                else "",
                "replication_mode": rel.get("replication_mode", "")
                if cluster_type == "Source"
                else "",
            }
            table_data.append(row)

        return table_data

    def _display_cluster_summary(
        self,
        cluster_info: Dict[str, Any],
        cluster_type: str = "Source",
        state_filter: Optional[List[str]] = None,
    ) -> None:
        """
        Display cluster replication relationships as table (uses qqutils).

        Args:
            cluster_info: Dict with cluster_name, cluster_id, relationships
            cluster_type: Either "Source" or "Destination"
            state_filter: Optional list of state strings to include
        """
        cluster_name = cluster_info.get("cluster_name", "Unknown")
        relationships = cluster_info.get("relationships", [])

        if state_filter:
            upper_states = {s.upper() for s in state_filter}
            relationships = [
                r for r in relationships if r.get("state", "").upper() in upper_states
            ]

        print("\n")
        print("=" * 130)
        print(f"{cluster_type} Cluster Summary:")
        print("=" * 130)
        print(f"Cluster Name: {cluster_name}")
        print(f"Total Relationships: {len(relationships)}")

        if not relationships:
            if state_filter:
                print(f"\nNo relationships found with state: {', '.join(state_filter)}")
            else:
                print(f"\nNo {cluster_type.lower()} replication relationships found.")
            return

        # Transform data and display using qqutils
        table_data = self._transform_relationships_to_table_data(
            cluster_info, cluster_type, state_filter=state_filter
        )

        # Select columns for display (compact view)
        display_columns = [
            "source_path",
            "target_path",
            "state",
            "remote_cluster",
            "replication_id",
        ]

        print("\n")
        display_table(table_data, columns=display_columns)

        # State summary
        state_counts = {}
        for rel in relationships:
            state = rel.get("state", "UNKNOWN")
            state_counts[state] = state_counts.get(state, 0) + 1

        print(f"\n{'State':<25} | {'Count':<10}")
        print("-" * 38)
        for state, count in sorted(state_counts.items()):
            print(f"{state:<25} | {count:<10}")

    def display_status(
        self,
        dst_info: Optional[Dict[str, Any]] = None,
        state_filter: Optional[List[str]] = None,
    ) -> None:
        """
        Screen display of relationships configured in both source and destination clusters.

        Args:
            dst_info: Optional destination cluster information
            state_filter: Optional list of state strings to include
        """
        source_info = self.get_source_info()
        self._display_cluster_summary(source_info, "Source", state_filter=state_filter)
        if dst_info:
            self._display_cluster_summary(
                dst_info, "Destination", state_filter=state_filter
            )

    def save_to_csv(
        self,
        filepath: str,
        source_info: Dict[str, Any],
        dst_info: Optional[Dict[str, Any]] = None,
        state_filter: Optional[List[str]] = None,
    ) -> None:
        """
        Save replication relationship data to CSV file (uses qqutils).

        Args:
            filepath: Path to CSV file to create
            source_info: Source cluster information
            dst_info: Optional destination cluster information
            state_filter: Optional list of state strings to include

        The CSV includes all relationship details without truncation.
        """
        # Transform source data
        csv_data = self._transform_relationships_to_table_data(
            source_info, "Source", state_filter=state_filter
        )

        # Add destination data if provided
        if dst_info:
            csv_data.extend(
                self._transform_relationships_to_table_data(
                    dst_info, "Destination", state_filter=state_filter
                )
            )

        # Use qqutils to export
        with open(filepath, "w") as csvfile:
            export_csv(csv_data, csvfile)

        logger.info(f"Saved replication data to CSV: {filepath}")

    def create_replications(
        self,
        basepath: str,
        dst: "TargetCluster",
        dst_path: str = "",
        filteri: Optional[List[str]] = None,
        filtere: Optional[List[str]] = None,
        depth: int = 1,
        dry_run: bool = False,
    ):
        """
        Creates replication relationships for directories below basepath.

        Args:
            basepath: Base path to search for directories
            dst: TargetCluster instance for destination
            dst_path: Path to prepend to destination (default: "")
            filteri: Include only directories containing these strings (default: None = all)
            filtere: Exclude directories containing these strings (default: None = none)
            depth: Number of directory levels below basepath to replicate (default: 1)
            dry_run: Preview mode — print what would be created without making API calls
        """
        if dst_path is None or dst_path == "/":
            dst_path = ""

        if depth > 2:
            logger.warning(
                f"--depth {depth} may generate a large number of relationships on large filesystems"
            )

        skipped_count = 0
        would_create = []

        first = True
        for entry in self.client.fs.tree_walk_preorder(path=basepath, max_depth=depth):
            if first:
                first = False
                logger.debug("Skipping base directory entry")
                continue
            if entry.get("type") == "FS_FILE_TYPE_DIRECTORY":
                path = entry.get("path")
                dir_name = path.rstrip("/").split("/")[-1]

                if filteri:
                    if not any(pattern in dir_name for pattern in filteri):
                        logger.info(
                            f"Skipping {path} - does not match include filter (patterns: {filteri})"
                        )
                        continue

                if filtere:
                    if any(pattern in dir_name for pattern in filtere):
                        logger.info(
                            f"Skipping {path} - matches exclude filter (patterns: {filtere})"
                        )
                        continue

                logger.info(f"Evaluating path {path}")
                if path not in self.repli_paths:
                    dst_address = dst.get_next_dst_ip()
                    dst_target_path = dst_path + path
                    if dry_run:
                        print(
                            f"  Would create: {path} → {dst_address}:{dst_target_path}"
                        )
                        would_create.append(path)
                    else:
                        logger.info(f"Using IP {dst_address} to set next replication")
                        replication_info = (
                            self.client.replication.create_source_relationship(
                                address=dst_address,
                                source_path=path,
                                target_path=dst_target_path,
                            )
                        )
                        replication_id = replication_info.get("id", "")
                        logger.info(
                            f"Created replication relationship {replication_id} with dst IP: {dst_address}, dst path: {dst_target_path}"
                        )
                        self.created_replications[path] = CreatedRelationship(
                            source_path=path,
                            dst_address=dst_address,
                            dst_path=dst_target_path,
                            replication_id=replication_id,
                        )
                else:
                    logger.info(
                        f"Replication already existing in folder {path}. Skipping."
                    )
                    skipped_count += 1

        if dry_run:
            print(
                f"\nDry run complete: {len(would_create)} relationship(s) would be created, "
                f"{skipped_count} already exist (would skip)"
            )
        else:
            created_count = len(self.created_replications)
            if created_count > 0:
                print("\nCreated Replications:")
                print(f"  {'Source Path':<40} {'Destination IP':<20} Destination Path")
                print("  " + "-" * 80)
                for rel in sorted(
                    self.created_replications.values(), key=lambda r: r.source_path
                ):
                    print(
                        f"  {rel.source_path:<40} {rel.dst_address:<20} {rel.dst_path}"
                    )
            print(
                f"\nCreated: {created_count}  |  Skipped (already existed): {skipped_count}"
            )

    def clean_replications(
        self, basepath, filteri=None, filtere=None, set_readonly=False
    ):
        """
        Delete source-side replication relationships under basepath

        Args:
            basepath: Base path to search for replications to delete
            filteri: Include only directories containing these strings (default: None = all)
            filtere: Exclude directories containing these strings (default: None = none)
            set_readonly: Set source paths to read-only (mode 0555) before deletion (default: False)

        Returns:
            Number of relationships deleted
        """
        deleted_count = 0
        for path, values in self.repli_paths.items():
            logger.info(f"Checking to delete {path} vs {basepath}")
            if path.startswith(basepath):
                # Get just the directory name, handling trailing slashes
                dir_name = path.rstrip("/").split("/")[-1]

                # Apply filters using substring matching (same logic as create)
                # If filteri is specified, only include directories containing any of the patterns
                if filteri:
                    if not any(pattern in dir_name for pattern in filteri):
                        logger.info(
                            f"Skipping {path} - does not match include filter (patterns: {filteri})"
                        )
                        continue

                # If filtere is specified, exclude directories containing any of the patterns
                if filtere:
                    if any(pattern in dir_name for pattern in filtere):
                        logger.info(
                            f"Skipping {path} - matches exclude filter (patterns: {filtere})"
                        )
                        continue

                # Set path to read-only before deleting replication if requested
                if set_readonly:
                    logger.info(f"Setting {path} to read-only (mode 0555)")
                    self.client.fs.set_file_attr(path=path, mode="0555")

                rid = values.get("replid")
                logger.info(f"Clearing replication with {rid} covering folder {path}")
                self.client.replication.delete_source_relationship(rid)
                deleted_count += 1
        return deleted_count


def validate_args(args, client_factory=None) -> Tuple[Optional[Any], Optional[Any]]:
    """
    Validate arguments and return (src_client, dst_client) tuple.

    Args:
        args: Parsed command-line arguments
        client_factory: Optional factory function for creating clients (for testing)

    Returns:
        Tuple of (src_client, dst_client) where either can be None

    Raises:
        ValueError: If required arguments for the action are missing
    """
    if client_factory is None:
        client_factory = Client

    # Validate filter arguments - filteri and filtere cannot be used together
    if hasattr(args, "filteri") and hasattr(args, "filtere"):
        if args.filteri and args.filtere:
            raise ValueError(
                "Cannot use both --filteri and --filtere together. "
                "Use --filteri to include only matching directories, "
                "or --filtere to exclude matching directories."
            )

    # First, validate required arguments based on action
    if args.action in ["summary", "create"]:
        if not args.src_host or not args.src_user:
            raise ValueError(
                f"--src_host and --src_user are required for '{args.action}' action"
            )

    if args.action == "create":
        if not args.dst_host or not args.dst_user:
            raise ValueError(
                "--dst_host and --dst_user are required for 'create' action"
            )

    if args.action == "clean":
        has_src = args.src_host and args.src_user
        has_dst = args.dst_host and args.dst_user
        if not has_src and not has_dst:
            raise ValueError("'clean' action requires at least src or dst credentials")

    if args.action == "accept":
        if not args.dst_host or not args.dst_user:
            raise ValueError(
                "--dst_host and --dst_user are required for 'accept' action"
            )

    # Now create clients after validation passes
    src_client = None
    dst_client = None

    if args.src_host and args.src_user:
        logger.info(f"Connecting to source cluster {args.src_host}...")
        src_creds = create_credentials(args.src_host, args.src_user, args.src_password)
        src_client = client_factory(src_creds)

    if args.dst_host and args.dst_user:
        logger.info(f"Connecting to destination cluster {args.dst_host}...")
        dst_creds = create_credentials(args.dst_host, args.dst_user, args.dst_password)
        dst_client = client_factory(dst_creds)

    return src_client, dst_client


def main():
    parser = argparse.ArgumentParser(
        description="Discover subfolders with max-depth1 and generate replication relationships",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Show replication summary for source cluster only
  python3 replication.py --src_host src.cluster.com --src_user admin --src_password pass --action summary

  # Show replication summary for BOTH source and destination clusters
  python3 replication.py --src_host src.cluster.com --src_user admin --src_password pass \\
    --dst_host dst.cluster.com --dst_user admin --dst_password pass --action summary

  # Create replications from /data to destination cluster
  python3 replication.py --src_host src.cluster.com --src_user admin --src_password pass \\
    --dst_host dst.cluster.com --dst_user admin --dst_password pass \\
    --basepath /data --action create

  # Accept pending replications on destination (no src required)
  python3 replication.py --dst_host dst.cluster.com --dst_user admin --action accept

  # Accept with confirmation prompt and password prompt
  python3 replication.py --dst_host dst.cluster.com --dst_user admin --action accept --confirm

  # Clean source-side relationships
  python3 replication.py --src_host src.cluster.com --src_user admin --basepath /data --action clean

  # Clean ENDED relationships on destination
  python3 replication.py --dst_host dst.cluster.com --dst_user admin --basepath /test-qwalk-parent --action clean

  # Clean BOTH source and destination at once
  python3 replication.py --src_host src.cluster.com --src_user admin \\
    --dst_host dst.cluster.com --dst_user admin --basepath /data --action clean
        """,
    )
    # Source cluster connection (required for summary/create/clean actions)
    parser.add_argument("--src_host", help="Source cluster hostname or IP address")
    parser.add_argument("--src_user", help="Source cluster username")
    parser.add_argument(
        "--src_password", help="Source cluster password (will prompt if not provided)"
    )
    parser.add_argument("--basepath", default="/", help="Directory path to search")
    parser.add_argument(
        "--csv",
        metavar="FILEPATH",
        help="Save summary to CSV file (e.g., --csv replication_status.csv). Full data, no truncation.",
    )
    parser.add_argument(
        "--csv-only",
        action="store_true",
        help="Export to CSV without displaying to screen (requires --csv)",
    )
    parser.add_argument(
        "--state",
        nargs="+",
        metavar="STATE",
        help="Filter summary to relationships in these states "
        "(e.g., --state ERROR or --state ERROR REPLICATING). "
        "Valid states: ESTABLISHED, REPLICATING, ERROR, DISCONNECTED. "
        "(summary action only)",
    )
    # Destination cluster connection (required for create/accept actions)
    parser.add_argument("--dst_host", help="Destination cluster hostname or IP address")
    parser.add_argument(
        "--dst",
        nargs="+",
        help="Specific destination FIP addresses to use. If omitted, uses all FIPs from network_id.",
    )
    parser.add_argument("--dst_user", help="Destination cluster username")
    parser.add_argument(
        "--dst_password",
        help="Destination cluster password (will prompt if not provided)",
    )
    parser.add_argument(
        "--dst_network",
        default="Default",
        help="Network name for floating IPs (default: 'Default'). "
        "View available networks in the Qumulo web UI under Cluster > Network.",
    )
    parser.add_argument(
        "--allow_non_empty_dir",
        action="store_true",
        help="Allow replication into non-empty directories (accept action only)",
    )
    parser.add_argument(
        "--confirm",
        action="store_true",
        help="Require confirmation before accepting replications (accept action only)",
    )
    # this arg will be used when creating replications to set the dst path
    parser.add_argument(
        "--dst_path",
        default="",
        help="path to prepend when creating relationships (default: empty, same as source path)",
    )
    # filter arguments for selective replication
    parser.add_argument(
        "--filteri",
        nargs="+",
        help="Include ONLY directories containing these strings (e.g., --filteri 'prod' 'staging'). Cannot be used with --filtere.",
    )
    parser.add_argument(
        "--filtere",
        nargs="+",
        help="Exclude directories containing these strings (e.g., --filtere 'test' 'temp'). Cannot be used with --filteri.",
    )
    parser.add_argument(
        "--set_readonly",
        action="store_true",
        help="Set source paths to read-only (mode 0555) before deleting replication relationships (clean action only)",
    )
    parser.add_argument(
        "--depth",
        type=int,
        default=1,
        help="Number of directory levels below --basepath to replicate (default: 1). "
        "With --basepath /data and --depth 1, replicates /data/proj-a but not /data/proj-a/sub. "
        "Warning: values above 2 may create many relationships on large filesystems. (create action only)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview what would be created without making any API changes (create action only). "
        "Prints the list of source → destination pairs that would be replicated.",
    )
    parser.add_argument(
        "--action", choices=["create", "clean", "summary", "accept"], default="summary"
    )

    args = parser.parse_args()

    # Validate arguments and create clients
    try:
        src_client, dst_client = validate_args(args)
    except ValueError as e:
        logger.error(str(e))
        sys.exit(1)
    except Exception as e:
        logger.error(f"Failed to connect to cluster: {e}")
        sys.exit(1)

    # Create managers once after validation
    rm = None
    target_cluster = None

    if src_client:
        rm = ReplicationManager(src_client)
        logger.info(f"Querying base path {args.basepath}")

    if dst_client:
        target_cluster = TargetCluster(
            dst_client,
            user_provided_ips=args.dst,
            network_name=args.dst_network,
        )

    if args.action == "summary":
        # Get source info
        source_info = rm.get_source_info()

        # Check if user wants destination info as well
        dst_info = None
        if target_cluster:
            try:
                dst_info = target_cluster.get_destination_info()
                logger.info(
                    f"Successfully retrieved destination cluster info: {dst_info.get('cluster_name')}"
                )
            except Exception as e:
                logger.error(
                    f"Failed to retrieve destination info: {type(e).__name__}: {e}"
                )
                dst_info = None

        # Display to screen (unless --csv-only)
        if not args.csv_only:
            rm.display_status(dst_info=dst_info, state_filter=args.state)

        # Save to CSV if requested
        if args.csv:
            rm.save_to_csv(args.csv, source_info, dst_info, state_filter=args.state)
            print(f"\nSaved to CSV: {args.csv}")
        elif args.csv_only:
            logger.error("--csv-only requires --csv FILEPATH")
            sys.exit(1)

    elif args.action == "create":
        try:
            rm.preflight_check(args.basepath)
        except ValueError as e:
            logger.error(str(e))
            sys.exit(1)
        rm.populate_replication_cache()
        rm.create_replications(
            args.basepath,
            dst=target_cluster,
            dst_path=args.dst_path,
            filteri=args.filteri,
            filtere=args.filtere,
            depth=args.depth,
            dry_run=args.dry_run,
        )

    elif args.action == "clean":
        # Clean source-side relationships if src_client provided
        if src_client:
            logger.info(f"Cleaning source-side relationships under {args.basepath}")
            # Populate repli_paths before cleaning (bug fix)
            rm.populate_replication_cache()
            src_deleted_count = rm.clean_replications(
                args.basepath,
                filteri=args.filteri,
                filtere=args.filtere,
                set_readonly=args.set_readonly,
            )
            logger.info(
                f"Deleted {src_deleted_count} replication relationship(s) on source"
            )

        # Clean destination-side ENDED relationships if dst_client provided
        if target_cluster:
            logger.info(
                f"Cleaning ENDED relationships on destination under {args.basepath}"
            )
            dst_deleted_count = target_cluster.clean_ended_replications(
                basepath=args.basepath
            )
            logger.info(
                f"Cleaned up {dst_deleted_count} ENDED replication relationship(s) on destination"
            )

    elif args.action == "accept":
        target_cluster.accept_pending_replications(
            allow_non_empty_dir=args.allow_non_empty_dir,
            confirm=args.confirm,
            filteri=args.filteri,
            filtere=args.filtere,
        )


if __name__ == "__main__":
    main()
