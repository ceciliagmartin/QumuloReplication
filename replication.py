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
import csv
import logging
import sys
from typing import List, Dict, Optional, Any, Tuple
from qqbase import Client, RestClient, create_credentials

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)


class TargetCluster:
    def __init__(
        self,
        client: RestClient,
        user_provided_ips: Optional[List[str]] = None,
        network_id: Optional[int] = 1,
    ) -> None:
        self.client = client.rc
        self.dst_load = {}
        self.available_ips = []
        self.network_id = network_id

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
        """Retrieve all floating IPs from destination cluster"""
        fip_data = self.client.network.get_floating_ip_allocation()
        # Find the network matching our network_id
        matching_network = None
        for network in fip_data:
            if network.get("id") == self.network_id:
                matching_network = network
                break

        if not matching_network:
            raise ValueError(f"Network ID {self.network_id} not found in cluster")

        floating_addresses = matching_network.get("floating_addresses", [])
        if not floating_addresses:
            raise ValueError(
                f"Network ID {self.network_id} has no Floating IPs available"
            )

        return floating_addresses

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

        logger.info(
            f"Deleted {deleted_count} of {len(ended_relationships)} ENDED relationship(s)"
        )
        return deleted_count

    def accept_pending_replications(
        self, allow_non_empty_dir: bool = False, confirm: bool = False
    ) -> List[Dict]:
        """
        Accept all pending replication relationships on destination cluster

        Args:
            allow_non_empty_dir: Allow replication into non-empty directories (default: False)
            confirm: Require user confirmation before accepting (default: False)

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
            qfile_id = repli.get("source_root_id")
            dst_ip = repli.get("target_address")

            self.repli_paths[repli_path] = {
                "fid": qfile_id,
                "replid": repli.get("id"),
                "dst": dst_ip,
            }

    @staticmethod
    def _truncate_string(s: str, max_len: int) -> str:
        """
        Truncate string to max length with '...' suffix if needed.

        Args:
            s: String to truncate
            max_len: Maximum length including '...'

        Returns:
            Truncated string
        """
        if len(s) <= max_len:
            return s
        return s[: max_len - 3] + "..."

    def _display_cluster_summary_card(
        self, cluster_info: Dict[str, Any], cluster_type: str = "Source"
    ) -> None:
        """
        Display cluster information in card/block format (easier to read, good for health checks).

        Args:
            cluster_info: Dict with cluster_name, cluster_id, relationships
            cluster_type: Either "Source" or "Destination"
        """
        cluster_name = cluster_info.get("cluster_name", "Unknown")
        cluster_id = cluster_info.get("cluster_id", "Unknown")
        relationships = cluster_info.get("relationships", [])

        # Determine field names based on cluster type
        if cluster_type == "Source":
            remote_cluster_field = "target_cluster_name"
            remote_label = "Target"
        else:  # Destination
            remote_cluster_field = "source_cluster_name"
            remote_label = "Source"

        print("\n")
        print("=" * 100)
        print(f"{cluster_type} Cluster Summary:")
        print("=" * 100)
        print(f"Cluster: {cluster_name} ({cluster_id[:16]}...)")
        print(f"Total Relationships: {len(relationships)}")

        if not relationships:
            print(f"\nNo {cluster_type.lower()} replication relationships found.")
            return

        print()

        # Display each relationship as a card
        for rel in sorted(relationships, key=lambda x: x.get("source_root_path", "")):
            source_path = rel.get("source_root_path", "N/A")
            target_path = rel.get("target_root_path", "N/A")
            remote_cluster = rel.get(remote_cluster_field, "N/A")
            state = rel.get("state", "UNKNOWN")
            rel_id = rel.get("id", "N/A")
            error = rel.get("error_from_last_job", "")
            recovery_point = rel.get("recovery_point", "")

            # State indicator
            if state == "ESTABLISHED" and not error:
                state_icon = "✓"
            elif state in ["REPLICATING", "CREATING"]:
                state_icon = "⟳"
            elif error or state in ["DISCONNECTED", "ERROR"]:
                state_icon = "✗"
            else:
                state_icon = "⚠"

            # Display card
            print(f"▸ {source_path} → {remote_cluster}:{target_path}")
            print(f"  State: {state} {state_icon}")
            print(f"  ID: {rel_id[:16]}...")

            # Show error prominently if present
            if error:
                print(f"  ⚠ Error: {error[:80]}{'...' if len(error) > 80 else ''}")

            # Show recovery point if available
            if recovery_point:
                # Format timestamp: 2025-10-21T06:40:30.804453983Z -> 2025-10-21 06:40:30
                timestamp = recovery_point.split("T")
                if len(timestamp) == 2:
                    date = timestamp[0]
                    time = timestamp[1].split(".")[0]
                    print(f"  Recovery Point: {date} {time}")

            # Show queued snapshots for source if available
            if cluster_type == "Source":
                queued = rel.get("queued_snapshot_count")
                if queued is not None and queued > 0:
                    print(f"  Queued Snapshots: {queued}")

                mode = rel.get("replication_mode", "")
                if "CONTINUOUS" in mode:
                    print(f"  Mode: Continuous")
                elif "SNAPSHOT" in mode:
                    print(f"  Mode: Snapshot Policy")

            print()  # Blank line between cards

        # State summary
        state_counts = {}
        for rel in relationships:
            state = rel.get("state", "UNKNOWN")
            state_counts[state] = state_counts.get(state, 0) + 1

        print("State Summary:")
        for state, count in sorted(state_counts.items()):
            icon = (
                "✓"
                if state == "ESTABLISHED"
                else ("⟳" if state == "REPLICATING" else "⚠")
            )
            print(f"  {icon} {state}: {count}")

    def _display_cluster_summary(
        self, cluster_info: Dict[str, Any], cluster_type: str = "Source"
    ) -> None:
        """
        Unified display for both source and destination cluster information (fixed-width table).

        Args:
            cluster_info: Dict with cluster_name, cluster_id, relationships
            cluster_type: Either "Source" or "Destination"
        """
        # Fixed column widths for predictable table size (~130 chars total)
        SOURCE_PATH_WIDTH = 35
        TARGET_PATH_WIDTH = 35
        STATE_WIDTH = 15
        CLUSTER_WIDTH = 20
        ID_WIDTH = 16  # Truncated UUID

        cluster_name = cluster_info.get("cluster_name", "Unknown")
        relationships = cluster_info.get("relationships", [])

        # Determine field names based on cluster type
        if cluster_type == "Source":
            local_path_field = "source_root_path"
            local_label = "Source"
            remote_cluster_field = "target_cluster_name"
            remote_path_field = "target_root_path"
            remote_label = "Target"
        else:  # Destination
            local_path_field = "target_root_path"
            local_label = "Target"
            remote_cluster_field = "source_cluster_name"
            remote_path_field = "source_root_path"
            remote_label = "Source"

        print("\n")
        print("=" * 130)
        print(f"{cluster_type} Cluster Summary:")
        print("=" * 130)
        print(f"Cluster Name: {cluster_name}")
        print(f"Total Relationships: {len(relationships)}")

        if not relationships:
            print(f"\nNo {cluster_type.lower()} replication relationships found.")
            return

        # Header
        print("\n")
        header = f"{f'{local_label} Path':<{SOURCE_PATH_WIDTH}} | {f'{remote_label} Path':<{TARGET_PATH_WIDTH}} | {'State':<{STATE_WIDTH}} | {f'{remote_label} Cluster':<{CLUSTER_WIDTH}} | {'ID (truncated)':<{ID_WIDTH}}"
        print(header)
        print("-" * len(header))

        # Rows with truncation
        for rel in sorted(relationships, key=lambda x: x.get("source_root_path", "")):
            local_path = self._truncate_string(
                rel.get(local_path_field, "N/A"), SOURCE_PATH_WIDTH
            )
            remote_path = self._truncate_string(
                rel.get(remote_path_field, "N/A"), TARGET_PATH_WIDTH
            )
            state = self._truncate_string(rel.get("state", "N/A"), STATE_WIDTH)
            remote_cluster = self._truncate_string(
                rel.get(remote_cluster_field, "N/A"), CLUSTER_WIDTH
            )
            rel_id = rel.get("id", "N/A")[:ID_WIDTH]  # Truncate UUID

            print(
                f"{local_path:<{SOURCE_PATH_WIDTH}} | {remote_path:<{TARGET_PATH_WIDTH}} | {state:<{STATE_WIDTH}} | {remote_cluster:<{CLUSTER_WIDTH}} | {rel_id:<{ID_WIDTH}}"
            )

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
        self, dst_info: Optional[Dict[str, Any]] = None, format: str = "table"
    ) -> None:
        """
        Screen display of relationships configured in both source and destination clusters.

        Args:
            dst_info: Optional destination cluster information
            format: Display format - "table" (default) or "card"
        """
        # Get source cluster information
        source_info = self.get_source_info()

        # Choose display method based on format
        if format == "card":
            self._display_cluster_summary_card(source_info, "Source")
            if dst_info:
                self._display_cluster_summary_card(dst_info, "Destination")
        else:  # Default to table format
            self._display_cluster_summary(source_info, "Source")
            if dst_info:
                self._display_cluster_summary(dst_info, "Destination")

    def save_to_csv(
        self,
        filepath: str,
        source_info: Dict[str, Any],
        dst_info: Optional[Dict[str, Any]] = None,
    ) -> None:
        """
        Save replication relationship data to CSV file.

        Args:
            filepath: Path to CSV file to create
            source_info: Source cluster information
            dst_info: Optional destination cluster information

        The CSV includes all relationship details without truncation.
        """
        fieldnames = [
            "cluster_type",
            "cluster_name",
            "cluster_id",
            "source_path",
            "target_path",
            "remote_cluster",
            "state",
            "replication_id",
            "error",
            "recovery_point",
            "queued_snapshots",
            "replication_mode",
        ]

        with open(filepath, "w", newline="") as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()

            # Write source relationships
            source_relationships = source_info.get("relationships", [])
            for rel in source_relationships:
                row = {
                    "cluster_type": "Source",
                    "cluster_name": source_info.get("cluster_name", ""),
                    "cluster_id": source_info.get("cluster_id", ""),
                    "source_path": rel.get("source_root_path", ""),
                    "target_path": rel.get("target_root_path", ""),
                    "remote_cluster": rel.get("target_cluster_name", ""),
                    "state": rel.get("state", ""),
                    "replication_id": rel.get("id", ""),
                    "error": rel.get("error_from_last_job", ""),
                    "recovery_point": rel.get("recovery_point", ""),
                    "queued_snapshots": rel.get("queued_snapshot_count", ""),
                    "replication_mode": rel.get("replication_mode", ""),
                }
                writer.writerow(row)

            # Write destination relationships if provided
            if dst_info:
                dst_relationships = dst_info.get("relationships", [])
                for rel in dst_relationships:
                    row = {
                        "cluster_type": "Destination",
                        "cluster_name": dst_info.get("cluster_name", ""),
                        "cluster_id": dst_info.get("cluster_id", ""),
                        "source_path": rel.get("source_root_path", ""),
                        "target_path": rel.get("target_root_path", ""),
                        "remote_cluster": rel.get("source_cluster_name", ""),
                        "state": rel.get("state", ""),
                        "replication_id": rel.get("id", ""),
                        "error": rel.get("error_from_last_job", ""),
                        "recovery_point": rel.get("recovery_point", ""),
                        "queued_snapshots": "",  # Not available on target side
                        "replication_mode": "",  # Not available on target side
                    }
                    writer.writerow(row)

        logger.info(f"Saved replication data to CSV: {filepath}")

    def create_replications(self, basepath: str, dst: "TargetCluster"):
        """
        Creates replication one level deep from basepath
        returns dict with replications created for tallying
        """
        first = True
        for entry in self.client.fs.tree_walk_preorder(path=basepath, max_depth=1):
            if first:
                first = False
                logger.debug("Skipping base directory entry")
                continue
            if entry.get("type") == "FS_FILE_TYPE_DIRECTORY":
                path = entry.get("path")
                logger.info(f"Evaluating path {path}")
                if path not in self.repli_paths:
                    dst_address = dst.get_next_dst_ip()
                    logger.info(f"Using IP {dst_address} to set next replication")
                    replication_info = (
                        self.client.replication.create_source_relationship(
                            address=dst_address, source_path=path, target_path=path
                        )
                    )
                    replication_id = replication_info.get("id", "")

                    logger.info(
                        f"Created replication relationship {replication_id} on with dst IP: {dst_address} \n path {path}"
                    )
                    self.created_replications.update(replication_info)
                else:
                    logger.info(
                        f"Replication already existing in folder {path}. Skipping."
                    )

    def clean_replications(self, basepath):
        """Delete source-side replication relationships under basepath"""
        for path, values in self.repli_paths.items():
            logger.info(f"Checking to delete {path} vs {basepath}")
            if path.startswith(basepath):
                rid = values.get("replid")
                logger.info(f"Clearing replication with {rid} covering folder {path}")
                self.client.replication.delete_source_relationship(rid)


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

    # First, validate required arguments based on action
    if args.action in ["summary", "create"]:
        if not args.src_host or not args.src_user:
            raise ValueError(
                f"--src_host and --src_user are required for '{args.action}' action"
            )

    if args.action == "create":
        if not args.dst_host or not args.dst_user:
            raise ValueError(
                f"--dst_host and --dst_user are required for 'create' action"
            )

    if args.action == "clean":
        has_src = args.src_host and args.src_user
        has_dst = args.dst_host and args.dst_user
        if not has_src and not has_dst:
            raise ValueError(
                "'clean' action requires at least src or dst credentials"
            )

    if args.action == "accept":
        if not args.dst_host or not args.dst_user:
            raise ValueError(
                f"--dst_host and --dst_user are required for 'accept' action"
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
    # Search parameters
    parser.add_argument("--basepath", default="/", help="Directory path to search")
    parser.add_argument(
        "--action", choices=["create", "clean", "summary", "accept"], default="summary"
    )
    parser.add_argument(
        "--format",
        choices=["table", "card"],
        default="table",
        help="Display format for summary: 'table' (default, compact columns) or 'card' (readable blocks, good for health checks)",
    )
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
        "--dst_network_id", default="1", help="Network ID for floating IPs (default: 1)"
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

    args = parser.parse_args()

    # Validate arguments and create clients
    try:
        src_client, dst_client = validate_args(args)
    except ValueError as e:
        logger.error(str(e))
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
            network_id=int(args.dst_network_id),
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
            rm.display_status(dst_info=dst_info, format=args.format)

        # Save to CSV if requested
        if args.csv:
            rm.save_to_csv(args.csv, source_info, dst_info)
            print(f"\nSaved to CSV: {args.csv}")
        elif args.csv_only:
            logger.error("--csv-only requires --csv FILEPATH")
            sys.exit(1)

    elif args.action == "create":
        # Populate repli_paths to avoid creating duplicate replications
        rm.populate_replication_cache()
        rm.create_replications(
            args.basepath,
            dst=target_cluster,
        )

    elif args.action == "clean":
        # Clean source-side relationships if src_client provided
        if src_client:
            logger.info(f"Cleaning source-side relationships under {args.basepath}")
            # Populate repli_paths before cleaning (bug fix)
            rm.populate_replication_cache()
            rm.clean_replications(args.basepath)

        # Clean destination-side ENDED relationships if dst_client provided
        if target_cluster:
            logger.info(
                f"Cleaning ENDED relationships on destination under {args.basepath}"
            )
            deleted_count = target_cluster.clean_ended_replications(
                basepath=args.basepath
            )
            print(
                f"\n✓ Cleaned up {deleted_count} ENDED replication relationship(s) on destination"
            )

    elif args.action == "accept":
        target_cluster.accept_pending_replications(
            allow_non_empty_dir=args.allow_non_empty_dir, confirm=args.confirm
        )


if __name__ == "__main__":
    main()
