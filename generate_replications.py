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
from typing import List, Dict, Optional, Any
from qqbase import Client, Creds, RestClient, create_credentials

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

        # Initialize load tracking for all IPs
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
        self.dst_load = {}
        self.created_replications = {}

    def get_destination_info(self) -> Dict[str, Any]:
        """
        Retrieve destination cluster information including target relationships.

        Returns:
            Dict containing cluster_name, cluster_id, and list of relationships

        Raises:
            Exception: If cluster configuration or relationships cannot be retrieved
        """
        # Get cluster configuration
        cluster_conf = self.client.cluster.get_cluster_conf()
        cluster_name = cluster_conf.get("cluster_name", "Unknown")
        cluster_id = cluster_conf.get("cluster_id", "Unknown")

        # Warn if cluster info is missing
        if cluster_name == "Unknown" or cluster_id == "Unknown":
            logger.warning(
                "Cluster configuration returned default values - API may have issues"
            )

        # Get all target relationships
        target_relationships = self.client.replication.list_target_relationship_statuses()

        return {
            "cluster_name": cluster_name,
            "cluster_id": cluster_id,
            "relationships": target_relationships,
        }

    def get_replication_status(self) -> List[str]:
        """get paths currently covered by replication on cluster"""
        replications = self.client.replication.list_source_relationships()
        for repli in replications:
            qfile_id = repli.get("source_root_id")
            repli_path = self.client.fs.get_file_attr(qfile_id).get("path")
            dst_ip = repli.get("target_address")
            self.repli_paths.update(
                {
                    repli_path: {
                        "fid": qfile_id,
                        "replid": repli.get("id"),
                        "dst": dst_ip,
                    }
                }
            )
            self.dst_load[dst_ip] = 1 + self.dst_load.get(dst_ip, 0)

    def display_status(self, dst_info: Optional[Dict[str, Any]] = None) -> None:
        """
        Screen display of relationships configured in SRC cluster
        and optionally destination cluster if dst_info is provided.

        Args:
            dst_info: Optional destination cluster information
        """
        # Display source cluster information
        if not self.repli_paths:
            print("\nNo replication relationships found.")
        else:
            # Calculate column widths dynamically
            paths = list(self.repli_paths.keys())
            path_width = max(len(p) for p in paths)

            # Header
            header = f"{'Path':<{path_width}} | {'Target IP':<15} | {'File ID':<32} | {'Replication ID':<36}"
            print("\nSource Cluster Replication Summary:")
            print("=" * len(header))
            print(header)
            print("-" * len(header))

            # Rows
            for path, details in sorted(self.repli_paths.items()):
                dst = details.get("dst", "N/A")
                fid = details.get("fid", "N/A")
                replid = details.get("replid", "N/A")
                print(f"{path:<{path_width}} | {dst:<15} | {fid:<32} | {replid:<36}")

            # Load summary
            if self.dst_load:
                print(f"\n{'Target IP':<15} | {'Replication Count':<20}")
                print("-" * 38)
                for ip, count in sorted(self.dst_load.items()):
                    print(f"{ip:<15} | {count:<20}")

        # Display destination cluster information if provided
        if dst_info:
            self._display_destination_status(dst_info)

    def _display_destination_status(self, dst_info: Dict[str, Any]) -> None:
        """Display destination cluster information"""
        cluster_name = dst_info.get("cluster_name", "Unknown")
        cluster_id = dst_info.get("cluster_id", "Unknown")
        relationships = dst_info.get("relationships", [])

        print("\n")
        print("=" * 100)
        print("Destination Cluster Summary:")
        print("=" * 100)
        print(f"Cluster Name: {cluster_name}")
        print(f"Cluster ID: {cluster_id}")
        print(f"Total Target Relationships: {len(relationships)}")

        if not relationships:
            print("\nNo target replication relationships found on destination cluster.")
            return

        # Calculate column widths
        if relationships:
            source_paths = [r.get("source_root_path", "N/A") for r in relationships]
            target_paths = [r.get("target_root_path", "N/A") for r in relationships]
            states = [r.get("state", "N/A") for r in relationships]
            source_clusters = [r.get("source_cluster_name", "N/A") for r in relationships]

            source_path_width = max(len(p) for p in source_paths) if source_paths else 20
            target_path_width = max(len(p) for p in target_paths) if target_paths else 20
            state_width = max(len(s) for s in states) if states else 15
            cluster_width = max(len(c) for c in source_clusters) if source_clusters else 15

            # Header
            print("\n")
            header = f"{'Source Path':<{source_path_width}} | {'Target Path':<{target_path_width}} | {'State':<{state_width}} | {'Source Cluster':<{cluster_width}} | {'Replication ID':<36}"
            print(header)
            print("-" * len(header))

            # Rows
            for rel in sorted(relationships, key=lambda x: x.get("target_root_path", "")):
                source_path = rel.get("source_root_path", "N/A")
                target_path = rel.get("target_root_path", "N/A")
                state = rel.get("state", "N/A")
                source_cluster = rel.get("source_cluster_name", "N/A")
                rel_id = rel.get("id", "N/A")
                print(f"{source_path:<{source_path_width}} | {target_path:<{target_path_width}} | {state:<{state_width}} | {source_cluster:<{cluster_width}} | {rel_id:<36}")

        # State summary
        if relationships:
            state_counts = {}
            for rel in relationships:
                state = rel.get("state", "UNKNOWN")
                state_counts[state] = state_counts.get(state, 0) + 1

            print(f"\n{'State':<25} | {'Count':<10}")
            print("-" * 38)
            for state, count in sorted(state_counts.items()):
                print(f"{state:<25} | {count:<10}")

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
        for path, values in self.repli_paths.items():
            if path.startswith(basepath):
                rid = values.get("replid")
                logger.info(f"Clearing replication with {rid} covering folder {path}")
                self.client.replication.delete_source_relationship(rid)


def main():
    parser = argparse.ArgumentParser(
        description="Discover subfolders with max-depth1 and generate replication relationships",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Show replication summary for source cluster only
  python3 generate_replications.py --src_host src.cluster.com --src_user admin --src_password pass --action summary

  # Show replication summary for BOTH source and destination clusters
  python3 generate_replications.py --src_host src.cluster.com --src_user admin --src_password pass \\
    --dst_host dst.cluster.com --dst_user admin --dst_password pass --action summary

  # Create replications from /data to destination cluster
  python3 generate_replications.py --src_host src.cluster.com --src_user admin --src_password pass \\
    --dst_host dst.cluster.com --dst_user admin --dst_password pass \\
    --basepath /data --action create

  # Accept pending replications on destination (no src required)
  python3 generate_replications.py --dst_host dst.cluster.com --dst_user admin --action accept

  # Accept with confirmation prompt and password prompt
  python3 generate_replications.py --dst_host dst.cluster.com --dst_user admin --action accept --confirm
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

    # Validate action-specific required arguments
    if args.action in ["summary", "create", "clean"]:
        if not args.src_host or not args.src_user:
            logger.error(
                f"--src_host and --src_user are required for '{args.action}' action"
            )
            sys.exit(1)

        # Connect to source cluster with password prompt if needed
        logger.info(f"Connecting to {args.src_host}...")
        src_creds = create_credentials(args.src_host, args.src_user, args.src_password)
        src_client = Client(src_creds)
        logger.info(f"Querying base path {args.basepath}")

        rm = ReplicationManager(src_client)
        # preload config so faster when querying
        replication_paths = rm.get_replication_status()

    if args.action == "summary":
        # Check if user wants destination info as well
        dst_info = None
        if args.dst_host and args.dst_user:
            try:
                logger.info(f"Connecting to destination cluster {args.dst_host}...")
                dst_creds = create_credentials(args.dst_host, args.dst_user, args.dst_password)
                dst_client = Client(dst_creds)

                # Create a temporary ReplicationManager for destination to get info
                dst_rm = ReplicationManager(dst_client)
                dst_info = dst_rm.get_destination_info()
                logger.info(f"Successfully retrieved destination cluster info: {dst_info.get('cluster_name')}")
            except Exception as e:
                logger.error(
                    f"Failed to connect to destination cluster: {type(e).__name__}: {e}"
                )
                logger.info("Displaying source cluster information only.")
                dst_info = None

        rm.display_status(dst_info=dst_info)

    elif args.action == "create":
        # Validate required destination parameters
        if not args.dst_host:
            logger.error("--dst_host is required for 'create' action")
            sys.exit(1)
        if not args.dst_user:
            logger.error("--dst_user is required for 'create' action")
            sys.exit(1)

        logger.info(f"Connecting to destination cluster {args.dst_host}...")
        dst_creds = create_credentials(args.dst_host, args.dst_user, args.dst_password)
        dst_client = Client(dst_creds)

        # Initialize TargetCluster with optional user-provided IPs and network_id
        target_cluster = TargetCluster(
            dst_client, user_provided_ips=args.dst, network_id=int(args.dst_network_id)
        )

        replications = rm.create_replications(
            args.basepath,
            dst=target_cluster,
        )

    elif args.action == "clean":
        rm.clean_replications(args.basepath)

    elif args.action == "accept":
        # Validate required destination parameters
        if not args.dst_host:
            logger.error("--dst_host is required for 'accept' action")
            sys.exit(1)
        if not args.dst_user:
            logger.error("--dst_user is required for 'accept' action")
            sys.exit(1)

        logger.info(f"Connecting to destination cluster {args.dst_host}...")
        dst_creds = create_credentials(args.dst_host, args.dst_user, args.dst_password)
        dst_client = Client(dst_creds)

        # Initialize TargetCluster
        target_cluster = TargetCluster(
            dst_client, user_provided_ips=args.dst, network_id=int(args.dst_network_id)
        )

        target_cluster.accept_pending_replications(
            allow_non_empty_dir=args.allow_non_empty_dir, confirm=args.confirm
        )


if __name__ == "__main__":
    main()
