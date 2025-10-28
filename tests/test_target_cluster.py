#!/usr/bin/env python3
"""
Tests for TargetCluster class using dependency injection (no mocking)
"""

import pytest
import logging
from typing import List, Dict, Any
from replication import TargetCluster

# Configure logging
logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)


# Fake RestClient implementation for testing (no mocking)
class FakeNetworkAPI:
    """Fake network API that simulates Qumulo network.list_network_status_v2()"""

    def __init__(self, networks: List[Dict[str, Any]]):
        """
        Args:
            networks: List of network configs, e.g.:
                [{"name": "Default", "floating_addresses": ["10.1.1.20"]}]
        """
        self.networks = networks

    def list_network_status_v2(self) -> List[Dict[str, Any]]:
        """Simulate the API response for network status v2

        Returns: [{'node_id': 1, 'node_name': 'node-1', 'network_statuses': [...]}]
        """
        return [
            {
                "node_id": 1,
                "node_name": "fake-node-1",
                "network_statuses": self.networks,
            }
        ]


class FakeReplicationAPI:
    """Fake replication API"""

    def __init__(self, target_relationships: List[Dict[str, Any]]):
        self.target_relationships = target_relationships

    def list_target_relationship_statuses(self) -> List[Dict[str, Any]]:
        """Simulate listing target relationships"""
        return self.target_relationships


class FakeClusterAPI:
    """Fake cluster API for getting cluster info"""

    def __init__(self, cluster_name: str = "test-cluster"):
        self.cluster_name = cluster_name

    def get_cluster_conf(self) -> Dict[str, str]:
        """Simulate getting cluster configuration"""
        return {
            "cluster_name": self.cluster_name,
        }


class FakeRestClient:
    """Fake RestClient that mimics qumulo.rest_client.RestClient"""

    def __init__(
        self,
        networks: List[Dict[str, Any]],
        replication_api: FakeReplicationAPI = None,
        cluster_api: FakeClusterAPI = None,
    ):
        self.network = FakeNetworkAPI(networks)
        self.replication = replication_api or FakeReplicationAPI([])
        self.cluster = cluster_api or FakeClusterAPI()


class FakeClient:
    """Fake Client that mimics the Client class from qqbase"""

    def __init__(
        self,
        networks: List[Dict[str, Any]],
        replication_api: FakeReplicationAPI = None,
        cluster_api: FakeClusterAPI = None,
    ):
        self.rc = FakeRestClient(networks, replication_api, cluster_api)


# Test cases
class TestTargetClusterInitialization:
    """Test TargetCluster initialization with various scenarios"""

    def test_init_with_multiple_cluster_fips(self):
        """Test initialization using all cluster FIPs (no user-provided IPs)"""
        cluster_ips = ["10.1.1.20", "10.1.1.21", "10.1.1.22"]
        fake_client = FakeClient(
            [{"name": "Default", "floating_addresses": cluster_ips}]
        )

        target = TargetCluster(fake_client, network_name="Default")

        assert target.available_ips == cluster_ips
        assert all(target.dst_load[ip] == 0 for ip in cluster_ips)
        assert len(target.dst_load) == 3

    def test_init_with_valid_user_provided_ips(self):
        """Test initialization with user-provided IPs that exist in cluster"""
        cluster_ips = ["10.1.1.20", "10.1.1.21", "10.1.1.22", "10.1.1.23"]
        user_ips = ["10.1.1.21", "10.1.1.22"]
        fake_client = FakeClient(
            [{"name": "Default", "floating_addresses": cluster_ips}]
        )

        target = TargetCluster(
            fake_client, user_provided_ips=user_ips, network_name="Default"
        )

        assert target.available_ips == user_ips
        assert all(target.dst_load[ip] == 0 for ip in user_ips)
        assert len(target.dst_load) == 2

    def test_init_with_single_ip_warns(self, caplog):
        """Test initialization with single IP logs warning"""
        cluster_ips = ["10.1.1.20", "10.1.1.21"]
        user_ips = ["10.1.1.20"]
        fake_client = FakeClient(
            [{"name": "Default", "floating_addresses": cluster_ips}]
        )

        with caplog.at_level(logging.WARNING):
            target = TargetCluster(
                fake_client, user_provided_ips=user_ips, network_name="Default"
            )

        assert target.available_ips == user_ips
        assert "may be unbalanced" in caplog.text

    def test_init_with_invalid_user_ips_fails_fast(self):
        """Test that invalid user IPs cause immediate failure"""
        cluster_ips = ["10.1.1.20", "10.1.1.21"]
        user_ips = ["10.1.1.99", "10.1.1.100"]
        fake_client = FakeClient(
            [{"name": "Default", "floating_addresses": cluster_ips}]
        )

        with pytest.raises(ValueError) as exc_info:
            TargetCluster(
                fake_client, user_provided_ips=user_ips, network_name="Default"
            )

        assert "Invalid destination IPs" in str(exc_info.value)
        assert "10.1.1.99" in str(exc_info.value)
        assert "10.1.1.100" in str(exc_info.value)

    def test_init_with_partial_invalid_ips_fails_fast(self):
        """Test that even one invalid IP causes failure"""
        cluster_ips = ["10.1.1.20", "10.1.1.21"]
        user_ips = ["10.1.1.20", "10.1.1.99"]  # One valid, one invalid
        fake_client = FakeClient(
            [{"name": "Default", "floating_addresses": cluster_ips}]
        )

        with pytest.raises(ValueError) as exc_info:
            TargetCluster(
                fake_client, user_provided_ips=user_ips, network_name="Default"
            )

        assert "10.1.1.99" in str(exc_info.value)

    def test_init_with_empty_cluster_fips(self):
        """Test initialization when cluster has no FIPs"""
        fake_client = FakeClient([{"name": "Default", "floating_addresses": []}])

        with pytest.raises(ValueError) as exc_info:
            TargetCluster(fake_client, network_name="Default")

        assert "has no Floating IPs available" in str(exc_info.value)

    def test_init_with_invalid_network_name(self):
        """Test that invalid network_name causes failure"""
        fake_client = FakeClient(
            [{"name": "Default", "floating_addresses": ["10.1.1.20", "10.1.1.21"]}]
        )

        with pytest.raises(ValueError) as exc_info:
            TargetCluster(fake_client, network_name="nonexistent")

        assert "nonexistent" in str(exc_info.value).lower()


class TestTargetClusterLoadBalancing:
    """Test least-used load balancing strategy"""

    def test_get_next_dst_ip_round_robin_on_equal_load(self):
        """Test that with equal load, IPs are selected fairly"""
        cluster_ips = ["10.1.1.20", "10.1.1.21", "10.1.1.22"]
        fake_client = FakeClient(
            [{"name": "Default", "floating_addresses": cluster_ips}]
        )
        target = TargetCluster(fake_client, network_name="Default")

        # Get 9 IPs (3 rounds)
        selected_ips = [target.get_next_dst_ip() for _ in range(9)]

        # Each IP should be selected 3 times
        for ip in cluster_ips:
            assert selected_ips.count(ip) == 3

        # Verify final load
        assert all(target.dst_load[ip] == 3 for ip in cluster_ips)

    def test_get_next_dst_ip_selects_least_used(self):
        """Test that the IP with minimum load is always selected"""
        cluster_ips = ["10.1.1.20", "10.1.1.21", "10.1.1.22"]
        fake_client = FakeClient(
            [{"name": "Default", "floating_addresses": cluster_ips}]
        )
        target = TargetCluster(fake_client, network_name="Default")

        # Manually set unequal load
        target.dst_load["10.1.1.20"] = 5
        target.dst_load["10.1.1.21"] = 2
        target.dst_load["10.1.1.22"] = 8

        # Should select 10.1.1.21 (load=2)
        next_ip = target.get_next_dst_ip()
        assert next_ip == "10.1.1.21"
        assert target.dst_load["10.1.1.21"] == 3

    def test_get_next_dst_ip_with_empty_load_fails(self):
        """Test that get_next_dst_ip fails when no IPs available"""
        fake_client = FakeClient([{"name": "Default", "floating_addresses": []}])

        with pytest.raises(ValueError) as exc_info:
            target = TargetCluster(fake_client, network_name="Default")

        assert "has no Floating IPs available" in str(exc_info.value)

    def test_load_balancing_with_initial_load(self):
        """Test that initial load state is respected"""
        cluster_ips = ["10.1.1.20", "10.1.1.21"]
        fake_client = FakeClient(
            [{"name": "Default", "floating_addresses": cluster_ips}]
        )
        target = TargetCluster(fake_client, network_name="Default")

        # Simulate existing replications
        target.dst_load["10.1.1.20"] = 10
        target.dst_load["10.1.1.21"] = 3

        # Next 7 calls should go to 10.1.1.21 to balance
        for i in range(7):
            next_ip = target.get_next_dst_ip()
            assert next_ip == "10.1.1.21"

        # Now both should have load=10
        assert target.dst_load["10.1.1.20"] == 10
        assert target.dst_load["10.1.1.21"] == 10

        # Next call can be either (both have equal load)
        next_ip = target.get_next_dst_ip()
        assert next_ip in cluster_ips


class TestTargetClusterFIPRetrieval:
    """Test FIP retrieval from cluster"""

    def test_get_dst_ips_returns_all_fips(self):
        """Test that get_dst_ips retrieves all cluster FIPs"""
        cluster_ips = ["10.1.1.20", "10.1.1.21", "10.1.1.22", "10.1.1.23"]
        fake_client = FakeClient(
            [{"name": "Default", "floating_addresses": cluster_ips}]
        )
        target = TargetCluster(fake_client, network_name="Default")

        retrieved_ips = target.get_dst_ips()

        assert set(retrieved_ips) == set(cluster_ips)
        assert len(retrieved_ips) == 4

    def test_get_dst_ips_handles_empty_response(self):
        """Test get_dst_ips when cluster has no FIPs"""
        fake_client = FakeClient([{"name": "Default", "floating_addresses": []}])

        with pytest.raises(ValueError) as exc_info:
            TargetCluster(fake_client, network_name="Default")

        assert "has no Floating IPs available" in str(exc_info.value)

    def test_get_dst_ips_from_multiple_networks(self):
        """Test that get_dst_ips retrieves FIPs from correct network"""
        fake_client = FakeClient(
            [
                {"name": "Default", "floating_addresses": ["10.1.1.20", "10.1.1.21"]},
                {
                    "name": "production",
                    "floating_addresses": ["10.2.2.30", "10.2.2.31", "10.2.2.32"],
                },
            ]
        )
        target = TargetCluster(fake_client, network_name="production")

        retrieved_ips = target.get_dst_ips()

        assert set(retrieved_ips) == {"10.2.2.30", "10.2.2.31", "10.2.2.32"}
        assert len(retrieved_ips) == 3


class TestTargetClusterIntegration:
    """Integration tests simulating real-world scenarios"""

    def test_typical_workflow_all_cluster_fips(self):
        """Test typical workflow: use all cluster FIPs for load balancing"""
        cluster_ips = ["10.1.1.20", "10.1.1.21", "10.1.1.22"]
        fake_client = FakeClient(
            [{"name": "Default", "floating_addresses": cluster_ips}]
        )

        # Initialize without user IPs
        target = TargetCluster(fake_client, network_name="Default")

        # Create 10 replications
        assignments = [target.get_next_dst_ip() for _ in range(10)]

        # Verify balanced distribution (3-4 each)
        for ip in cluster_ips:
            count = assignments.count(ip)
            assert 3 <= count <= 4, f"IP {ip} has {count} assignments, expected 3-4"

    def test_typical_workflow_subset_of_fips(self):
        """Test typical workflow: user specifies subset of FIPs"""
        cluster_ips = ["10.1.1.20", "10.1.1.21", "10.1.1.22", "10.1.1.23", "10.1.1.24"]
        user_ips = ["10.1.1.21", "10.1.1.23"]
        fake_client = FakeClient(
            [{"name": "Default", "floating_addresses": cluster_ips}]
        )

        # Initialize with subset
        target = TargetCluster(
            fake_client, user_provided_ips=user_ips, network_name="Default"
        )

        # Create 10 replications
        assignments = [target.get_next_dst_ip() for _ in range(10)]

        # Only user-specified IPs should be used
        assert all(ip in user_ips for ip in assignments)
        # Should be evenly balanced (5 each)
        assert assignments.count("10.1.1.21") == 5
        assert assignments.count("10.1.1.23") == 5

    def test_workflow_with_existing_replication_load(self):
        """Test workflow starting with existing replications (unbalanced load)"""
        cluster_ips = ["10.1.1.20", "10.1.1.21", "10.1.1.22"]
        fake_client = FakeClient(
            [{"name": "Default", "floating_addresses": cluster_ips}]
        )
        target = TargetCluster(fake_client, network_name="Default")

        # Simulate existing unbalanced replications
        target.dst_load["10.1.1.20"] = 15
        target.dst_load["10.1.1.21"] = 8
        target.dst_load["10.1.1.22"] = 2

        # Create 20 new replications
        assignments = [target.get_next_dst_ip() for _ in range(20)]

        # 10.1.1.22 should get most assignments to balance
        # Final state should be more balanced
        final_load = target.dst_load
        max_load = max(final_load.values())
        min_load = min(final_load.values())

        # The difference should be reduced (perfect balance would be 15)
        assert max_load - min_load <= 2, f"Load not balanced: {final_load}"


class TestTargetClusterDestinationInfo:
    """Test get_destination_info() method on TargetCluster"""

    def test_get_destination_info_returns_structured_data(self):
        """Test: get_destination_info returns properly structured data with real API format"""
        # Real target relationship data structure from pdb output
        target_relationships = [
            {
                "id": "23fa5196-cca8-4692-8e96-2a8d65186c29",
                "state": "DISCONNECTED",
                "end_reason": "",
                "source_cluster_name": "qwhat",
                "source_cluster_uuid": "7ea26661-0796-409c-a440-330757c26816",
                "source_root_path": "/snap_replication/",
                "source_root_read_only": False,
                "source_address": None,
                "source_port": None,
                "target_cluster_name": "qwho",
                "target_cluster_uuid": "dc611450-c2ba-4073-99a1-4999778f222d",
                "target_root_path": "/snap_replication/",
                "target_root_read_only": False,
                "job_state": "REPLICATION_NOT_RUNNING",
                "job_start_time": "",
                "recovery_point": "2025-04-22T15:10:00.00023319Z",
                "error_from_last_job": "",
                "duration_of_last_job": {"nanoseconds": "51844430"},
                "target_root_id": "62",
                "replication_enabled": True,
                "replication_job_status": None,
                "recovery_point_snapshot": {
                    "id": 88831,
                    "name": "revert_to_37140_for_qwhat",
                },
                "lock_key": None,
            },
            {
                "id": "b3b7d559-e89c-4323-a408-efeb38f60eb6",
                "state": "ESTABLISHED",
                "end_reason": "",
                "source_cluster_name": "qtest",
                "source_cluster_uuid": "fb9119f3-9ecd-4110-b6c4-44f65ecec31f",
                "source_root_path": "/Users/",
                "source_root_read_only": False,
                "source_address": None,
                "source_port": None,
                "target_cluster_name": "qwho",
                "target_cluster_uuid": "dc611450-c2ba-4073-99a1-4999778f222d",
                "target_root_path": "/Users/",
                "target_root_read_only": True,
                "job_state": "REPLICATION_NOT_RUNNING",
                "job_start_time": "",
                "recovery_point": "2025-10-21T06:39:30.802679475Z",
                "error_from_last_job": "",
                "duration_of_last_job": {"nanoseconds": "276261772"},
                "target_root_id": "4",
                "replication_enabled": True,
                "replication_job_status": None,
                "recovery_point_snapshot": {
                    "id": 10038,
                    "name": "replication_from_qtest",
                },
                "lock_key": None,
            },
        ]

        cluster_ips = ["10.120.0.81"]
        repl_api = FakeReplicationAPI(target_relationships)
        cluster_api = FakeClusterAPI("qwho")
        fake_client = FakeClient(
            [{"name": "Default", "floating_addresses": cluster_ips}],
            repl_api,
            cluster_api,
        )

        target = TargetCluster(fake_client, network_name="Default")
        dst_info = target.get_destination_info()

        # Verify structure
        assert "cluster_name" in dst_info
        assert "relationships" in dst_info
        assert dst_info["cluster_name"] == "qwho"
        assert len(dst_info["relationships"]) == 2
        assert dst_info["relationships"][0]["state"] == "DISCONNECTED"
        assert dst_info["relationships"][1]["state"] == "ESTABLISHED"

    def test_get_destination_info_handles_empty_relationships(self):
        """Test: get_destination_info handles clusters with no relationships"""
        cluster_ips = ["10.120.0.81"]
        repl_api = FakeReplicationAPI([])
        cluster_api = FakeClusterAPI("empty-cluster")
        fake_client = FakeClient(
            [{"name": "Default", "floating_addresses": cluster_ips}],
            repl_api,
            cluster_api,
        )

        target = TargetCluster(fake_client, network_name="Default")
        dst_info = target.get_destination_info()

        assert dst_info["cluster_name"] == "empty-cluster"
        assert dst_info["relationships"] == []


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
