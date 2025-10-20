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
    """Fake network API that simulates Qumulo network.get_floating_ip_allocation()"""

    def __init__(self, networks: Dict[int, List[str]]):
        """
        Args:
            networks: Dict mapping network_id to list of floating IPs
                     e.g., {1: ["10.1.1.20", "10.1.1.21"], 2: ["10.2.2.30"]}
        """
        self.networks = networks

    def get_floating_ip_allocation(self) -> List[Dict[str, Any]]:
        """Simulate the API response for floating IP allocation

        Returns: [{'id': 1, 'floating_addresses': [...]}, {'id': 2, 'floating_addresses': [...]}]
        """
        return [
            {"id": net_id, "floating_addresses": addresses}
            for net_id, addresses in self.networks.items()
        ]


class FakeRestClient:
    """Fake RestClient that mimics qumulo.rest_client.RestClient"""

    def __init__(self, networks: Dict[int, List[str]]):
        self.network = FakeNetworkAPI(networks)


class FakeClient:
    """Fake Client that mimics the Client class from qqbase"""

    def __init__(self, networks: Dict[int, List[str]]):
        self.rc = FakeRestClient(networks)


# Test cases
class TestTargetClusterInitialization:
    """Test TargetCluster initialization with various scenarios"""

    def test_init_with_multiple_cluster_fips(self):
        """Test initialization using all cluster FIPs (no user-provided IPs)"""
        cluster_ips = ["10.1.1.20", "10.1.1.21", "10.1.1.22"]
        fake_client = FakeClient({1: cluster_ips})

        target = TargetCluster(fake_client, network_id=1)

        assert target.available_ips == cluster_ips
        assert all(target.dst_load[ip] == 0 for ip in cluster_ips)
        assert len(target.dst_load) == 3

    def test_init_with_valid_user_provided_ips(self):
        """Test initialization with user-provided IPs that exist in cluster"""
        cluster_ips = ["10.1.1.20", "10.1.1.21", "10.1.1.22", "10.1.1.23"]
        user_ips = ["10.1.1.21", "10.1.1.22"]
        fake_client = FakeClient({1: cluster_ips})

        target = TargetCluster(fake_client, user_provided_ips=user_ips, network_id=1)

        assert target.available_ips == user_ips
        assert all(target.dst_load[ip] == 0 for ip in user_ips)
        assert len(target.dst_load) == 2

    def test_init_with_single_ip_warns(self, caplog):
        """Test initialization with single IP logs warning"""
        cluster_ips = ["10.1.1.20", "10.1.1.21"]
        user_ips = ["10.1.1.20"]
        fake_client = FakeClient({1: cluster_ips})

        with caplog.at_level(logging.WARNING):
            target = TargetCluster(fake_client, user_provided_ips=user_ips, network_id=1)

        assert target.available_ips == user_ips
        assert "may be unbalanced" in caplog.text

    def test_init_with_invalid_user_ips_fails_fast(self):
        """Test that invalid user IPs cause immediate failure"""
        cluster_ips = ["10.1.1.20", "10.1.1.21"]
        user_ips = ["10.1.1.99", "10.1.1.100"]
        fake_client = FakeClient({1: cluster_ips})

        with pytest.raises(ValueError) as exc_info:
            TargetCluster(fake_client, user_provided_ips=user_ips, network_id=1)

        assert "Invalid destination IPs" in str(exc_info.value)
        assert "10.1.1.99" in str(exc_info.value)
        assert "10.1.1.100" in str(exc_info.value)

    def test_init_with_partial_invalid_ips_fails_fast(self):
        """Test that even one invalid IP causes failure"""
        cluster_ips = ["10.1.1.20", "10.1.1.21"]
        user_ips = ["10.1.1.20", "10.1.1.99"]  # One valid, one invalid
        fake_client = FakeClient({1: cluster_ips})

        with pytest.raises(ValueError) as exc_info:
            TargetCluster(fake_client, user_provided_ips=user_ips, network_id=1)

        assert "10.1.1.99" in str(exc_info.value)

    def test_init_with_empty_cluster_fips(self):
        """Test initialization when cluster has no FIPs"""
        fake_client = FakeClient({1: []})

        with pytest.raises(ValueError) as exc_info:
            TargetCluster(fake_client, network_id=1)

        assert "has no Floating IPs available" in str(exc_info.value)

    def test_init_with_invalid_network_id(self):
        """Test that invalid network_id causes failure"""
        fake_client = FakeClient({1: ["10.1.1.20", "10.1.1.21"]})

        with pytest.raises(ValueError) as exc_info:
            TargetCluster(fake_client, network_id=99)

        assert "Network ID 99 not found" in str(exc_info.value)


class TestTargetClusterLoadBalancing:
    """Test least-used load balancing strategy"""

    def test_get_next_dst_ip_round_robin_on_equal_load(self):
        """Test that with equal load, IPs are selected fairly"""
        cluster_ips = ["10.1.1.20", "10.1.1.21", "10.1.1.22"]
        fake_client = FakeClient({1: cluster_ips})
        target = TargetCluster(fake_client, network_id=1)

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
        fake_client = FakeClient({1: cluster_ips})
        target = TargetCluster(fake_client, network_id=1)

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
        fake_client = FakeClient({1: []})

        with pytest.raises(ValueError) as exc_info:
            target = TargetCluster(fake_client, network_id=1)

        assert "has no Floating IPs available" in str(exc_info.value)

    def test_load_balancing_with_initial_load(self):
        """Test that initial load state is respected"""
        cluster_ips = ["10.1.1.20", "10.1.1.21"]
        fake_client = FakeClient({1: cluster_ips})
        target = TargetCluster(fake_client, network_id=1)

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
        fake_client = FakeClient({1: cluster_ips})
        target = TargetCluster(fake_client, network_id=1)

        retrieved_ips = target.get_dst_ips()

        assert set(retrieved_ips) == set(cluster_ips)
        assert len(retrieved_ips) == 4

    def test_get_dst_ips_handles_empty_response(self):
        """Test get_dst_ips when cluster has no FIPs"""
        fake_client = FakeClient({1: []})

        with pytest.raises(ValueError) as exc_info:
            TargetCluster(fake_client, network_id=1)

        assert "has no Floating IPs available" in str(exc_info.value)

    def test_get_dst_ips_from_multiple_networks(self):
        """Test that get_dst_ips retrieves FIPs from correct network"""
        fake_client = FakeClient({
            1: ["10.1.1.20", "10.1.1.21"],
            2: ["10.2.2.30", "10.2.2.31", "10.2.2.32"]
        })
        target = TargetCluster(fake_client, network_id=2)

        retrieved_ips = target.get_dst_ips()

        assert set(retrieved_ips) == {"10.2.2.30", "10.2.2.31", "10.2.2.32"}
        assert len(retrieved_ips) == 3


class TestTargetClusterIntegration:
    """Integration tests simulating real-world scenarios"""

    def test_typical_workflow_all_cluster_fips(self):
        """Test typical workflow: use all cluster FIPs for load balancing"""
        cluster_ips = ["10.1.1.20", "10.1.1.21", "10.1.1.22"]
        fake_client = FakeClient({1: cluster_ips})

        # Initialize without user IPs
        target = TargetCluster(fake_client, network_id=1)

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
        fake_client = FakeClient({1: cluster_ips})

        # Initialize with subset
        target = TargetCluster(fake_client, user_provided_ips=user_ips, network_id=1)

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
        fake_client = FakeClient({1: cluster_ips})
        target = TargetCluster(fake_client, network_id=1)

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


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
