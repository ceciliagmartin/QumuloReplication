#!/usr/bin/env python3
"""
Tests for ReplicationManager summary functionality with destination cluster info
Following TDD approach - these tests should fail initially
"""

import pytest
import logging
from typing import List, Dict, Any
from io import StringIO
import sys
from generate_replications import ReplicationManager, TargetCluster

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)


# Fake API implementations for testing
class FakeFileSystemAPI:
    """Fake file system API"""

    def __init__(self, file_attrs: Dict[str, Dict[str, str]]):
        """
        Args:
            file_attrs: Dict mapping file_id to attributes
                       e.g., {"123": {"path": "/test/path", "type": "FS_FILE_TYPE_DIRECTORY"}}
        """
        self.file_attrs = file_attrs

    def get_file_attr(self, file_id: str) -> Dict[str, str]:
        """Simulate getting file attributes"""
        if file_id not in self.file_attrs:
            raise ValueError(f"File ID {file_id} not found")
        return self.file_attrs[file_id]


class FakeReplicationAPI:
    """Fake replication API"""

    def __init__(self, source_relationships: List[Dict[str, Any]], target_relationships: List[Dict[str, Any]] = None):
        """
        Args:
            source_relationships: List of source replication relationships
            target_relationships: List of target replication relationships (for destination cluster)
        """
        self.source_relationships = source_relationships
        self.target_relationships = target_relationships or []

    def list_source_relationships(self) -> List[Dict[str, Any]]:
        """Simulate listing source relationships"""
        return self.source_relationships

    def list_target_relationship_statuses(self) -> List[Dict[str, Any]]:
        """Simulate listing target relationships"""
        return self.target_relationships


class FakeClusterAPI:
    """Fake cluster API for getting cluster info"""

    def __init__(self, cluster_name: str = "test-cluster", cluster_id: str = "cluster-123"):
        self.cluster_name = cluster_name
        self.cluster_id = cluster_id

    def get_cluster_conf(self) -> Dict[str, str]:
        """Simulate getting cluster configuration"""
        return {
            "cluster_name": self.cluster_name,
            "cluster_id": self.cluster_id
        }


class FakeRestClient:
    """Fake RestClient"""

    def __init__(self, fs_api: FakeFileSystemAPI, repl_api: FakeReplicationAPI, cluster_api: FakeClusterAPI = None):
        self.fs = fs_api
        self.replication = repl_api
        self.cluster = cluster_api or FakeClusterAPI()


class FakeClient:
    """Fake Client"""

    def __init__(self, rest_client: FakeRestClient):
        self.rc = rest_client


class TestReplicationSummaryWithoutDestination:
    """Test that existing summary functionality still works (backward compatibility)"""

    def test_summary_without_dst_shows_source_only(self, capsys):
        """Test: Summary without dst_host shows only source cluster info"""
        # Setup fake source cluster with replications
        file_attrs = {
            "fid-001": {"path": "/data/folder1"},
            "fid-002": {"path": "/data/folder2"},
            "fid-003": {"path": "/data/folder3"}
        }

        source_relationships = [
            {
                "id": "repl-001",
                "source_root_id": "fid-001",
                "target_address": "10.120.3.54"
            },
            {
                "id": "repl-002",
                "source_root_id": "fid-002",
                "target_address": "10.120.3.55"
            },
            {
                "id": "repl-003",
                "source_root_id": "fid-003",
                "target_address": "10.120.3.54"
            }
        ]

        fs_api = FakeFileSystemAPI(file_attrs)
        repl_api = FakeReplicationAPI(source_relationships)
        rest_client = FakeRestClient(fs_api, repl_api)
        fake_client = FakeClient(rest_client)

        # Create ReplicationManager and get status
        rm = ReplicationManager(fake_client)
        rm.get_replication_status()
        rm.display_status()

        # Capture output
        captured = capsys.readouterr()

        # Assertions
        assert "Source Cluster Replication Summary:" in captured.out
        assert "/data/folder1" in captured.out
        assert "/data/folder2" in captured.out
        assert "/data/folder3" in captured.out
        assert "10.120.3.54" in captured.out
        assert "10.120.3.55" in captured.out
        assert "Target IP" in captured.out
        assert "Replication Count" in captured.out

        # Should NOT have destination section (not implemented yet)
        assert "Destination Cluster Summary:" not in captured.out

    def test_summary_empty_source_cluster(self, capsys):
        """Test: Summary with no replications shows appropriate message"""
        fs_api = FakeFileSystemAPI({})
        repl_api = FakeReplicationAPI([])
        rest_client = FakeRestClient(fs_api, repl_api)
        fake_client = FakeClient(rest_client)

        rm = ReplicationManager(fake_client)
        rm.get_replication_status()
        rm.display_status()

        captured = capsys.readouterr()
        assert "No replication relationships found." in captured.out


class TestReplicationSummaryWithDestination:
    """Test new functionality: showing destination cluster info when dst_info provided"""

    def test_summary_with_dst_shows_both_clusters(self, capsys):
        """Test: Summary with dst_info shows both source and destination cluster info"""
        # Setup source cluster
        file_attrs = {
            "fid-001": {"path": "/data/folder1"},
            "fid-002": {"path": "/data/folder2"}
        }

        source_relationships = [
            {
                "id": "repl-001",
                "source_root_id": "fid-001",
                "target_address": "10.120.3.54"
            },
            {
                "id": "repl-002",
                "source_root_id": "fid-002",
                "target_address": "10.120.3.55"
            }
        ]

        fs_api = FakeFileSystemAPI(file_attrs)
        repl_api = FakeReplicationAPI(source_relationships)
        rest_client = FakeRestClient(fs_api, repl_api)
        fake_client = FakeClient(rest_client)

        # Setup destination info (this is what we'll pass to display_status)
        dst_info = {
            "cluster_name": "destination-cluster",
            "cluster_id": "dst-cluster-123",
            "relationships": [
                {
                    "id": "repl-001",
                    "source_root_path": "/data/folder1",
                    "target_root_path": "/data/folder1",
                    "state": "ESTABLISHED",
                    "source_cluster_name": "source-cluster"
                },
                {
                    "id": "repl-002",
                    "source_root_path": "/data/folder2",
                    "target_root_path": "/data/folder2",
                    "state": "ESTABLISHED",
                    "source_cluster_name": "source-cluster"
                }
            ]
        }

        # Create ReplicationManager and display status with dst_info
        rm = ReplicationManager(fake_client)
        rm.get_replication_status()
        rm.display_status(dst_info=dst_info)

        captured = capsys.readouterr()

        # Should have BOTH source and destination sections
        assert "Source Cluster Replication Summary:" in captured.out
        assert "Destination Cluster Summary:" in captured.out
        assert "destination-cluster" in captured.out
        assert "ESTABLISHED" in captured.out
        assert "/data/folder1" in captured.out
        assert "/data/folder2" in captured.out

    def test_dst_info_shows_relationship_states(self, capsys):
        """Test: Destination info shows relationship states (ESTABLISHED, PENDING, etc.)"""
        # Minimal source setup
        fs_api = FakeFileSystemAPI({})
        repl_api = FakeReplicationAPI([])
        rest_client = FakeRestClient(fs_api, repl_api)
        fake_client = FakeClient(rest_client)

        # Destination info with various states
        dst_info = {
            "cluster_name": "dst-cluster",
            "cluster_id": "dst-123",
            "relationships": [
                {
                    "id": "repl-001",
                    "source_root_path": "/path1",
                    "target_root_path": "/path1",
                    "state": "ESTABLISHED",
                    "source_cluster_name": "src-cluster"
                },
                {
                    "id": "repl-002",
                    "source_root_path": "/path2",
                    "target_root_path": "/path2",
                    "state": "AWAITING_AUTHORIZATION",
                    "source_cluster_name": "src-cluster"
                },
                {
                    "id": "repl-003",
                    "source_root_path": "/path3",
                    "target_root_path": "/path3",
                    "state": "REPLICATING",
                    "source_cluster_name": "src-cluster"
                }
            ]
        }

        rm = ReplicationManager(fake_client)
        rm.display_status(dst_info=dst_info)

        captured = capsys.readouterr()

        # Should display all different states
        assert "ESTABLISHED" in captured.out
        assert "AWAITING_AUTHORIZATION" in captured.out
        assert "REPLICATING" in captured.out
        assert "Destination Cluster Summary:" in captured.out

    def test_summary_with_empty_dst_info(self, capsys):
        """Test: Summary with dst_info but no relationships shows appropriate message"""
        fs_api = FakeFileSystemAPI({})
        repl_api = FakeReplicationAPI([])
        rest_client = FakeRestClient(fs_api, repl_api)
        fake_client = FakeClient(rest_client)

        # Destination with no relationships
        dst_info = {
            "cluster_name": "dst-cluster",
            "cluster_id": "dst-123",
            "relationships": []
        }

        rm = ReplicationManager(fake_client)
        rm.display_status(dst_info=dst_info)

        captured = capsys.readouterr()

        assert "Destination Cluster Summary:" in captured.out
        assert "No target replication relationships found" in captured.out or "dst-cluster" in captured.out


class TestDestinationInfoRetrieval:
    """Test retrieving destination cluster information"""

    def test_get_destination_info_returns_structured_data(self):
        """Test: get_destination_info returns properly structured data"""
        # Setup destination cluster with target relationships
        target_relationships = [
            {
                "id": "repl-001",
                "source_root_path": "/data/folder1",
                "target_root_path": "/data/folder1",
                "state": "ESTABLISHED",
                "source_cluster_name": "source-cluster"
            },
            {
                "id": "repl-002",
                "source_root_path": "/data/folder2",
                "target_root_path": "/data/folder2",
                "state": "AWAITING_AUTHORIZATION",
                "source_cluster_name": "source-cluster"
            }
        ]

        fs_api = FakeFileSystemAPI({})
        repl_api = FakeReplicationAPI([], target_relationships)
        cluster_api = FakeClusterAPI("destination-cluster", "dst-123")
        rest_client = FakeRestClient(fs_api, repl_api, cluster_api)
        fake_client = FakeClient(rest_client)

        # This method doesn't exist yet - will implement next
        rm = ReplicationManager(fake_client)
        dst_info = rm.get_destination_info()

        # Verify structure
        assert "cluster_name" in dst_info
        assert "cluster_id" in dst_info
        assert "relationships" in dst_info
        assert dst_info["cluster_name"] == "destination-cluster"
        assert len(dst_info["relationships"]) == 2
        assert dst_info["relationships"][0]["state"] == "ESTABLISHED"
        assert dst_info["relationships"][1]["state"] == "AWAITING_AUTHORIZATION"

    def test_get_destination_info_handles_empty_relationships(self):
        """Test: get_destination_info handles clusters with no relationships"""
        fs_api = FakeFileSystemAPI({})
        repl_api = FakeReplicationAPI([], [])
        cluster_api = FakeClusterAPI("empty-cluster", "empty-123")
        rest_client = FakeRestClient(fs_api, repl_api, cluster_api)
        fake_client = FakeClient(rest_client)

        rm = ReplicationManager(fake_client)
        dst_info = rm.get_destination_info()

        assert dst_info["cluster_name"] == "empty-cluster"
        assert dst_info["relationships"] == []


class TestConnectionErrorHandling:
    """Test graceful error handling when connection to destination fails"""

    def test_dst_connection_failure_handled_gracefully(self):
        """Test: Connection failure to dst_host doesn't crash, shows error message"""
        # This will be tested at the integration level in main()
        # For now, we'll verify that None dst_info doesn't crash display_status

        fs_api = FakeFileSystemAPI({})
        repl_api = FakeReplicationAPI([])
        rest_client = FakeRestClient(fs_api, repl_api)
        fake_client = FakeClient(rest_client)

        rm = ReplicationManager(fake_client)

        # Should not raise an exception when dst_info is None
        try:
            rm.display_status(dst_info=None)
            # Success - no exception raised
        except Exception as e:
            pytest.fail(f"display_status raised exception with None dst_info: {e}")


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
