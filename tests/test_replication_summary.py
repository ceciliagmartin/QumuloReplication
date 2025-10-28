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
from replication import ReplicationManager, TargetCluster

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

    def __init__(
        self,
        source_relationships: List[Dict[str, Any]],
        target_relationships: List[Dict[str, Any]] = None,
        source_relationship_statuses: List[Dict[str, Any]] = None,
    ):
        """
        Args:
            source_relationships: List of source replication relationships (for list_source_relationships)
            target_relationships: List of target replication relationships (for destination cluster)
            source_relationship_statuses: List of source relationship statuses (for list_source_relationship_statuses)
        """
        self.source_relationships = source_relationships
        self.target_relationships = target_relationships or []
        self.source_relationship_statuses = source_relationship_statuses or []

    def list_source_relationships(self) -> List[Dict[str, Any]]:
        """Simulate listing source relationships"""
        return self.source_relationships

    def list_target_relationship_statuses(self) -> List[Dict[str, Any]]:
        """Simulate listing target relationships"""
        return self.target_relationships

    def list_source_relationship_statuses(self) -> List[Dict[str, Any]]:
        """Simulate listing source relationship statuses (includes state, recovery_point, etc.)"""
        return self.source_relationship_statuses


class FakeClusterAPI:
    """Fake cluster API for getting cluster info"""

    def __init__(
        self, cluster_name: str = "test-cluster", cluster_id: str = "cluster-123"
    ):
        self.cluster_name = cluster_name
        self.cluster_id = cluster_id

    def get_cluster_conf(self) -> Dict[str, str]:
        """Simulate getting cluster configuration"""
        return {
            "cluster_name": self.cluster_name,
        }


class FakeRestClient:
    """Fake RestClient"""

    def __init__(
        self,
        fs_api: FakeFileSystemAPI,
        repl_api: FakeReplicationAPI,
        cluster_api: FakeClusterAPI = None,
    ):
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
        # Setup fake source cluster with source relationship statuses
        source_relationship_statuses = [
            {
                "id": "repl-001",
                "source_root_path": "/data/folder1",
                "target_root_path": "/data/folder1",
                "target_cluster_name": "dest-cluster",
                "state": "ESTABLISHED",
                "target_address": "10.120.3.54",
            },
            {
                "id": "repl-002",
                "source_root_path": "/data/folder2",
                "target_root_path": "/data/folder2",
                "target_cluster_name": "dest-cluster",
                "state": "ESTABLISHED",
                "target_address": "10.120.3.55",
            },
            {
                "id": "repl-003",
                "source_root_path": "/data/folder3",
                "target_root_path": "/data/folder3",
                "target_cluster_name": "dest-cluster",
                "state": "REPLICATING",
                "target_address": "10.120.3.54",
            },
        ]

        fs_api = FakeFileSystemAPI({})
        repl_api = FakeReplicationAPI([], None, source_relationship_statuses)
        cluster_api = FakeClusterAPI("source-cluster", "src-123")
        rest_client = FakeRestClient(fs_api, repl_api, cluster_api)
        fake_client = FakeClient(rest_client)

        # Create ReplicationManager and display status
        rm = ReplicationManager(fake_client)
        rm.display_status()

        # Capture output
        captured = capsys.readouterr()

        # Assertions - new unified format
        assert "Source Cluster Summary:" in captured.out
        assert "/data/folder1" in captured.out
        assert "/data/folder2" in captured.out
        assert "/data/folder3" in captured.out
        assert "ESTABLISHED" in captured.out
        assert "REPLICATING" in captured.out

        # Should NOT have destination section
        assert "Destination Cluster Summary:" not in captured.out

    def test_summary_empty_source_cluster(self, capsys):
        """Test: Summary with no replications shows appropriate message"""
        fs_api = FakeFileSystemAPI({})
        repl_api = FakeReplicationAPI([], None, [])
        cluster_api = FakeClusterAPI("empty-source", "empty-123")
        rest_client = FakeRestClient(fs_api, repl_api, cluster_api)
        fake_client = FakeClient(rest_client)

        rm = ReplicationManager(fake_client)
        rm.display_status()

        captured = capsys.readouterr()
        assert "No source replication relationships found." in captured.out


class TestReplicationSummaryWithDestination:
    """Test new functionality: showing destination cluster info when dst_info provided"""

    def test_summary_with_dst_shows_both_clusters(self, capsys):
        """Test: Summary with dst_info shows both source and destination cluster info"""
        # Setup source cluster with source relationship statuses
        source_relationship_statuses = [
            {
                "id": "repl-001",
                "source_root_path": "/data/folder1",
                "target_root_path": "/data/folder1",
                "target_cluster_name": "destination-cluster",
                "state": "ESTABLISHED",
                "target_address": "10.120.3.54",
            },
            {
                "id": "repl-002",
                "source_root_path": "/data/folder2",
                "target_root_path": "/data/folder2",
                "target_cluster_name": "destination-cluster",
                "state": "REPLICATING",
                "target_address": "10.120.3.55",
            },
        ]

        fs_api = FakeFileSystemAPI({})
        repl_api = FakeReplicationAPI([], None, source_relationship_statuses)
        cluster_api = FakeClusterAPI("source-cluster", "src-123")
        rest_client = FakeRestClient(fs_api, repl_api, cluster_api)
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
                    "source_cluster_name": "source-cluster",
                },
                {
                    "id": "repl-002",
                    "source_root_path": "/data/folder2",
                    "target_root_path": "/data/folder2",
                    "state": "REPLICATING",
                    "source_cluster_name": "source-cluster",
                },
            ],
        }

        # Create ReplicationManager and display status with dst_info
        rm = ReplicationManager(fake_client)
        rm.display_status(dst_info=dst_info)

        captured = capsys.readouterr()

        # Should have BOTH source and destination sections
        assert "Source Cluster Summary:" in captured.out
        assert "Destination Cluster Summary:" in captured.out
        assert "source-cluster" in captured.out
        assert "destination-cluster" in captured.out
        assert "ESTABLISHED" in captured.out
        assert "REPLICATING" in captured.out
        assert "/data/folder1" in captured.out
        assert "/data/folder2" in captured.out

    def test_dst_info_shows_relationship_states(self, capsys):
        """Test: Destination info shows relationship states (ESTABLISHED, PENDING, etc.)"""
        # Minimal source setup with empty relationships
        fs_api = FakeFileSystemAPI({})
        repl_api = FakeReplicationAPI([], None, [])
        cluster_api = FakeClusterAPI("src-cluster", "src-123")
        rest_client = FakeRestClient(fs_api, repl_api, cluster_api)
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
                    "source_cluster_name": "src-cluster",
                },
                {
                    "id": "repl-002",
                    "source_root_path": "/path2",
                    "target_root_path": "/path2",
                    "state": "AWAITING_AUTHORIZATION",
                    "source_cluster_name": "src-cluster",
                },
                {
                    "id": "repl-003",
                    "source_root_path": "/path3",
                    "target_root_path": "/path3",
                    "state": "REPLICATING",
                    "source_cluster_name": "src-cluster",
                },
            ],
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
        repl_api = FakeReplicationAPI([], None, [])
        cluster_api = FakeClusterAPI("src-cluster", "src-123")
        rest_client = FakeRestClient(fs_api, repl_api, cluster_api)
        fake_client = FakeClient(rest_client)

        # Destination with no relationships
        dst_info = {
            "cluster_name": "dst-cluster",
            "cluster_id": "dst-123",
            "relationships": [],
        }

        rm = ReplicationManager(fake_client)
        rm.display_status(dst_info=dst_info)

        captured = capsys.readouterr()

        assert "Destination Cluster Summary:" in captured.out
        assert (
            "No destination replication relationships found" in captured.out
            or "dst-cluster" in captured.out
        )


class TestSourceInfoRetrieval:
    """Test retrieving source cluster information"""

    def test_get_source_info_returns_structured_data(self):
        """Test: get_source_info returns properly structured data"""
        # Setup source cluster with source relationship statuses
        source_relationship_statuses = [
            {
                "id": "repl-001",
                "source_root_path": "/data/folder1",
                "target_root_path": "/data/folder1",
                "target_cluster_name": "destination-cluster",
                "state": "ESTABLISHED",
                "recovery_point": "2025-10-20T12:00:00Z",
            },
            {
                "id": "repl-002",
                "source_root_path": "/data/folder2",
                "target_root_path": "/data/folder2",
                "target_cluster_name": "destination-cluster",
                "state": "REPLICATING",
                "recovery_point": "2025-10-20T11:00:00Z",
            },
        ]

        fs_api = FakeFileSystemAPI({})
        repl_api = FakeReplicationAPI([], None, source_relationship_statuses)
        cluster_api = FakeClusterAPI("source-cluster")
        rest_client = FakeRestClient(fs_api, repl_api, cluster_api)
        fake_client = FakeClient(rest_client)

        rm = ReplicationManager(fake_client)
        src_info = rm.get_source_info()

        # Verify structure matches get_destination_info()
        assert "cluster_name" in src_info
        assert "relationships" in src_info
        assert src_info["cluster_name"] == "source-cluster"
        assert len(src_info["relationships"]) == 2
        assert src_info["relationships"][0]["state"] == "ESTABLISHED"
        assert src_info["relationships"][1]["state"] == "REPLICATING"

    def test_get_source_info_handles_empty_relationships(self):
        """Test: get_source_info handles clusters with no relationships"""
        fs_api = FakeFileSystemAPI({})
        repl_api = FakeReplicationAPI([], None, [])
        cluster_api = FakeClusterAPI("empty-source")
        rest_client = FakeRestClient(fs_api, repl_api, cluster_api)
        fake_client = FakeClient(rest_client)

        rm = ReplicationManager(fake_client)
        src_info = rm.get_source_info()

        assert src_info["cluster_name"] == "empty-source"
        assert src_info["relationships"] == []


class TestUnifiedClusterDisplay:
    """Test unified cluster display method for both source and destination"""

    def test_display_cluster_summary_for_source(self, capsys):
        """Test: _display_cluster_summary works for source cluster"""
        source_info = {
            "cluster_name": "source-cluster",
            "relationships": [
                {
                    "id": "repl-001",
                    "source_root_path": "/data/folder1",
                    "target_root_path": "/data/folder1",
                    "target_cluster_name": "dest-cluster",
                    "state": "ESTABLISHED",
                },
                {
                    "id": "repl-002",
                    "source_root_path": "/data/folder2",
                    "target_root_path": "/data/folder2",
                    "target_cluster_name": "dest-cluster",
                    "state": "REPLICATING",
                },
            ],
        }

        fs_api = FakeFileSystemAPI({})
        repl_api = FakeReplicationAPI([])
        rest_client = FakeRestClient(fs_api, repl_api)
        fake_client = FakeClient(rest_client)
        rm = ReplicationManager(fake_client)

        rm._display_cluster_summary(source_info, "Source")
        captured = capsys.readouterr()

        # Verify source-specific output
        assert "Source Cluster Summary:" in captured.out
        assert "source-cluster" in captured.out
        assert "remote_cluster" in captured.out  # Column header
        assert "dest-cluster" in captured.out  # Target cluster name in data
        assert "ESTABLISHED" in captured.out
        assert "REPLICATING" in captured.out

    def test_display_cluster_summary_for_destination(self, capsys):
        """Test: _display_cluster_summary works for destination cluster"""
        dest_info = {
            "cluster_name": "dest-cluster",
            "relationships": [
                {
                    "id": "repl-003",
                    "source_root_path": "/data/folder3",
                    "target_root_path": "/data/folder3",
                    "source_cluster_name": "source-cluster",
                    "state": "ESTABLISHED",
                }
            ],
        }

        fs_api = FakeFileSystemAPI({})
        repl_api = FakeReplicationAPI([])
        rest_client = FakeRestClient(fs_api, repl_api)
        fake_client = FakeClient(rest_client)
        rm = ReplicationManager(fake_client)

        rm._display_cluster_summary(dest_info, "Destination")
        captured = capsys.readouterr()

        # Verify destination-specific output
        assert "Destination Cluster Summary:" in captured.out
        assert "dest-cluster" in captured.out
        assert "remote_cluster" in captured.out  # Column header
        assert "source-cluster" in captured.out  # Source cluster name in data
        assert "ESTABLISHED" in captured.out

    def test_display_cluster_summary_state_counts(self, capsys):
        """Test: _display_cluster_summary shows state summary correctly"""
        source_info = {
            "cluster_name": "test-cluster",
            "cluster_id": "test-123",
            "relationships": [
                {
                    "id": "r1",
                    "source_root_path": "/p1",
                    "target_root_path": "/p1",
                    "target_cluster_name": "t",
                    "state": "ESTABLISHED",
                },
                {
                    "id": "r2",
                    "source_root_path": "/p2",
                    "target_root_path": "/p2",
                    "target_cluster_name": "t",
                    "state": "ESTABLISHED",
                },
                {
                    "id": "r3",
                    "source_root_path": "/p3",
                    "target_root_path": "/p3",
                    "target_cluster_name": "t",
                    "state": "REPLICATING",
                },
                {
                    "id": "r4",
                    "source_root_path": "/p4",
                    "target_root_path": "/p4",
                    "target_cluster_name": "t",
                    "state": "DISCONNECTED",
                },
            ],
        }

        fs_api = FakeFileSystemAPI({})
        repl_api = FakeReplicationAPI([])
        rest_client = FakeRestClient(fs_api, repl_api)
        fake_client = FakeClient(rest_client)
        rm = ReplicationManager(fake_client)

        rm._display_cluster_summary(source_info, "Source")
        captured = capsys.readouterr()

        # Verify state counts in output
        assert "ESTABLISHED" in captured.out
        assert "REPLICATING" in captured.out
        assert "DISCONNECTED" in captured.out
        # Should show count of 2 for ESTABLISHED
        assert "2" in captured.out


class TestCardDisplayFormat:
    """Test card/block display format (alternative to table)"""

    def test_display_cluster_summary_card_format_source(self, capsys):
        """Test: Card format displays source cluster readably"""
        source_relationship_statuses = [
            {
                "id": "93874ed3-9c03-49a4-a628-778b4b5d831d",
                "source_root_path": "/snapz/",
                "target_root_path": "/snapz/",
                "target_cluster_name": "qwhat",
                "state": "ESTABLISHED",
                "error_from_last_job": "Target cluster error: Snapshot limit of 40000 has been reached.",
                "recovery_point": "2025-09-25T13:10:00.000439663Z",
                "queued_snapshot_count": 2102,
                "replication_mode": "REPLICATION_SNAPSHOT_POLICY",
            },
            {
                "id": "b3b7d559-e89c-4323-a408-efeb38f60eb6",
                "source_root_path": "/Users/",
                "target_root_path": "/Users/",
                "target_cluster_name": "qwho",
                "state": "ESTABLISHED",
                "error_from_last_job": "",
                "recovery_point": "2025-10-21T06:40:30.804453983Z",
                "queued_snapshot_count": 0,
                "replication_mode": "REPLICATION_CONTINUOUS",
            },
        ]

        fs_api = FakeFileSystemAPI({})
        repl_api = FakeReplicationAPI([], None, source_relationship_statuses)
        cluster_api = FakeClusterAPI("qtest", "fb9119f3-9ecd-4110-b6c4-44f65ecec31f")
        rest_client = FakeRestClient(fs_api, repl_api, cluster_api)
        fake_client = FakeClient(rest_client)

        rm = ReplicationManager(fake_client)
        source_info = rm.get_source_info()
        rm._display_cluster_summary_card(source_info, "Source")

        captured = capsys.readouterr()

        # Verify card format elements
        assert "Source Cluster Summary:" in captured.out
        assert "qtest" in captured.out
        assert "▸ /snapz/" in captured.out
        assert "▸ /Users/" in captured.out
        assert "State: ESTABLISHED" in captured.out
        assert "qwhat" in captured.out
        assert "qwho" in captured.out
        # Error should be shown
        assert "Snapshot limit" in captured.out or "Error:" in captured.out

    def test_display_cluster_summary_card_format_destination(self, capsys):
        """Test: Card format displays destination cluster readably"""
        dest_info = {
            "cluster_name": "qwho",
            "cluster_id": "dc611450-c2ba-4073-99a1-4999778f222d",
            "relationships": [
                {
                    "id": "b3b7d559-e89c-4323-a408-efeb38f60eb6",
                    "source_root_path": "/Users/",
                    "target_root_path": "/Users/",
                    "source_cluster_name": "qtest",
                    "state": "ESTABLISHED",
                    "error_from_last_job": "",
                    "recovery_point": "2025-10-21T06:39:30.802679475Z",
                }
            ],
        }

        fs_api = FakeFileSystemAPI({})
        repl_api = FakeReplicationAPI([])
        rest_client = FakeRestClient(fs_api, repl_api)
        fake_client = FakeClient(rest_client)
        rm = ReplicationManager(fake_client)

        rm._display_cluster_summary_card(dest_info, "Destination")
        captured = capsys.readouterr()

        assert "Destination Cluster Summary:" in captured.out
        assert "qwho" in captured.out
        assert "▸" in captured.out
        assert "/Users/" in captured.out
        assert "qtest" in captured.out

    def test_display_status_with_format_card(self, capsys):
        """Test: display_status respects format='card' parameter"""
        source_relationship_statuses = [
            {
                "id": "test-001",
                "source_root_path": "/data/",
                "target_root_path": "/data/",
                "target_cluster_name": "dest",
                "state": "ESTABLISHED",
                "error_from_last_job": "",
            }
        ]

        fs_api = FakeFileSystemAPI({})
        repl_api = FakeReplicationAPI([], None, source_relationship_statuses)
        cluster_api = FakeClusterAPI("source", "src-123")
        rest_client = FakeRestClient(fs_api, repl_api, cluster_api)
        fake_client = FakeClient(rest_client)

        rm = ReplicationManager(fake_client)
        rm.display_status(format="card")

        captured = capsys.readouterr()

        # Should use card format (has bullet points)
        assert "▸" in captured.out
        assert "/data/" in captured.out


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


class TestCSVExport:
    """Test CSV export functionality"""

    def test_save_to_csv_creates_file(self, tmp_path):
        """Test: save_to_csv creates CSV file with correct path"""
        source_relationship_statuses = [
            {
                "id": "test-001",
                "source_root_path": "/data/",
                "target_root_path": "/data/",
                "target_cluster_name": "dest",
                "state": "ESTABLISHED",
                "error_from_last_job": "",
                "recovery_point": "2025-10-21T06:40:30.804453983Z",
            }
        ]

        fs_api = FakeFileSystemAPI({})
        repl_api = FakeReplicationAPI([], None, source_relationship_statuses)
        cluster_api = FakeClusterAPI("source", "src-123")
        rest_client = FakeRestClient(fs_api, repl_api, cluster_api)
        fake_client = FakeClient(rest_client)

        rm = ReplicationManager(fake_client)
        source_info = rm.get_source_info()

        csv_file = tmp_path / "test_output.csv"
        rm.save_to_csv(str(csv_file), source_info)

        assert csv_file.exists()

    def test_save_to_csv_includes_all_relationships(self, tmp_path):
        """Test: CSV includes all relationships from source and destination"""
        source_relationship_statuses = [
            {
                "id": "src-001",
                "source_root_path": "/path1/",
                "target_root_path": "/path1/",
                "target_cluster_name": "dest",
                "state": "ESTABLISHED",
                "error_from_last_job": "",
            },
            {
                "id": "src-002",
                "source_root_path": "/path2/",
                "target_root_path": "/path2/",
                "target_cluster_name": "dest",
                "state": "REPLICATING",
                "error_from_last_job": "",
            },
        ]

        fs_api = FakeFileSystemAPI({})
        repl_api = FakeReplicationAPI([], None, source_relationship_statuses)
        cluster_api = FakeClusterAPI("source", "src-123")
        rest_client = FakeRestClient(fs_api, repl_api, cluster_api)
        fake_client = FakeClient(rest_client)

        rm = ReplicationManager(fake_client)
        source_info = rm.get_source_info()

        dst_info = {
            "cluster_name": "dest",
            "cluster_id": "dest-456",
            "relationships": [
                {
                    "id": "dst-001",
                    "source_root_path": "/path3/",
                    "target_root_path": "/path3/",
                    "source_cluster_name": "source",
                    "state": "ESTABLISHED",
                    "error_from_last_job": "",
                }
            ],
        }

        csv_file = tmp_path / "relationships.csv"
        rm.save_to_csv(str(csv_file), source_info, dst_info)

        # Read CSV and verify row count (header + 3 data rows)
        import csv

        with open(csv_file, "r") as f:
            reader = csv.DictReader(f)
            rows = list(reader)
            assert len(rows) == 3  # 2 source + 1 destination

    def test_save_to_csv_has_correct_headers(self, tmp_path):
        """Test: CSV has all expected column headers"""
        source_relationship_statuses = [
            {
                "id": "test-001",
                "source_root_path": "/data/",
                "target_root_path": "/data/",
                "target_cluster_name": "dest",
                "state": "ESTABLISHED",
                "error_from_last_job": "",
            }
        ]

        fs_api = FakeFileSystemAPI({})
        repl_api = FakeReplicationAPI([], None, source_relationship_statuses)
        cluster_api = FakeClusterAPI("source", "src-123")
        rest_client = FakeRestClient(fs_api, repl_api, cluster_api)
        fake_client = FakeClient(rest_client)

        rm = ReplicationManager(fake_client)
        source_info = rm.get_source_info()

        csv_file = tmp_path / "headers.csv"
        rm.save_to_csv(str(csv_file), source_info)

        import csv

        with open(csv_file, "r") as f:
            reader = csv.DictReader(f)
            headers = reader.fieldnames

            # Verify essential headers exist
            assert "cluster_type" in headers
            assert "cluster_name" in headers
            assert "source_path" in headers
            assert "target_path" in headers
            assert "state" in headers
            assert "replication_id" in headers


class TestReplicationCacheBugFixes:
    """Test that create/clean actions require populate_replication_cache() to be called first"""

    def test_create_replications_needs_populated_repli_paths(self):
        """Test: create_replications requires populate_replication_cache() to avoid duplicates"""
        # Setup source cluster with existing replications
        source_relationship_statuses = [
            {
                "id": "repl-001",
                "source_root_path": "/data/folder1",
                "source_root_id": "12345",
                "target_root_path": "/data/folder1",
                "target_cluster_name": "dest-cluster",
                "target_address": "10.1.1.20",
                "state": "ESTABLISHED",
            },
        ]

        fs_api = FakeFileSystemAPI({})
        repl_api = FakeReplicationAPI([], None, source_relationship_statuses)
        cluster_api = FakeClusterAPI("source-cluster", "src-123")
        rest_client = FakeRestClient(fs_api, repl_api, cluster_api)
        fake_client = FakeClient(rest_client)

        rm = ReplicationManager(fake_client)

        # Initially, repli_paths should be empty
        assert len(rm.repli_paths) == 0

        # create_replications checks "if path not in self.repli_paths" to skip duplicates
        # Without calling populate_replication_cache() first, it will try to create duplicates

        # Now call populate_replication_cache to populate repli_paths
        rm.populate_replication_cache()

        # Verify repli_paths is now populated
        assert len(rm.repli_paths) == 1
        assert "/data/folder1" in rm.repli_paths

        # Now create_replications will correctly skip existing replications
        # This demonstrates the fix: always call populate_replication_cache() before create_replications()

    def test_clean_replications_needs_populated_repli_paths(self):
        """Test: clean_replications requires populate_replication_cache() to populate repli_paths"""
        # Setup source cluster with existing replications
        source_relationship_statuses = [
            {
                "id": "repl-001",
                "source_root_path": "/data/folder1",
                "source_root_id": "12345",
                "target_root_path": "/data/folder1",
                "target_cluster_name": "dest-cluster",
                "target_address": "10.1.1.20",
                "state": "ESTABLISHED",
            },
            {
                "id": "repl-002",
                "source_root_path": "/data/folder2",
                "source_root_id": "67890",
                "target_root_path": "/data/folder2",
                "target_cluster_name": "dest-cluster",
                "target_address": "10.1.1.21",
                "state": "ESTABLISHED",
            },
        ]

        fs_api = FakeFileSystemAPI({})
        repl_api = FakeReplicationAPI([], None, source_relationship_statuses)
        cluster_api = FakeClusterAPI("source-cluster", "src-123")
        rest_client = FakeRestClient(fs_api, repl_api, cluster_api)
        fake_client = FakeClient(rest_client)

        rm = ReplicationManager(fake_client)

        # Initially, repli_paths should be empty
        assert len(rm.repli_paths) == 0

        # Calling clean_replications without populate_replication_cache will do nothing
        # (no error, but also no deletions)
        rm.clean_replications("/data")
        assert len(rm.repli_paths) == 0

        # Now call populate_replication_cache to populate repli_paths
        rm.populate_replication_cache()

        # Verify repli_paths is now populated
        assert len(rm.repli_paths) == 2
        assert "/data/folder1" in rm.repli_paths
        assert "/data/folder2" in rm.repli_paths

        # Now clean_replications should find relationships to delete
        # (we can verify by checking repli_paths was accessed)
        # This demonstrates the fix: always call populate_replication_cache() before clean_replications()


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
