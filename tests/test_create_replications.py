#!/usr/bin/env python3
"""
Tests for create_replications with dst_path argument - TDD approach
"""

import pytest
import logging
from typing import List, Dict, Any
from replication import ReplicationManager, TargetCluster

logger = logging.getLogger(__name__)


class FakeTreeWalkEntry:
    """Represents a directory entry from tree_walk_preorder"""

    def __init__(self, path: str, entry_type: str = "FS_FILE_TYPE_DIRECTORY"):
        self.data = {"path": path, "type": entry_type}

    def get(self, key: str):
        return self.data.get(key)


class FakeFileSystemAPI:
    """Fake file system API for testing"""

    def __init__(self, directories: List[str]):
        """
        Args:
            directories: List of directory paths to simulate
        """
        self.directories = directories

    def tree_walk_preorder(self, path: str, max_depth: int = 1) -> List[Dict[str, str]]:
        """Simulate tree walk - returns base dir first, then subdirs"""
        results = []
        # First entry is always the base path itself
        results.append({"path": path, "type": "FS_FILE_TYPE_DIRECTORY"})

        # Then add subdirectories
        for dir_path in self.directories:
            if dir_path.startswith(path) and dir_path != path:
                results.append({"path": dir_path, "type": "FS_FILE_TYPE_DIRECTORY"})

        return results


class FakeReplicationAPI:
    """Fake replication API for testing"""

    def __init__(self):
        self.created_replications = []
        self.source_relationship_statuses = []
        self.deleted_replications = []

    def create_source_relationship(
        self, address: str, source_path: str, target_path: str
    ) -> Dict[str, Any]:
        """Simulate creating a replication relationship"""
        repl_id = f"repl-{len(self.created_replications) + 1:03d}"
        replication = {
            "id": repl_id,
            "address": address,
            "source_path": source_path,
            "target_path": target_path,
        }
        self.created_replications.append(replication)
        return replication

    def delete_source_relationship(self, replication_id: str):
        """Simulate deleting a replication relationship"""
        self.deleted_replications.append(replication_id)

    def list_source_relationship_statuses(self) -> List[Dict[str, Any]]:
        """Return existing source relationship statuses"""
        return self.source_relationship_statuses


class FakeClusterAPI:
    """Fake cluster API"""

    def __init__(self, cluster_name: str = "test-cluster"):
        self.cluster_name = cluster_name

    def get_cluster_conf(self) -> Dict[str, str]:
        return {"cluster_name": self.cluster_name}


class FakeNetworkAPI:
    """Fake network API for TargetCluster - matches list_network_status_v2() structure"""

    def __init__(self, networks: List[Dict[str, Any]]):
        """
        Args:
            networks: List of network configs, e.g.:
                [{"name": "Default", "floating_addresses": ["10.1.1.20"]}]
        """
        self.networks = networks

    def list_network_status_v2(self) -> List[Dict[str, Any]]:
        """Return fake network status matching real API structure"""
        return [{
            "node_id": 1,
            "node_name": "fake-node-1",
            "network_statuses": self.networks
        }]


class FakeRestClient:
    """Fake RestClient"""

    def __init__(
        self,
        fs_api: FakeFileSystemAPI,
        repl_api: FakeReplicationAPI,
        cluster_api: FakeClusterAPI = None,
        network_api: FakeNetworkAPI = None,
    ):
        self.fs = fs_api
        self.replication = repl_api
        self.cluster = cluster_api or FakeClusterAPI()
        self.network = network_api or FakeNetworkAPI([{
            "name": "Default",
            "floating_addresses": ["10.1.1.20", "10.1.1.21"]
        }])


class FakeClient:
    """Fake Client wrapper"""

    def __init__(self, rest_client: FakeRestClient):
        self.rc = rest_client


class TestDstPathNotProvided:
    """Test dst_path when not provided (None or empty string)"""

    def test_dst_path_none_creates_replications_with_same_path(self):
        """
        Test: When dst_path is None, destination path should match source path
        AC: Source /data/project → Destination /data/project
        """
        # Setup
        directories = ["/data/project1", "/data/project2"]
        fs_api = FakeFileSystemAPI(directories)
        repl_api = FakeReplicationAPI()
        network_api = FakeNetworkAPI([{"name": "Default", "floating_addresses": ["10.1.1.20"]}])
        rest_client = FakeRestClient(fs_api, repl_api, network_api=network_api)
        fake_client = FakeClient(rest_client)

        # Create managers
        rm = ReplicationManager(fake_client)
        target_cluster = TargetCluster(fake_client, user_provided_ips=["10.1.1.20"], network_name="Default")

        # Execute with dst_path=None
        rm.create_replications(basepath="/data", dst=target_cluster, dst_path=None)

        # Verify
        created = repl_api.created_replications
        assert len(created) == 2
        assert created[0]["source_path"] == "/data/project1"
        assert created[0]["target_path"] == "/data/project1"  # Same as source
        assert created[1]["source_path"] == "/data/project2"
        assert created[1]["target_path"] == "/data/project2"  # Same as source

    def test_dst_path_empty_string_creates_replications_with_same_path(self):
        """
        Test: When dst_path is empty string "", destination path should match source path
        AC: Source /data/project → Destination /data/project
        """
        # Setup
        directories = ["/data/project1"]
        fs_api = FakeFileSystemAPI(directories)
        repl_api = FakeReplicationAPI()
        network_api = FakeNetworkAPI([{"name": "Default", "floating_addresses": ["10.1.1.20"]}])
        rest_client = FakeRestClient(fs_api, repl_api, network_api=network_api)
        fake_client = FakeClient(rest_client)

        # Create managers
        rm = ReplicationManager(fake_client)
        target_cluster = TargetCluster(fake_client, user_provided_ips=["10.1.1.20"], network_name="Default")

        # Execute with dst_path=""
        rm.create_replications(basepath="/data", dst=target_cluster, dst_path="")

        # Verify
        created = repl_api.created_replications
        assert len(created) == 1
        assert created[0]["source_path"] == "/data/project1"
        assert created[0]["target_path"] == "/data/project1"  # Same as source


class TestDstPathSlash:
    """Test dst_path when set to / (root)"""

    def test_dst_path_slash_creates_replications_with_same_path(self):
        """
        Test: When dst_path is "/", destination path should match source path
        AC: Source /data/project with dst_path="/" → Destination /data/project
        Real use case: User sets "/" thinking it's the default, should work same as not provided
        """
        # Setup
        directories = ["/data/project1", "/data/project2"]
        fs_api = FakeFileSystemAPI(directories)
        repl_api = FakeReplicationAPI()
        network_api = FakeNetworkAPI([{"name": "Default", "floating_addresses": ["10.1.1.20"]}])
        rest_client = FakeRestClient(fs_api, repl_api, network_api=network_api)
        fake_client = FakeClient(rest_client)

        # Create managers
        rm = ReplicationManager(fake_client)
        target_cluster = TargetCluster(fake_client, user_provided_ips=["10.1.1.20"], network_name="Default")

        # Execute with dst_path="/"
        rm.create_replications(basepath="/data", dst=target_cluster, dst_path="/")

        # Verify
        created = repl_api.created_replications
        assert len(created) == 2
        assert created[0]["source_path"] == "/data/project1"
        assert (
            created[0]["target_path"] == "/data/project1"
        )  # Same as source (no prepend)
        assert created[1]["source_path"] == "/data/project2"
        assert (
            created[1]["target_path"] == "/data/project2"
        )  # Same as source (no prepend)


class TestDstPathCustomPath:
    """Test dst_path with custom paths"""

    def test_dst_path_custom_prepends_to_destination(self):
        """
        Test: When dst_path is "/backup", destination path should be prepended
        AC: Source /data/project with dst_path="/backup" → Destination /backup/data/project
        Real use case: Replicate to backup directory structure
        """
        # Setup
        directories = ["/data/project1", "/data/project2"]
        fs_api = FakeFileSystemAPI(directories)
        repl_api = FakeReplicationAPI()
        network_api = FakeNetworkAPI([{"name": "Default", "floating_addresses": ["10.1.1.20"]}])
        rest_client = FakeRestClient(fs_api, repl_api, network_api=network_api)
        fake_client = FakeClient(rest_client)

        # Create managers
        rm = ReplicationManager(fake_client)
        target_cluster = TargetCluster(fake_client, user_provided_ips=["10.1.1.20"], network_name="Default")

        # Execute with dst_path="/backup"
        rm.create_replications(basepath="/data", dst=target_cluster, dst_path="/backup")

        # Verify
        created = repl_api.created_replications
        assert len(created) == 2
        assert created[0]["source_path"] == "/data/project1"
        assert created[0]["target_path"] == "/backup/data/project1"  # Prepended
        assert created[1]["source_path"] == "/data/project2"
        assert created[1]["target_path"] == "/backup/data/project2"  # Prepended

    def test_dst_path_without_leading_slash_prepends_correctly(self):
        """
        Test: When dst_path is "backup" (no leading slash), it should still prepend
        AC: Source /data/project with dst_path="backup" → Destination backup/data/project
        """
        # Setup
        directories = ["/data/project1"]
        fs_api = FakeFileSystemAPI(directories)
        repl_api = FakeReplicationAPI()
        network_api = FakeNetworkAPI([{"name": "Default", "floating_addresses": ["10.1.1.20"]}])
        rest_client = FakeRestClient(fs_api, repl_api, network_api=network_api)
        fake_client = FakeClient(rest_client)

        # Create managers
        rm = ReplicationManager(fake_client)
        target_cluster = TargetCluster(fake_client, user_provided_ips=["10.1.1.20"], network_name="Default")

        # Execute with dst_path="backup"
        rm.create_replications(basepath="/data", dst=target_cluster, dst_path="backup")

        # Verify
        created = repl_api.created_replications
        assert len(created) == 1
        assert created[0]["source_path"] == "/data/project1"
        assert created[0]["target_path"] == "backup/data/project1"  # Prepended

    def test_dst_path_nested_path_prepends_correctly(self):
        """
        Test: When dst_path is "/dr/backups", it should prepend nested path
        AC: Source /prod/db with dst_path="/dr/backups" → Destination /dr/backups/prod/db
        Real use case: Disaster recovery with specific backup hierarchy
        """
        # Setup
        directories = ["/prod/db1", "/prod/db2"]
        fs_api = FakeFileSystemAPI(directories)
        repl_api = FakeReplicationAPI()
        network_api = FakeNetworkAPI([{"name": "Default", "floating_addresses": ["10.1.1.20"]}])
        rest_client = FakeRestClient(fs_api, repl_api, network_api=network_api)
        fake_client = FakeClient(rest_client)

        # Create managers
        rm = ReplicationManager(fake_client)
        target_cluster = TargetCluster(fake_client, user_provided_ips=["10.1.1.20"], network_name="Default")

        # Execute with dst_path="/dr/backups"
        rm.create_replications(
            basepath="/prod", dst=target_cluster, dst_path="/dr/backups"
        )

        # Verify
        created = repl_api.created_replications
        assert len(created) == 2
        assert created[0]["source_path"] == "/prod/db1"
        assert created[0]["target_path"] == "/dr/backups/prod/db1"  # Nested prepend
        assert created[1]["source_path"] == "/prod/db2"
        assert created[1]["target_path"] == "/dr/backups/prod/db2"  # Nested prepend


class TestDstPathWithExistingReplications:
    """Test dst_path doesn't create duplicates when replications exist"""

    def test_dst_path_skips_existing_replications(self):
        """
        Test: When replication already exists, it should skip even with dst_path
        AC: Existing replication is not duplicated
        """
        # Setup with existing replication
        directories = ["/data/project1", "/data/project2"]
        fs_api = FakeFileSystemAPI(directories)
        repl_api = FakeReplicationAPI()

        # Add existing replication status
        repl_api.source_relationship_statuses = [
            {
                "id": "existing-001",
                "source_root_path": "/data/project1",
                "source_root_id": "12345",
                "target_root_path": "/backup/data/project1",
                "target_address": "10.1.1.20",
                "state": "ESTABLISHED",
            }
        ]

        network_api = FakeNetworkAPI([{"name": "Default", "floating_addresses": ["10.1.1.20"]}])
        rest_client = FakeRestClient(fs_api, repl_api, network_api=network_api)
        fake_client = FakeClient(rest_client)

        # Create managers
        rm = ReplicationManager(fake_client)
        target_cluster = TargetCluster(fake_client, user_provided_ips=["10.1.1.20"], network_name="Default")

        # Populate cache to load existing replications
        rm.populate_replication_cache()

        # Execute with dst_path="/backup"
        rm.create_replications(basepath="/data", dst=target_cluster, dst_path="/backup")

        # Verify - should only create project2, not project1 (already exists)
        created = repl_api.created_replications
        assert len(created) == 1
        assert created[0]["source_path"] == "/data/project2"
        assert created[0]["target_path"] == "/backup/data/project2"


class TestFilterInclude:
    """Test --filteri (include filter) functionality"""

    def test_filteri_single_pattern_includes_only_matching_dirs(self):
        """
        Test: --filteri with single pattern includes only matching directories
        AC: Only directories containing "prod" are replicated
        Real use case: Replicate only production directories
        """
        # Setup with multiple directories
        directories = [
            "/data/prod-db1",
            "/data/prod-db2",
            "/data/test-db1",
            "/data/staging-db",
        ]
        fs_api = FakeFileSystemAPI(directories)
        repl_api = FakeReplicationAPI()
        network_api = FakeNetworkAPI([{"name": "Default", "floating_addresses": ["10.1.1.20"]}])
        rest_client = FakeRestClient(fs_api, repl_api, network_api=network_api)
        fake_client = FakeClient(rest_client)

        # Create managers
        rm = ReplicationManager(fake_client)
        target_cluster = TargetCluster(fake_client, user_provided_ips=["10.1.1.20"], network_name="Default")

        # Execute with filteri="prod" (substring match)
        rm.create_replications(
            basepath="/data", dst=target_cluster, dst_path="", filteri=["prod"]
        )

        # Verify - only directories containing "prod" should be created
        created = repl_api.created_replications
        assert len(created) == 2
        assert created[0]["source_path"] == "/data/prod-db1"
        assert created[1]["source_path"] == "/data/prod-db2"

    def test_filteri_multiple_patterns_includes_all_matches(self):
        """
        Test: --filteri with multiple patterns includes directories matching ANY pattern
        AC: Directories containing "prod" OR "staging" are replicated
        Real use case: Replicate both production and staging environments
        """
        # Setup
        directories = [
            "/data/prod-db",
            "/data/staging-db",
            "/data/test-db",
            "/data/dev-db",
        ]
        fs_api = FakeFileSystemAPI(directories)
        repl_api = FakeReplicationAPI()
        network_api = FakeNetworkAPI([{"name": "Default", "floating_addresses": ["10.1.1.20"]}])
        rest_client = FakeRestClient(fs_api, repl_api, network_api=network_api)
        fake_client = FakeClient(rest_client)

        # Create managers
        rm = ReplicationManager(fake_client)
        target_cluster = TargetCluster(fake_client, user_provided_ips=["10.1.1.20"], network_name="Default")

        # Execute with multiple patterns (substring matching)
        rm.create_replications(
            basepath="/data",
            dst=target_cluster,
            dst_path="",
            filteri=["prod", "staging"],
        )

        # Verify
        created = repl_api.created_replications
        assert len(created) == 2
        assert created[0]["source_path"] == "/data/prod-db"
        assert created[1]["source_path"] == "/data/staging-db"

    def test_filteri_no_matches_creates_nothing(self):
        """
        Test: --filteri with no matching directories creates no replications
        AC: If no directories match the pattern, no replications are created
        """
        # Setup
        directories = ["/data/test-db1", "/data/test-db2"]
        fs_api = FakeFileSystemAPI(directories)
        repl_api = FakeReplicationAPI()
        network_api = FakeNetworkAPI([{"name": "Default", "floating_addresses": ["10.1.1.20"]}])
        rest_client = FakeRestClient(fs_api, repl_api, network_api=network_api)
        fake_client = FakeClient(rest_client)

        # Create managers
        rm = ReplicationManager(fake_client)
        target_cluster = TargetCluster(fake_client, user_provided_ips=["10.1.1.20"], network_name="Default")

        # Execute with filteri that doesn't match
        rm.create_replications(
            basepath="/data", dst=target_cluster, dst_path="", filteri=["prod-*"]
        )

        # Verify - nothing created
        created = repl_api.created_replications
        assert len(created) == 0

    def test_filteri_handles_trailing_slashes_in_paths(self):
        """
        Test: --filteri correctly handles paths with trailing slashes (real API behavior)
        AC: Paths like '/snapz/tessdasd/' should match substring 'tess'
        This is a regression test for the bug where trailing slashes caused filters to fail
        """
        # Setup - simulate real API returning paths WITH trailing slashes
        directories = ["/snapz/tessdasd/", "/snapz/other/"]
        fs_api = FakeFileSystemAPI(directories)
        repl_api = FakeReplicationAPI()
        network_api = FakeNetworkAPI([{"name": "Default", "floating_addresses": ["10.1.1.20"]}])
        rest_client = FakeRestClient(fs_api, repl_api, network_api=network_api)
        fake_client = FakeClient(rest_client)

        # Create managers
        rm = ReplicationManager(fake_client)
        target_cluster = TargetCluster(fake_client, user_provided_ips=["10.1.1.20"], network_name="Default")

        # Execute with filteri pattern 'tess' (substring match)
        rm.create_replications(
            basepath="/snapz", dst=target_cluster, dst_path="", filteri=["tess"]
        )

        # Verify - only tessdasd should match (even with trailing slash)
        created = repl_api.created_replications
        assert len(created) == 1
        assert created[0]["source_path"] == "/snapz/tessdasd/"


class TestFilterExclude:
    """Test --filtere (exclude filter) functionality"""

    def test_filtere_single_pattern_excludes_matching_dirs(self):
        """
        Test: --filtere with single pattern excludes matching directories
        AC: Directories containing "test" are excluded from replication
        Real use case: Exclude test directories from replication
        """
        # Setup
        directories = [
            "/data/prod-db1",
            "/data/prod-db2",
            "/data/test-db1",
            "/data/test-db2",
        ]
        fs_api = FakeFileSystemAPI(directories)
        repl_api = FakeReplicationAPI()
        network_api = FakeNetworkAPI([{"name": "Default", "floating_addresses": ["10.1.1.20"]}])
        rest_client = FakeRestClient(fs_api, repl_api, network_api=network_api)
        fake_client = FakeClient(rest_client)

        # Create managers
        rm = ReplicationManager(fake_client)
        target_cluster = TargetCluster(fake_client, user_provided_ips=["10.1.1.20"], network_name="Default")

        # Execute with filtere="test" (substring match)
        rm.create_replications(
            basepath="/data", dst=target_cluster, dst_path="", filtere=["test"]
        )

        # Verify - directories containing "test" should be excluded
        created = repl_api.created_replications
        assert len(created) == 2
        assert created[0]["source_path"] == "/data/prod-db1"
        assert created[1]["source_path"] == "/data/prod-db2"

    def test_filtere_multiple_patterns_excludes_all_matches(self):
        """
        Test: --filtere with multiple patterns excludes directories matching ANY pattern
        AC: Directories containing "test" OR "temp" are excluded
        Real use case: Exclude both test and temporary directories
        """
        # Setup
        directories = [
            "/data/prod-db",
            "/data/test-db",
            "/data/temp-cache",
            "/data/staging-db",
        ]
        fs_api = FakeFileSystemAPI(directories)
        repl_api = FakeReplicationAPI()
        network_api = FakeNetworkAPI([{"name": "Default", "floating_addresses": ["10.1.1.20"]}])
        rest_client = FakeRestClient(fs_api, repl_api, network_api=network_api)
        fake_client = FakeClient(rest_client)

        # Create managers
        rm = ReplicationManager(fake_client)
        target_cluster = TargetCluster(fake_client, user_provided_ips=["10.1.1.20"], network_name="Default")

        # Execute with multiple exclude patterns (substring matching)
        rm.create_replications(
            basepath="/data", dst=target_cluster, dst_path="", filtere=["test", "temp"]
        )

        # Verify
        created = repl_api.created_replications
        assert len(created) == 2
        assert created[0]["source_path"] == "/data/prod-db"
        assert created[1]["source_path"] == "/data/staging-db"

    def test_filtere_all_excluded_creates_nothing(self):
        """
        Test: --filtere that matches all directories creates no replications
        AC: If all directories are excluded, no replications are created
        """
        # Setup
        directories = ["/data/test-db1", "/data/test-db2"]
        fs_api = FakeFileSystemAPI(directories)
        repl_api = FakeReplicationAPI()
        network_api = FakeNetworkAPI([{"name": "Default", "floating_addresses": ["10.1.1.20"]}])
        rest_client = FakeRestClient(fs_api, repl_api, network_api=network_api)
        fake_client = FakeClient(rest_client)

        # Create managers
        rm = ReplicationManager(fake_client)
        target_cluster = TargetCluster(fake_client, user_provided_ips=["10.1.1.20"], network_name="Default")

        # Execute with filtere that matches everything (substring match)
        rm.create_replications(
            basepath="/data", dst=target_cluster, dst_path="", filtere=["test"]
        )

        # Verify - nothing created
        created = repl_api.created_replications
        assert len(created) == 0


class TestFilterCombinations:
    """Test combinations of filters with dst_path and other features"""

    def test_filteri_with_dst_path_works_correctly(self):
        """
        Test: --filteri combined with --dst_path works correctly
        AC: Only matching directories are replicated with prepended path
        Real use case: Replicate prod directories to backup location
        """
        # Setup
        directories = ["/data/prod-db1", "/data/test-db1"]
        fs_api = FakeFileSystemAPI(directories)
        repl_api = FakeReplicationAPI()
        network_api = FakeNetworkAPI([{"name": "Default", "floating_addresses": ["10.1.1.20"]}])
        rest_client = FakeRestClient(fs_api, repl_api, network_api=network_api)
        fake_client = FakeClient(rest_client)

        # Create managers
        rm = ReplicationManager(fake_client)
        target_cluster = TargetCluster(fake_client, user_provided_ips=["10.1.1.20"], network_name="Default")

        # Execute with both filteri and dst_path (substring matching)
        rm.create_replications(
            basepath="/data", dst=target_cluster, dst_path="/backup", filteri=["prod"]
        )

        # Verify
        created = repl_api.created_replications
        assert len(created) == 1
        assert created[0]["source_path"] == "/data/prod-db1"
        assert created[0]["target_path"] == "/backup/data/prod-db1"

    def test_filtere_with_dst_path_works_correctly(self):
        """
        Test: --filtere combined with --dst_path works correctly
        AC: Non-excluded directories are replicated with prepended path
        """
        # Setup
        directories = ["/data/prod-db1", "/data/test-db1"]
        fs_api = FakeFileSystemAPI(directories)
        repl_api = FakeReplicationAPI()
        network_api = FakeNetworkAPI([{"name": "Default", "floating_addresses": ["10.1.1.20"]}])
        rest_client = FakeRestClient(fs_api, repl_api, network_api=network_api)
        fake_client = FakeClient(rest_client)

        # Create managers
        rm = ReplicationManager(fake_client)
        target_cluster = TargetCluster(fake_client, user_provided_ips=["10.1.1.20"], network_name="Default")

        # Execute with both filtere and dst_path (substring matching)
        rm.create_replications(
            basepath="/data", dst=target_cluster, dst_path="/backup", filtere=["test"]
        )

        # Verify
        created = repl_api.created_replications
        assert len(created) == 1
        assert created[0]["source_path"] == "/data/prod-db1"
        assert created[0]["target_path"] == "/backup/data/prod-db1"

    def test_no_filters_replicates_all_directories(self):
        """
        Test: When no filters are provided, all directories are replicated
        AC: All subdirectories under basepath are replicated
        """
        # Setup
        directories = ["/data/prod-db", "/data/test-db", "/data/staging-db"]
        fs_api = FakeFileSystemAPI(directories)
        repl_api = FakeReplicationAPI()
        network_api = FakeNetworkAPI([{"name": "Default", "floating_addresses": ["10.1.1.20"]}])
        rest_client = FakeRestClient(fs_api, repl_api, network_api=network_api)
        fake_client = FakeClient(rest_client)

        # Create managers
        rm = ReplicationManager(fake_client)
        target_cluster = TargetCluster(fake_client, user_provided_ips=["10.1.1.20"], network_name="Default")

        # Execute without any filters
        rm.create_replications(
            basepath="/data",
            dst=target_cluster,
            dst_path="",
            filteri=None,
            filtere=None,
        )

        # Verify - all directories replicated
        created = repl_api.created_replications
        assert len(created) == 3


class TestCleanWithFilters:
    """Test clean_replications with filter support"""

    def test_clean_with_filtere_skips_excluded_dirs(self):
        """
        Test: clean with --filtere excludes matching directories from deletion
        AC: Directories containing "foo" should NOT be deleted
        Real use case: Clean all replications except production (exclude prod)
        """
        # Setup - create some replications first
        directories = ["/data/prod-db", "/data/test-db", "/data/foo-app"]
        fs_api = FakeFileSystemAPI(directories)
        repl_api = FakeReplicationAPI()

        # Add existing replications to simulate what would be cleaned
        repl_api.source_relationship_statuses = [
            {
                "id": "repl-001",
                "source_root_path": "/data/prod-db",
                "source_root_id": "1",
                "target_address": "10.1.1.20",
            },
            {
                "id": "repl-002",
                "source_root_path": "/data/test-db",
                "source_root_id": "2",
                "target_address": "10.1.1.20",
            },
            {
                "id": "repl-003",
                "source_root_path": "/data/foo-app",
                "source_root_id": "3",
                "target_address": "10.1.1.20",
            },
        ]

        network_api = FakeNetworkAPI([{"name": "Default", "floating_addresses": ["10.1.1.20"]}])
        rest_client = FakeRestClient(fs_api, repl_api, network_api=network_api)
        fake_client = FakeClient(rest_client)

        # Create manager and populate cache
        rm = ReplicationManager(fake_client)
        rm.populate_replication_cache()

        # Execute clean with filtere="foo" - should skip foo-app
        deleted_count = rm.clean_replications(basepath="/data", filtere=["foo"])

        # Verify - should delete 2 (prod-db and test-db), skip foo-app
        assert deleted_count == 2

    def test_clean_with_filteri_only_deletes_matching_dirs(self):
        """
        Test: clean with --filteri only deletes matching directories
        AC: Only directories containing "test" should be deleted
        Real use case: Clean only test replications, keep prod
        """
        # Setup
        directories = ["/data/prod-db", "/data/test-db1", "/data/test-db2"]
        fs_api = FakeFileSystemAPI(directories)
        repl_api = FakeReplicationAPI()

        # Add existing replications
        repl_api.source_relationship_statuses = [
            {
                "id": "repl-001",
                "source_root_path": "/data/prod-db",
                "source_root_id": "1",
                "target_address": "10.1.1.20",
            },
            {
                "id": "repl-002",
                "source_root_path": "/data/test-db1",
                "source_root_id": "2",
                "target_address": "10.1.1.20",
            },
            {
                "id": "repl-003",
                "source_root_path": "/data/test-db2",
                "source_root_id": "3",
                "target_address": "10.1.1.20",
            },
        ]

        network_api = FakeNetworkAPI([{"name": "Default", "floating_addresses": ["10.1.1.20"]}])
        rest_client = FakeRestClient(fs_api, repl_api, network_api=network_api)
        fake_client = FakeClient(rest_client)

        # Create manager and populate cache
        rm = ReplicationManager(fake_client)
        rm.populate_replication_cache()

        # Execute clean with filteri="test" - should only delete test dirs
        deleted_count = rm.clean_replications(basepath="/data", filteri=["test"])

        # Verify - should delete only 2 test dirs, skip prod
        assert deleted_count == 2

    def test_clean_without_filters_deletes_all(self):
        """
        Test: clean without filters deletes all replications under basepath
        AC: All replications should be deleted when no filters specified
        """
        # Setup
        directories = ["/data/prod-db", "/data/test-db", "/data/foo-app"]
        fs_api = FakeFileSystemAPI(directories)
        repl_api = FakeReplicationAPI()

        # Add existing replications
        repl_api.source_relationship_statuses = [
            {
                "id": "repl-001",
                "source_root_path": "/data/prod-db",
                "source_root_id": "1",
                "target_address": "10.1.1.20",
            },
            {
                "id": "repl-002",
                "source_root_path": "/data/test-db",
                "source_root_id": "2",
                "target_address": "10.1.1.20",
            },
            {
                "id": "repl-003",
                "source_root_path": "/data/foo-app",
                "source_root_id": "3",
                "target_address": "10.1.1.20",
            },
        ]

        network_api = FakeNetworkAPI([{"name": "Default", "floating_addresses": ["10.1.1.20"]}])
        rest_client = FakeRestClient(fs_api, repl_api, network_api=network_api)
        fake_client = FakeClient(rest_client)

        # Create manager and populate cache
        rm = ReplicationManager(fake_client)
        rm.populate_replication_cache()

        # Execute clean without filters - should delete all
        deleted_count = rm.clean_replications(basepath="/data")

        # Verify - should delete all 3
        assert deleted_count == 3


class TestNetworkNameSingleNetwork:
    """Test network name resolution with single network"""

    def test_default_network_name_finds_default_network(self):
        """
        Test: Default network_name="Default" finds the Default network
        AC: Uses floating IPs from "Default" network
        Real use case: Most common scenario - single Default network
        """
        # Setup with single "Default" network
        fs_api = FakeFileSystemAPI([])
        repl_api = FakeReplicationAPI()
        network_api = FakeNetworkAPI([{
            "name": "Default",
            "floating_addresses": ["10.1.1.20", "10.1.1.21"]
        }])
        rest_client = FakeRestClient(fs_api, repl_api, network_api=network_api)
        fake_client = FakeClient(rest_client)

        # Create TargetCluster with default network_name="Default"
        target_cluster = TargetCluster(fake_client, network_name="Default")

        # Verify IPs are from Default network
        assert target_cluster.available_ips == ["10.1.1.20", "10.1.1.21"]

    def test_custom_network_name_finds_network(self):
        """
        Test: Custom network_name finds the correct network
        AC: network_name="production" finds and uses "production" network
        """
        # Setup with custom network name
        fs_api = FakeFileSystemAPI([])
        repl_api = FakeReplicationAPI()
        network_api = FakeNetworkAPI([{
            "name": "production",
            "floating_addresses": ["10.2.2.30", "10.2.2.31"]
        }])
        rest_client = FakeRestClient(fs_api, repl_api, network_api=network_api)
        fake_client = FakeClient(rest_client)

        # Create TargetCluster with custom network name
        target_cluster = TargetCluster(fake_client, network_name="production")

        # Verify IPs are from production network
        assert target_cluster.available_ips == ["10.2.2.30", "10.2.2.31"]

    def test_network_name_not_found_raises_error(self):
        """
        Test: Nonexistent network_name raises ValueError
        AC: network_name="nonexistent" when only "Default" exists raises clear error
        """
        # Setup with only Default network
        fs_api = FakeFileSystemAPI([])
        repl_api = FakeReplicationAPI()
        network_api = FakeNetworkAPI([{
            "name": "Default",
            "floating_addresses": ["10.1.1.20"]
        }])
        rest_client = FakeRestClient(fs_api, repl_api, network_api=network_api)
        fake_client = FakeClient(rest_client)

        # Try to create with nonexistent network name
        with pytest.raises(ValueError) as exc_info:
            TargetCluster(fake_client, network_name="nonexistent")

        # Verify error mentions the network name
        assert "nonexistent" in str(exc_info.value).lower()

    def test_error_message_lists_available_networks(self):
        """
        Test: Error message lists available networks for user guidance
        AC: Error shows "Available networks: Default, test" when network not found
        """
        # Setup with Default network
        fs_api = FakeFileSystemAPI([])
        repl_api = FakeReplicationAPI()
        network_api = FakeNetworkAPI([{
            "name": "Default",
            "floating_addresses": ["10.1.1.20"]
        }])
        rest_client = FakeRestClient(fs_api, repl_api, network_api=network_api)
        fake_client = FakeClient(rest_client)

        # Try to create with wrong network name
        with pytest.raises(ValueError) as exc_info:
            TargetCluster(fake_client, network_name="wrongname")

        # Verify error lists available networks
        error_msg = str(exc_info.value)
        assert "available" in error_msg.lower()
        assert "Default" in error_msg


class TestNetworkNameMultipleNetworks:
    """Test network name resolution with multiple networks"""

    def test_two_networks_default_name_uses_default(self):
        """
        Test: With 2 networks, network_name="Default" uses Default network
        AC: Correctly identifies and uses "Default" network among multiple
        Real use case: Cluster with production + test networks
        """
        # Setup with two networks
        fs_api = FakeFileSystemAPI([])
        repl_api = FakeReplicationAPI()
        network_api = FakeNetworkAPI([
            {
                "name": "Default",
                "floating_addresses": ["10.120.3.54"]
            },
            {
                "name": "test",
                "floating_addresses": ["10.120.3.55"]
            }
        ])
        rest_client = FakeRestClient(fs_api, repl_api, network_api=network_api)
        fake_client = FakeClient(rest_client)

        # Create with Default network
        target_cluster = TargetCluster(fake_client, network_name="Default")

        # Verify uses Default network's IP, not test network
        assert target_cluster.available_ips == ["10.120.3.54"]
        assert "10.120.3.55" not in target_cluster.available_ips

    def test_two_networks_custom_name_uses_correct_one(self):
        """
        Test: With 2 networks, network_name="test" uses test network
        AC: Finds and uses "test" network correctly
        """
        # Setup with two networks
        fs_api = FakeFileSystemAPI([])
        repl_api = FakeReplicationAPI()
        network_api = FakeNetworkAPI([
            {
                "name": "Default",
                "floating_addresses": ["10.120.3.54"]
            },
            {
                "name": "test",
                "floating_addresses": ["10.120.3.55", "10.120.3.56"]
            }
        ])
        rest_client = FakeRestClient(fs_api, repl_api, network_api=network_api)
        fake_client = FakeClient(rest_client)

        # Create with test network
        target_cluster = TargetCluster(fake_client, network_name="test")

        # Verify uses test network's IPs
        assert target_cluster.available_ips == ["10.120.3.55", "10.120.3.56"]
        assert "10.120.3.54" not in target_cluster.available_ips

    def test_multiple_nodes_collects_fips_from_all(self):
        """
        Test: Multi-node cluster collects FIPs from all nodes
        AC: With 3 nodes (each with 1 FIP), collects all 3 FIPs
        Real use case: 4-node cluster with floating IPs distributed across nodes
        """
        # Setup custom network API that returns multiple nodes
        class MultiNodeNetworkAPI:
            def __init__(self):
                pass

            def list_network_status_v2(self):
                return [
                    {
                        "node_id": 1,
                        "node_name": "node-1",
                        "network_statuses": [{
                            "name": "Default",
                            "floating_addresses": ["10.1.1.20"]
                        }]
                    },
                    {
                        "node_id": 2,
                        "node_name": "node-2",
                        "network_statuses": [{
                            "name": "Default",
                            "floating_addresses": ["10.1.1.21"]
                        }]
                    },
                    {
                        "node_id": 3,
                        "node_name": "node-3",
                        "network_statuses": [{
                            "name": "Default",
                            "floating_addresses": ["10.1.1.22"]
                        }]
                    }
                ]

        fs_api = FakeFileSystemAPI([])
        repl_api = FakeReplicationAPI()
        rest_client = FakeRestClient(fs_api, repl_api, network_api=MultiNodeNetworkAPI())
        fake_client = FakeClient(rest_client)

        # Create target cluster
        target_cluster = TargetCluster(fake_client, network_name="Default")

        # Verify collects FIPs from all nodes (3 nodes × 1 FIP each = 3 total)
        assert len(target_cluster.available_ips) == 3
        assert "10.1.1.20" in target_cluster.available_ips
        assert "10.1.1.21" in target_cluster.available_ips
        assert "10.1.1.22" in target_cluster.available_ips

    def test_network_with_no_fips_skipped(self):
        """
        Test: Network with empty floating_addresses is skipped
        AC: Node with no FIPs doesn't add to available_ips list
        Real use case: Network configured but FIPs not yet assigned
        """
        # Setup with one node having FIPs, one without
        class MixedFIPNetworkAPI:
            def list_network_status_v2(self):
                return [
                    {
                        "node_id": 1,
                        "node_name": "node-1",
                        "network_statuses": [{
                            "name": "Default",
                            "floating_addresses": ["10.1.1.20"]
                        }]
                    },
                    {
                        "node_id": 2,
                        "node_name": "node-2",
                        "network_statuses": [{
                            "name": "Default",
                            "floating_addresses": []  # Empty!
                        }]
                    }
                ]

        fs_api = FakeFileSystemAPI([])
        repl_api = FakeReplicationAPI()
        rest_client = FakeRestClient(fs_api, repl_api, network_api=MixedFIPNetworkAPI())
        fake_client = FakeClient(rest_client)

        # Create target cluster
        target_cluster = TargetCluster(fake_client, network_name="Default")

        # Verify only node-1's FIP is collected
        assert target_cluster.available_ips == ["10.1.1.20"]

    def test_multiple_nodes_with_multiple_fips_each(self):
        """
        Test: Multiple nodes each with multiple FIPs - collects ALL FIPs
        AC: Node1 has 3 FIPs + Node2 has 2 FIPs → returns all 5 FIPs
        Real use case: Large cluster with many FIPs distributed across nodes
        """
        # Setup multiple nodes, each with multiple FIPs
        class MultiNodeMultiFIPNetworkAPI:
            def list_network_status_v2(self):
                return [
                    {
                        "node_id": 1,
                        "node_name": "node-1",
                        "network_statuses": [{
                            "name": "Default",
                            "floating_addresses": ["10.1.1.20", "10.1.1.21", "10.1.1.22"]
                        }]
                    },
                    {
                        "node_id": 2,
                        "node_name": "node-2",
                        "network_statuses": [{
                            "name": "Default",
                            "floating_addresses": ["10.1.1.23", "10.1.1.24"]
                        }]
                    },
                    {
                        "node_id": 3,
                        "node_name": "node-3",
                        "network_statuses": [{
                            "name": "Default",
                            "floating_addresses": ["10.1.1.25"]
                        }]
                    }
                ]

        fs_api = FakeFileSystemAPI([])
        repl_api = FakeReplicationAPI()
        rest_client = FakeRestClient(fs_api, repl_api, network_api=MultiNodeMultiFIPNetworkAPI())
        fake_client = FakeClient(rest_client)

        # Create target cluster
        target_cluster = TargetCluster(fake_client, network_name="Default")

        # Verify collects ALL FIPs from all nodes (3 + 2 + 1 = 6 total)
        assert len(target_cluster.available_ips) == 6
        expected_ips = ["10.1.1.20", "10.1.1.21", "10.1.1.22", "10.1.1.23", "10.1.1.24", "10.1.1.25"]
        assert set(target_cluster.available_ips) == set(expected_ips)


class TestNetworkNameValidation:
    """Test network name validation and edge cases"""

    def test_user_provided_ips_skips_network_lookup(self):
        """
        Test: When user provides IPs, network_name is ignored
        AC: user_provided_ips=["10.1.1.50"] bypasses network discovery
        Real use case: User wants specific IPs regardless of network config
        """
        # Setup with Default network
        fs_api = FakeFileSystemAPI([])
        repl_api = FakeReplicationAPI()
        network_api = FakeNetworkAPI([{
            "name": "Default",
            "floating_addresses": ["10.1.1.20", "10.1.1.21"]
        }])
        rest_client = FakeRestClient(fs_api, repl_api, network_api=network_api)
        fake_client = FakeClient(rest_client)

        # User provides custom IPs
        target_cluster = TargetCluster(
            fake_client,
            user_provided_ips=["10.1.1.20"],  # Only one of the available IPs
            network_name="Default"
        )

        # Verify uses user-provided IP, not all from network
        assert target_cluster.available_ips == ["10.1.1.20"]

    def test_user_provided_ips_validated_against_all_networks(self):
        """
        Test: User-provided IPs are validated against cluster FIPs
        AC: IP not in any network raises ValueError
        """
        # Setup with Default network
        fs_api = FakeFileSystemAPI([])
        repl_api = FakeReplicationAPI()
        network_api = FakeNetworkAPI([{
            "name": "Default",
            "floating_addresses": ["10.1.1.20", "10.1.1.21"]
        }])
        rest_client = FakeRestClient(fs_api, repl_api, network_api=network_api)
        fake_client = FakeClient(rest_client)

        # User provides invalid IP
        with pytest.raises(ValueError) as exc_info:
            TargetCluster(
                fake_client,
                user_provided_ips=["192.168.1.99"],  # Not in cluster
                network_name="Default"
            )

        # Verify error mentions invalid IP
        assert "192.168.1.99" in str(exc_info.value)

    def test_network_name_case_sensitive(self):
        """
        Test: Network name matching is case-sensitive
        AC: "default" does not match "Default"
        """
        # Setup with "Default" network (capital D)
        fs_api = FakeFileSystemAPI([])
        repl_api = FakeReplicationAPI()
        network_api = FakeNetworkAPI([{
            "name": "Default",
            "floating_addresses": ["10.1.1.20"]
        }])
        rest_client = FakeRestClient(fs_api, repl_api, network_api=network_api)
        fake_client = FakeClient(rest_client)

        # Try with lowercase "default"
        with pytest.raises(ValueError) as exc_info:
            TargetCluster(fake_client, network_name="default")

        # Verify error - network not found
        assert "default" in str(exc_info.value).lower()
        assert "Default" in str(exc_info.value)  # Shows available with capital D


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
