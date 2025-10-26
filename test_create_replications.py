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
        self.data = {
            "path": path,
            "type": entry_type
        }

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

    def create_source_relationship(self, address: str, source_path: str, target_path: str) -> Dict[str, Any]:
        """Simulate creating a replication relationship"""
        repl_id = f"repl-{len(self.created_replications) + 1:03d}"
        replication = {
            "id": repl_id,
            "address": address,
            "source_path": source_path,
            "target_path": target_path
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
    """Fake network API for TargetCluster"""
    def __init__(self, floating_ips: List[str], network_id: int = 1):
        self.floating_ips = floating_ips
        self.network_id = network_id

    def get_floating_ip_allocation(self) -> List[Dict[str, Any]]:
        """Return fake floating IP allocation"""
        return [
            {
                "id": self.network_id,
                "floating_addresses": self.floating_ips
            }
        ]


class FakeRestClient:
    """Fake RestClient"""
    def __init__(self, fs_api: FakeFileSystemAPI, repl_api: FakeReplicationAPI,
                 cluster_api: FakeClusterAPI = None, network_api: FakeNetworkAPI = None):
        self.fs = fs_api
        self.replication = repl_api
        self.cluster = cluster_api or FakeClusterAPI()
        self.network = network_api or FakeNetworkAPI(["10.1.1.20", "10.1.1.21"])


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
        network_api = FakeNetworkAPI(["10.1.1.20"])
        rest_client = FakeRestClient(fs_api, repl_api, network_api=network_api)
        fake_client = FakeClient(rest_client)

        # Create managers
        rm = ReplicationManager(fake_client)
        target_cluster = TargetCluster(fake_client, user_provided_ips=["10.1.1.20"])

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
        network_api = FakeNetworkAPI(["10.1.1.20"])
        rest_client = FakeRestClient(fs_api, repl_api, network_api=network_api)
        fake_client = FakeClient(rest_client)

        # Create managers
        rm = ReplicationManager(fake_client)
        target_cluster = TargetCluster(fake_client, user_provided_ips=["10.1.1.20"])

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
        network_api = FakeNetworkAPI(["10.1.1.20"])
        rest_client = FakeRestClient(fs_api, repl_api, network_api=network_api)
        fake_client = FakeClient(rest_client)

        # Create managers
        rm = ReplicationManager(fake_client)
        target_cluster = TargetCluster(fake_client, user_provided_ips=["10.1.1.20"])

        # Execute with dst_path="/"
        rm.create_replications(basepath="/data", dst=target_cluster, dst_path="/")

        # Verify
        created = repl_api.created_replications
        assert len(created) == 2
        assert created[0]["source_path"] == "/data/project1"
        assert created[0]["target_path"] == "/data/project1"  # Same as source (no prepend)
        assert created[1]["source_path"] == "/data/project2"
        assert created[1]["target_path"] == "/data/project2"  # Same as source (no prepend)


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
        network_api = FakeNetworkAPI(["10.1.1.20"])
        rest_client = FakeRestClient(fs_api, repl_api, network_api=network_api)
        fake_client = FakeClient(rest_client)

        # Create managers
        rm = ReplicationManager(fake_client)
        target_cluster = TargetCluster(fake_client, user_provided_ips=["10.1.1.20"])

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
        network_api = FakeNetworkAPI(["10.1.1.20"])
        rest_client = FakeRestClient(fs_api, repl_api, network_api=network_api)
        fake_client = FakeClient(rest_client)

        # Create managers
        rm = ReplicationManager(fake_client)
        target_cluster = TargetCluster(fake_client, user_provided_ips=["10.1.1.20"])

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
        network_api = FakeNetworkAPI(["10.1.1.20"])
        rest_client = FakeRestClient(fs_api, repl_api, network_api=network_api)
        fake_client = FakeClient(rest_client)

        # Create managers
        rm = ReplicationManager(fake_client)
        target_cluster = TargetCluster(fake_client, user_provided_ips=["10.1.1.20"])

        # Execute with dst_path="/dr/backups"
        rm.create_replications(basepath="/prod", dst=target_cluster, dst_path="/dr/backups")

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
                "state": "ESTABLISHED"
            }
        ]

        network_api = FakeNetworkAPI(["10.1.1.20"])
        rest_client = FakeRestClient(fs_api, repl_api, network_api=network_api)
        fake_client = FakeClient(rest_client)

        # Create managers
        rm = ReplicationManager(fake_client)
        target_cluster = TargetCluster(fake_client, user_provided_ips=["10.1.1.20"])

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
        directories = ["/data/prod-db1", "/data/prod-db2", "/data/test-db1", "/data/staging-db"]
        fs_api = FakeFileSystemAPI(directories)
        repl_api = FakeReplicationAPI()
        network_api = FakeNetworkAPI(["10.1.1.20"])
        rest_client = FakeRestClient(fs_api, repl_api, network_api=network_api)
        fake_client = FakeClient(rest_client)

        # Create managers
        rm = ReplicationManager(fake_client)
        target_cluster = TargetCluster(fake_client, user_provided_ips=["10.1.1.20"])

        # Execute with filteri="prod" (substring match)
        rm.create_replications(basepath="/data", dst=target_cluster, dst_path="", filteri=["prod"])

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
        directories = ["/data/prod-db", "/data/staging-db", "/data/test-db", "/data/dev-db"]
        fs_api = FakeFileSystemAPI(directories)
        repl_api = FakeReplicationAPI()
        network_api = FakeNetworkAPI(["10.1.1.20"])
        rest_client = FakeRestClient(fs_api, repl_api, network_api=network_api)
        fake_client = FakeClient(rest_client)

        # Create managers
        rm = ReplicationManager(fake_client)
        target_cluster = TargetCluster(fake_client, user_provided_ips=["10.1.1.20"])

        # Execute with multiple patterns (substring matching)
        rm.create_replications(basepath="/data", dst=target_cluster, dst_path="", filteri=["prod", "staging"])

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
        network_api = FakeNetworkAPI(["10.1.1.20"])
        rest_client = FakeRestClient(fs_api, repl_api, network_api=network_api)
        fake_client = FakeClient(rest_client)

        # Create managers
        rm = ReplicationManager(fake_client)
        target_cluster = TargetCluster(fake_client, user_provided_ips=["10.1.1.20"])

        # Execute with filteri that doesn't match
        rm.create_replications(basepath="/data", dst=target_cluster, dst_path="", filteri=["prod-*"])

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
        network_api = FakeNetworkAPI(["10.1.1.20"])
        rest_client = FakeRestClient(fs_api, repl_api, network_api=network_api)
        fake_client = FakeClient(rest_client)

        # Create managers
        rm = ReplicationManager(fake_client)
        target_cluster = TargetCluster(fake_client, user_provided_ips=["10.1.1.20"])

        # Execute with filteri pattern 'tess' (substring match)
        rm.create_replications(basepath="/snapz", dst=target_cluster, dst_path="", filteri=["tess"])

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
        directories = ["/data/prod-db1", "/data/prod-db2", "/data/test-db1", "/data/test-db2"]
        fs_api = FakeFileSystemAPI(directories)
        repl_api = FakeReplicationAPI()
        network_api = FakeNetworkAPI(["10.1.1.20"])
        rest_client = FakeRestClient(fs_api, repl_api, network_api=network_api)
        fake_client = FakeClient(rest_client)

        # Create managers
        rm = ReplicationManager(fake_client)
        target_cluster = TargetCluster(fake_client, user_provided_ips=["10.1.1.20"])

        # Execute with filtere="test" (substring match)
        rm.create_replications(basepath="/data", dst=target_cluster, dst_path="", filtere=["test"])

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
        directories = ["/data/prod-db", "/data/test-db", "/data/temp-cache", "/data/staging-db"]
        fs_api = FakeFileSystemAPI(directories)
        repl_api = FakeReplicationAPI()
        network_api = FakeNetworkAPI(["10.1.1.20"])
        rest_client = FakeRestClient(fs_api, repl_api, network_api=network_api)
        fake_client = FakeClient(rest_client)

        # Create managers
        rm = ReplicationManager(fake_client)
        target_cluster = TargetCluster(fake_client, user_provided_ips=["10.1.1.20"])

        # Execute with multiple exclude patterns (substring matching)
        rm.create_replications(basepath="/data", dst=target_cluster, dst_path="", filtere=["test", "temp"])

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
        network_api = FakeNetworkAPI(["10.1.1.20"])
        rest_client = FakeRestClient(fs_api, repl_api, network_api=network_api)
        fake_client = FakeClient(rest_client)

        # Create managers
        rm = ReplicationManager(fake_client)
        target_cluster = TargetCluster(fake_client, user_provided_ips=["10.1.1.20"])

        # Execute with filtere that matches everything (substring match)
        rm.create_replications(basepath="/data", dst=target_cluster, dst_path="", filtere=["test"])

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
        network_api = FakeNetworkAPI(["10.1.1.20"])
        rest_client = FakeRestClient(fs_api, repl_api, network_api=network_api)
        fake_client = FakeClient(rest_client)

        # Create managers
        rm = ReplicationManager(fake_client)
        target_cluster = TargetCluster(fake_client, user_provided_ips=["10.1.1.20"])

        # Execute with both filteri and dst_path (substring matching)
        rm.create_replications(basepath="/data", dst=target_cluster, dst_path="/backup", filteri=["prod"])

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
        network_api = FakeNetworkAPI(["10.1.1.20"])
        rest_client = FakeRestClient(fs_api, repl_api, network_api=network_api)
        fake_client = FakeClient(rest_client)

        # Create managers
        rm = ReplicationManager(fake_client)
        target_cluster = TargetCluster(fake_client, user_provided_ips=["10.1.1.20"])

        # Execute with both filtere and dst_path (substring matching)
        rm.create_replications(basepath="/data", dst=target_cluster, dst_path="/backup", filtere=["test"])

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
        network_api = FakeNetworkAPI(["10.1.1.20"])
        rest_client = FakeRestClient(fs_api, repl_api, network_api=network_api)
        fake_client = FakeClient(rest_client)

        # Create managers
        rm = ReplicationManager(fake_client)
        target_cluster = TargetCluster(fake_client, user_provided_ips=["10.1.1.20"])

        # Execute without any filters
        rm.create_replications(basepath="/data", dst=target_cluster, dst_path="", filteri=None, filtere=None)

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
            {"id": "repl-001", "source_root_path": "/data/prod-db", "source_root_id": "1", "target_address": "10.1.1.20"},
            {"id": "repl-002", "source_root_path": "/data/test-db", "source_root_id": "2", "target_address": "10.1.1.20"},
            {"id": "repl-003", "source_root_path": "/data/foo-app", "source_root_id": "3", "target_address": "10.1.1.20"},
        ]

        network_api = FakeNetworkAPI(["10.1.1.20"])
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
            {"id": "repl-001", "source_root_path": "/data/prod-db", "source_root_id": "1", "target_address": "10.1.1.20"},
            {"id": "repl-002", "source_root_path": "/data/test-db1", "source_root_id": "2", "target_address": "10.1.1.20"},
            {"id": "repl-003", "source_root_path": "/data/test-db2", "source_root_id": "3", "target_address": "10.1.1.20"},
        ]

        network_api = FakeNetworkAPI(["10.1.1.20"])
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
            {"id": "repl-001", "source_root_path": "/data/prod-db", "source_root_id": "1", "target_address": "10.1.1.20"},
            {"id": "repl-002", "source_root_path": "/data/test-db", "source_root_id": "2", "target_address": "10.1.1.20"},
            {"id": "repl-003", "source_root_path": "/data/foo-app", "source_root_id": "3", "target_address": "10.1.1.20"},
        ]

        network_api = FakeNetworkAPI(["10.1.1.20"])
        rest_client = FakeRestClient(fs_api, repl_api, network_api=network_api)
        fake_client = FakeClient(rest_client)

        # Create manager and populate cache
        rm = ReplicationManager(fake_client)
        rm.populate_replication_cache()

        # Execute clean without filters - should delete all
        deleted_count = rm.clean_replications(basepath="/data")

        # Verify - should delete all 3
        assert deleted_count == 3


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
