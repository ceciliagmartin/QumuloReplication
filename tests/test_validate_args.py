#!/usr/bin/env python3
"""
Tests for validate_args function - TDD approach
"""

import pytest
import argparse
from typing import Optional
from replication import validate_args


class FakeClient:
    """Fake Client for testing - doesn't require network connection"""

    def __init__(self, creds):
        self.creds = creds
        self.rc = None  # Placeholder


class FakeArgs:
    """Fake argparse.Namespace for testing"""

    def __init__(
        self,
        action: str,
        src_host: Optional[str] = None,
        src_user: Optional[str] = None,
        src_password: Optional[str] = None,
        dst_host: Optional[str] = None,
        dst_user: Optional[str] = None,
        dst_password: Optional[str] = None,
        basepath: str = "/",
        dst: Optional[list] = None,
        dst_network_id: str = "1",
        dst_path: str = "",
        filteri: Optional[list] = None,
        filtere: Optional[list] = None,
    ):
        self.action = action
        self.src_host = src_host
        self.src_user = src_user
        self.src_password = src_password
        self.dst_host = dst_host
        self.dst_user = dst_user
        self.dst_password = dst_password
        self.basepath = basepath
        self.dst = dst
        self.dst_network_id = dst_network_id
        self.dst_path = dst_path
        self.filteri = filteri
        self.filtere = filtere


class TestValidateArgsSummaryAction:
    """Test validation for summary action"""

    def test_summary_with_src_only_returns_src_client(self):
        """Test: summary with only src credentials returns (src_client, None)"""
        args = FakeArgs(
            action="summary",
            src_host="src.example.com",
            src_user="admin",
            src_password="password123",
        )

        src_client, dst_client = validate_args(args, client_factory=FakeClient)

        assert src_client is not None
        assert dst_client is None

    def test_summary_with_both_returns_both_clients(self):
        """Test: summary with src+dst credentials returns (src_client, dst_client)"""
        args = FakeArgs(
            action="summary",
            src_host="src.example.com",
            src_user="admin",
            src_password="password123",
            dst_host="dst.example.com",
            dst_user="admin",
            dst_password="password456",
        )

        src_client, dst_client = validate_args(args, client_factory=FakeClient)

        assert src_client is not None
        assert dst_client is not None

    def test_summary_without_src_raises_error(self):
        """Test: summary without src credentials raises ValueError"""
        args = FakeArgs(action="summary")

        with pytest.raises(ValueError) as exc_info:
            validate_args(args)

        assert "src_host" in str(exc_info.value).lower()
        assert "summary" in str(exc_info.value)


class TestValidateArgsCreateAction:
    """Test validation for create action"""

    def test_create_with_both_returns_both_clients(self):
        """Test: create with src+dst returns (src_client, dst_client)"""
        args = FakeArgs(
            action="create",
            src_host="src.example.com",
            src_user="admin",
            src_password="password123",
            dst_host="dst.example.com",
            dst_user="admin",
            dst_password="password456",
        )

        src_client, dst_client = validate_args(args, client_factory=FakeClient)

        assert src_client is not None
        assert dst_client is not None

    def test_create_without_src_raises_error(self):
        """Test: create without src raises ValueError"""
        args = FakeArgs(
            action="create",
            dst_host="dst.example.com",
            dst_user="admin",
            dst_password="password456",
        )

        with pytest.raises(ValueError) as exc_info:
            validate_args(args, client_factory=FakeClient)

        assert "src" in str(exc_info.value).lower()

    def test_create_without_dst_raises_error(self):
        """Test: create without dst raises ValueError"""
        args = FakeArgs(
            action="create",
            src_host="src.example.com",
            src_user="admin",
            src_password="password123",
        )

        with pytest.raises(ValueError) as exc_info:
            validate_args(args, client_factory=FakeClient)

        assert "dst" in str(exc_info.value).lower()
        assert "create" in str(exc_info.value)

    def test_create_without_both_raises_error(self):
        """Test: create without any credentials raises ValueError"""
        args = FakeArgs(action="create")

        with pytest.raises(ValueError) as exc_info:
            validate_args(args, client_factory=FakeClient)

        assert "create" in str(exc_info.value)


class TestValidateArgsCleanAction:
    """Test validation for clean action"""

    def test_clean_with_src_only_returns_src_client(self):
        """Test: clean with only src returns (src_client, None)"""
        args = FakeArgs(
            action="clean",
            src_host="src.example.com",
            src_user="admin",
            src_password="password123",
        )

        src_client, dst_client = validate_args(args, client_factory=FakeClient)

        assert src_client is not None
        assert dst_client is None

    def test_clean_with_dst_only_returns_dst_client(self):
        """Test: clean with only dst returns (None, dst_client)"""
        args = FakeArgs(
            action="clean",
            dst_host="dst.example.com",
            dst_user="admin",
            dst_password="password456",
        )

        src_client, dst_client = validate_args(args, client_factory=FakeClient)

        assert src_client is None
        assert dst_client is not None

    def test_clean_with_both_returns_both_clients(self):
        """Test: clean with src+dst returns (src_client, dst_client)"""
        args = FakeArgs(
            action="clean",
            src_host="src.example.com",
            src_user="admin",
            src_password="password123",
            dst_host="dst.example.com",
            dst_user="admin",
            dst_password="password456",
        )

        src_client, dst_client = validate_args(args, client_factory=FakeClient)

        assert src_client is not None
        assert dst_client is not None

    def test_clean_without_any_raises_error(self):
        """Test: clean without any credentials raises ValueError"""
        args = FakeArgs(action="clean")

        with pytest.raises(ValueError) as exc_info:
            validate_args(args, client_factory=FakeClient)

        assert "clean" in str(exc_info.value)
        assert "at least" in str(exc_info.value).lower()


class TestValidateArgsAcceptAction:
    """Test validation for accept action"""

    def test_accept_with_dst_returns_dst_client(self):
        """Test: accept with dst returns (None, dst_client)"""
        args = FakeArgs(
            action="accept",
            dst_host="dst.example.com",
            dst_user="admin",
            dst_password="password456",
        )

        src_client, dst_client = validate_args(args, client_factory=FakeClient)

        assert src_client is None
        assert dst_client is not None

    def test_accept_without_dst_raises_error(self):
        """Test: accept without dst raises ValueError"""
        args = FakeArgs(action="accept")

        with pytest.raises(ValueError) as exc_info:
            validate_args(args, client_factory=FakeClient)

        assert "dst" in str(exc_info.value).lower()
        assert "accept" in str(exc_info.value)

    def test_accept_with_src_only_raises_error(self):
        """Test: accept with only src (no dst) raises ValueError"""
        args = FakeArgs(
            action="accept",
            src_host="src.example.com",
            src_user="admin",
            src_password="password123",
        )

        with pytest.raises(ValueError) as exc_info:
            validate_args(args, client_factory=FakeClient)

        assert "dst" in str(exc_info.value).lower()


class TestValidateArgsFilterValidation:
    """Test validation for filteri and filtere arguments"""

    def test_filteri_and_filtere_together_raises_error(self):
        """
        Test: Using both --filteri and --filtere together raises ValueError
        AC: Mutual exclusion - cannot use include and exclude filters together
        """
        args = FakeArgs(
            action="create",
            src_host="src.example.com",
            src_user="admin",
            src_password="password123",
            dst_host="dst.example.com",
            dst_user="admin",
            dst_password="password456",
            filteri=["prod-*"],
            filtere=["test-*"],
        )

        with pytest.raises(ValueError) as exc_info:
            validate_args(args, client_factory=FakeClient)

        assert (
            "filteri" in str(exc_info.value).lower()
            or "filtere" in str(exc_info.value).lower()
        )
        assert (
            "both" in str(exc_info.value).lower()
            or "together" in str(exc_info.value).lower()
        )

    def test_filteri_alone_is_valid(self):
        """
        Test: Using --filteri alone is valid for create action
        AC: Include filter can be used without exclude filter
        """
        args = FakeArgs(
            action="create",
            src_host="src.example.com",
            src_user="admin",
            src_password="password123",
            dst_host="dst.example.com",
            dst_user="admin",
            dst_password="password456",
            filteri=["prod-*"],
        )

        # Should not raise
        src_client, dst_client = validate_args(args, client_factory=FakeClient)
        assert src_client is not None
        assert dst_client is not None

    def test_filtere_alone_is_valid(self):
        """
        Test: Using --filtere alone is valid for create action
        AC: Exclude filter can be used without include filter
        """
        args = FakeArgs(
            action="create",
            src_host="src.example.com",
            src_user="admin",
            src_password="password123",
            dst_host="dst.example.com",
            dst_user="admin",
            dst_password="password456",
            filtere=["test-*", "temp-*"],
        )

        # Should not raise
        src_client, dst_client = validate_args(args, client_factory=FakeClient)
        assert src_client is not None
        assert dst_client is not None

    def test_no_filters_is_valid(self):
        """
        Test: Not providing any filters is valid (default behavior)
        AC: Replicate all directories when no filters specified
        """
        args = FakeArgs(
            action="create",
            src_host="src.example.com",
            src_user="admin",
            src_password="password123",
            dst_host="dst.example.com",
            dst_user="admin",
            dst_password="password456",
            filteri=None,
            filtere=None,
        )

        # Should not raise
        src_client, dst_client = validate_args(args, client_factory=FakeClient)
        assert src_client is not None
        assert dst_client is not None


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
