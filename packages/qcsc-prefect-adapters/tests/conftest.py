"""Pytest configuration for qcsc-prefect-adapters tests."""

from __future__ import annotations

import pytest


def pytest_addoption(parser: pytest.Parser) -> None:
    """Add --kind flag to enable kind-based Kubernetes integration tests."""
    parser.addoption(
        "--kind",
        action="store_true",
        default=False,
        help="Run kind-based Kubernetes integration tests (requires kind + Docker).",
    )
