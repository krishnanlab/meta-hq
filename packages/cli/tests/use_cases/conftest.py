"""Fixtures for the documented use-case tests (see mini_package.py)."""

from pathlib import Path

import pytest
from mini_package import configure_metahq


@pytest.fixture
def mini_metahq(tmp_path, monkeypatch) -> Path:
    """A configured MetaHQ install using the mini data package (see module doc)."""
    return configure_metahq(tmp_path, monkeypatch)
