import os
from pathlib import Path

import pytest


@pytest.fixture(scope="session")
def test_resources_root() -> Path:
    return Path(__file__).parent / "resources"


@pytest.fixture
def pmc_doc(test_resources_root: Path) -> str:
    with open(test_resources_root / "PMC10335194.xml", "r") as f:
        return f.read()

@pytest.fixture
def pmc_doc_2(test_resources_root: Path) -> str:
    with open(test_resources_root / "PMC3671108.xml", "r") as f:
        return f.read()

@pytest.fixture
def pmc_doc_3(test_resources_root: Path) -> str:
    with open(test_resources_root / "PMC3717426.xml", "r") as f:
        return f.read()
