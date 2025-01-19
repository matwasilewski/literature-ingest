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

@pytest.fixture
def pmc_doc_4(test_resources_root: Path) -> str:
    with open(test_resources_root / "PMC_test_4.xml", "r") as f:
        return f.read()

@pytest.fixture
def pmc_doc_5(test_resources_root: Path) -> str:
    with open(test_resources_root / "PMC_test_5.xml", "r") as f:
        return f.read()

@pytest.fixture
def pmc_doc_6(test_resources_root: Path) -> str:
    with open(test_resources_root / "PMC_test_6.xml", "r") as f:
        return f.read()

@pytest.fixture
def pmc_doc_7(test_resources_root: Path) -> str:
    with open(test_resources_root / "PMC_test_7.xml", "r") as f:
        return f.read()

@pytest.fixture
def pmc_doc_8(test_resources_root: Path) -> str:
    with open(test_resources_root / "PMC_test_8.xml", "r") as f:
        return f.read()

@pytest.fixture
def pmc_doc_9(test_resources_root: Path) -> str:
    with open(test_resources_root / "PMC_test_9.xml", "r") as f:
        return f.read()

@pytest.fixture
def pubmed_sample(test_resources_root: Path) -> Path:
    return test_resources_root / "pubmed25n0001.xml.gz"

@pytest.fixture
def pubmed_doc():
    """Load sample PubMed XML document"""
    with open(Path("tests/resources/pubmed_sample.xml"), "r") as f:
        return f.read()
