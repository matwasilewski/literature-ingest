from pathlib import Path
import pytest

from literature_ingest.pmc import PMC_OPEN_ACCESS_NONCOMMERCIAL_XML_DIR, PMCFTPClient


def test_pmc_connection():
    """Test that we can successfully connect to the PMC FTP server"""
    client = PMCFTPClient()
    try:
        client.connect()
        assert client.ftp is not None
        assert client.ftp.pwd() == PMC_OPEN_ACCESS_NONCOMMERCIAL_XML_DIR

        files = client.list_directory()
        assert len(files) > 0  # Should have at least some files/directories

    finally:
        client.close()
        assert client.ftp is None  # Verify connection is closed

def test_connection_error_handling():
    """Test error handling with invalid connection attempts"""
    client = PMCFTPClient()
    client.host = "invalid.host.example.com"

    with pytest.raises(Exception):
        client.connect()

@pytest.fixture
def temp_baseline_dir(tmp_path):
    """Fixture that provides a temporary directory for baseline downloads"""
    baseline_dir = tmp_path / "baselines"
    baseline_dir.mkdir()
    return baseline_dir

def test_download_baselines(temp_baseline_dir):
    """Test downloading baseline files"""
    client = PMCFTPClient()
    client.connect()
    client._download_pmc_baselines(base_dir=temp_baseline_dir, dry_run=True)
    client.close()
    assert True
