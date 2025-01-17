import pytest
from src.literature_ingest.pmc import PMC_OPEN_ACCESS_NONCOMMERCIAL_XML_DIR, PMCFTPClient


def test_pmc_connection():
    """Test that we can successfully connect to the PMC FTP server"""
    client = PMCFTPClient()
    try:
        # Test connection
        client.connect()
        assert client.ftp is not None
        assert client.ftp.pwd() == PMC_OPEN_ACCESS_NONCOMMERCIAL_XML_DIR

        # Test basic directory listing
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
