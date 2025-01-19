from pathlib import Path
import tempfile
import pytest

from literature_ingest.data_engineering import unzip_and_filter, unzip_to_local


def test_unzip_and_filter(test_resources_root: Path) -> None:
    file_name = "oa_noncomm_xml.incr.2024-12-22.tar.gz"
    archive_file = test_resources_root / file_name

    with tempfile.TemporaryDirectory() as temp_dir:
        target_dir = Path(temp_dir)
        files = unzip_and_filter(archive_file, target_dir)
        assert len(files) == 95

        # Assert only .xml files are extracted
        assert all(file.name.endswith(".xml") for file in files)

        # Assert the first file is PMC7617240.xml
        assert files[0].name == "PMC7617240.xml"
        assert (Path(temp_dir) / "PMC7617240.xml").exists()


def test_unzip_pubmed_sample(pubmed_sample):
    """Test unzipping a PubMed sample file to a temporary directory."""
    with tempfile.TemporaryDirectory() as temp_dir:
        # Unzip the file
        files = unzip_to_local(pubmed_sample, Path(temp_dir))

        # Check that we got exactly one file
        assert len(files) == 1

        # Check that the file exists and is not empty
        unzipped_file = files[0]
        assert unzipped_file.exists()
        assert unzipped_file.stat().st_size > 100

        # Check that the file has the correct extension (without .gz)
        assert unzipped_file.suffix == ".xml"
