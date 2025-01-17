from pathlib import Path
import tempfile

from literature_ingest.data_engineering import unzip_and_filter


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
