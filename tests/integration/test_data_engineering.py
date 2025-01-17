from pathlib import Path
import tempfile

from cloudpathlib import CloudPath
from literature_ingest.data_engineering import unzip_and_filter


def test_unzip_and_filter(test_resources_root: Path) -> None:
    file_name = "oa_noncomm_xml.incr.2024-12-22.tar.gz"
    archive_file = test_resources_root / file_name

    unzip_and_filter(archive_file, CloudPath("gs://mtrx-us-central1-hub-dev-storage/literature"), use_gsutil=True, overwrite=True)
