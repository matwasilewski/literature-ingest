from pathlib import Path
import tempfile

from cloudpathlib import CloudPath
from literature_ingest.data_engineering import unzip_and_filter

from literature_ingest.utils.config import settings

def test_unzip_and_filter(test_resources_root: Path) -> None:
    file_name = "oa_noncomm_xml.incr.2024-12-22.tar.gz"
    archive_file = test_resources_root / file_name

    if not settings.TEST_BUCKET:
        raise ValueError("TEST_BUCKET is not set - skipping test")

    unzip_and_filter(archive_file, CloudPath(f"gs://{settings.TEST_BUCKET}"), use_gsutil=True, overwrite=True)
