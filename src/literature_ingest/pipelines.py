from collections import defaultdict
from pathlib import Path
from typing import List

from literature_ingest.data_engineering import unzip_and_filter
from literature_ingest.models import ArticleType, Document
from literature_ingest.pmc import PMCFTPClient, PMCParser


def pipeline_parse_pmc(unzipped_dir: Path, parsed_dir: Path = Path("data/pipelines/pmc/parsed/")):
    parser = PMCParser()
    parsed_dir.mkdir(parents=True, exist_ok=True)

    print(f"Parsing {unzipped_dir}, totalling {len(list(unzipped_dir.glob('*.xml')))} files...")
    unzipped_files = [Path(x) for x in list(unzipped_dir.glob("*.xml"))]
    parsed_files = parser.parse_docs(unzipped_files, parsed_dir)

    print(f"Parsed {len(parsed_files)} files...")
    print(f"Unique article types: {parser.unique_article_types}")


def pipeline_ingest_pmc_sample(source_dir: Path = Path("data/pipelines/sample_pmc/raw/"), unzipped_dir: Path = Path("data/pipelines/sample_pmc/unzipped/"), parsed_dir: Path = Path("data/pipelines/sample_pmc/parsed/"), file_names: List[str] = ['oa_noncomm_xml.PMC002xxxxxx.baseline.2024-12-18.tar.gz']):
        # Create directories
    source_dir.mkdir(parents=True, exist_ok=True)
    unzipped_dir.mkdir(parents=True, exist_ok=True)
    parsed_dir.mkdir(parents=True, exist_ok=True)

    # Download data
    pmc_downloader = PMCFTPClient()

    print("Downloading PMC baselines...")
    pmc_downloader._download_pmc_baselines_sample(source_dir, file_names=file_names)
    print(f"Downloaded {len(list(source_dir.rglob('*.tar.gz')))} files...")
    print("DONE: Download PMC data")

    # Unzip data
    print(f"Unzipping {source_dir}...")
    for file in source_dir.rglob("*.tar.gz"):
        print(f"Unzipping {file}...")
        unzipped_files_list = unzip_and_filter(file, unzipped_dir, extension=".xml", use_gsutil=False, overwrite=True)
        print(f"Unzipped {len(unzipped_files_list)} files...")
    print(f"Unzipped {unzipped_dir}, to the total of {len(list(unzipped_dir.glob('*.xml')))} files...")

    print("Parsing PMC data..." )
    pipeline_parse_pmc(unzipped_dir, parsed_dir)
    print("DONE: Parse PMC data")



def pipeline_ingest_pmc(source_dir: Path = Path("data/pipelines/pmc/raw/"), unzipped_dir: Path = Path("data/pipelines/pmc/unzipped/"), parsed_dir: Path = Path("data/pipelines/pmc/parsed/")):
    # Create directories
    source_dir.mkdir(parents=True, exist_ok=True)
    unzipped_dir.mkdir(parents=True, exist_ok=True)
    parsed_dir.mkdir(parents=True, exist_ok=True)

    # Download data
    pmc_downloader = PMCFTPClient()

    print("Downloading PMC baselines...")
    pmc_downloader._download_pmc_baselines(source_dir)
    print("Downloading PMC incremental...")
    pmc_downloader._download_pmc_incremental(source_dir)
    print(f"Downloaded {len(list(source_dir.rglob('*.tar.gz')))} files...")
    print("DONE: Download PMC data")

    # Unzip data
    print(f"Unzipping {source_dir}...")
    for file in source_dir.rglob("*.tar.gz"):
        print(f"Unzipping {file}...")
        unzipped_files_list = unzip_and_filter(file, unzipped_dir, extension=".xml", use_gsutil=False, overwrite=True)
        print(f"Unzipped {len(unzipped_files_list)} files...")
    print(f"Unzipped {unzipped_dir}, to the total of {len(list(unzipped_dir.glob('*.xml')))} files...")
    print("DONE: Unzip PMC data")

    print("Parsing PMC data..." )
    pipeline_parse_pmc(unzipped_dir, parsed_dir)
    print("DONE: Parse PMC data")
