from collections import defaultdict
from pathlib import Path
from typing import List

from literature_ingest.data_engineering import unzip_and_filter
from literature_ingest.models import ArticleType, Document
from literature_ingest.pmc import PMCFTPClient, PMCParser
from pydantic import BaseModel

class PipelineResult(BaseModel):
    success: bool
    message: str
    docs_ingested: List[Path]
    docs_success: List[Path]
    docs_failed: List[Path]
    docs_skipped: List[Path]
    docs_with_warnings: List[Path]


"""
Idea: every step accepts a list of files as input (so conversions between directory and file lists are possible) and returns a list of files as outputs.

Saving those, we can trace file lineage easily.

"""

def pipeline_parse_missing_files(unzipped_files: List[Path], parsed_dir: Path = Path("data/pipelines/pmc/parsed/")):
    parser = PMCParser()
    parsed_dir.mkdir(parents=True, exist_ok=True)

    # get list of files that are already parsed
    parsed_files = list(parsed_dir.glob("*.json"))
    parsed_files_set = set([Path(x).stem for x in parsed_files])
    unzipped_files_set = set([Path(x).stem for x in unzipped_files])
    unzipped_files_to_parse = unzipped_files_set - parsed_files_set
    unzipped_files_to_parse = [Path(x) for x in unzipped_files_to_parse]

    print(f"Parsing {len(unzipped_files_to_parse)} files, out of total available {len(unzipped_files)}...")
    parsed_files = parser.parse_docs(unzipped_files_to_parse, parsed_dir)

    print(f"Parsed {len(parsed_files)} files, out of intended {len(unzipped_files_to_parse)} - ({len(parsed_files) / len(unzipped_files_to_parse) * 100:.2f}%)...")
    parser.print_article_type_distribution()
    return parsed_files


def pipeline_parse_pmc(unzipped_files: List[Path], parsed_dir: Path = Path("data/pipelines/pmc/parsed/")):
    parser = PMCParser()
    parsed_dir.mkdir(parents=True, exist_ok=True)

    print(f"Parsing {len(unzipped_files)} files...")
    parsed_files = parser.parse_docs(unzipped_files, parsed_dir)

    print(f"Parsed {len(parsed_files)} files...")
    parser.print_article_type_distribution()
    return parsed_files

def pipeline_ingest_pmc_sample(
        raw_dir: Path = Path("data/pipelines/sample_pmc/raw/"),
        unzipped_dir: Path = Path("data/pipelines/sample_pmc/unzipped/"),
        parsed_dir: Path = Path("data/pipelines/sample_pmc/parsed/"),
        file_names: List[str] = ['oa_noncomm_xml.PMC002xxxxxx.baseline.2024-12-18.tar.gz']
    ):
    # Create directories
    raw_dir.mkdir(parents=True, exist_ok=True)
    unzipped_dir.mkdir(parents=True, exist_ok=True)
    parsed_dir.mkdir(parents=True, exist_ok=True)

    # Download data
    pmc_downloader = PMCFTPClient()

    print("Downloading PMC baselines...")
    baseline_files_downloaded = pmc_downloader._download_pmc_baselines_sample(raw_dir, file_names=file_names)
    print(f"Downloaded {len(baseline_files_downloaded)} files...")
    print("DONE: Download PMC data")

    # Unzip data
    print(f"Unzipping {raw_dir}...")
    for file in baseline_files_downloaded:
        print(f"Unzipping {file}...")
        unzipped_files_list = unzip_and_filter(file, unzipped_dir, extension=".xml", use_gsutil=False, overwrite=True)
        print(f"Unzipped {len(unzipped_files_list)} files...")
    print(f"Unzipped {unzipped_dir}, to the total of {len(list(unzipped_dir.glob('*.xml')))} files...")

    print("Parsing PMC data..." )
    parsed_files = pipeline_parse_pmc(unzipped_dir, parsed_dir)
    print(f"Parsed {len(parsed_files)} files...")
    print("DONE: Parse PMC data")




def pipeline_ingest_pmc(
        raw_dir: Path = Path("data/pipelines/pmc/raw/"),
        unzipped_dir: Path = Path("data/pipelines/pmc/unzipped/"),
        parsed_dir: Path = Path("data/pipelines/pmc/parsed/"),
    ):
    # Create directories
    raw_dir.mkdir(parents=True, exist_ok=True)
    unzipped_dir.mkdir(parents=True, exist_ok=True)
    parsed_dir.mkdir(parents=True, exist_ok=True)

    # Download data
    pmc_downloader = PMCFTPClient()

    print("Downloading PMC baselines...")
    baseline_files_downloaded = pmc_downloader._download_pmc_baselines(raw_dir)
    print("Downloading PMC incremental...")
    incremental_files_downloaded = pmc_downloader._download_pmc_incremental(raw_dir)
    print(f"Downloaded {len(baseline_files_downloaded)} files...")
    print("DONE: Download PMC data")

    # Unzip data
    print(f"Unzipping {raw_dir}...")
    for file in baseline_files_downloaded + incremental_files_downloaded:
        print(f"Unzipping {file}...")
        unzipped_files_list = unzip_and_filter(file, unzipped_dir, extension=".xml", use_gsutil=False, overwrite=True)
        print(f"Unzipped {len(unzipped_files_list)} files...")
    print(f"Unzipped {unzipped_dir}, to the total of {len(list(unzipped_dir.glob('*.xml')))} files...")
    print("DONE: Unzip PMC data")

    # parse data
    print("Parsing PMC data..." )
    parsed_files = pipeline_parse_pmc(unzipped_dir, parsed_dir)
    print(f"Parsed {len(parsed_files)} files...")
    print("DONE: Parse PMC data")
