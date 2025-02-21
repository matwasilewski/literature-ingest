from collections import defaultdict
from pathlib import Path
from typing import List

from literature_ingest.data_engineering import unzip_and_filter
from literature_ingest.pmc import PMC_OPEN_ACCESS_NONCOMMERCIAL_XML_DIR, PUBMED_OPEN_ACCESS_DIR, PMCFTPClient, PMCParser, PubMedFTPClient
from literature_ingest.pubmed import PubMedParser
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

def pipeline_parse_missing_files_in_pmc(
        unzipped_files: List[Path],
        parsed_dir: Path = Path("data/pipelines/pmc/parsed/"),
    ):
    parser = PMCParser()
    parsed_dir.mkdir(parents=True, exist_ok=True)

    # get list of files that are already parsed
    already_parsed_files = list(parsed_dir.glob("*.json"))
    already_parsed_files_set = set([Path(x).stem for x in already_parsed_files])

    # get list of files that are not already parsed
    unzipped_files_set = {Path(x).stem: x for x in unzipped_files}
    unzipped_files_to_parse = {k: v for k, v in unzipped_files_set.items() if k not in already_parsed_files_set}
    unzipped_files_to_parse = list(unzipped_files_to_parse.values())

    print(f"Parsing {len(unzipped_files_to_parse)} files, out of total available {len(unzipped_files)}...")
    parsed_files = parser.parse_docs(unzipped_files_to_parse, parsed_dir)

    actual_parsed_files = set(parsed_files)
    failed_files = []

    for file in unzipped_files_to_parse:
        if file.stem + ".json" not in actual_parsed_files:
            failed_files.append(file)

    print(f"Parsed {len(parsed_files)} files, out of intended {len(unzipped_files_to_parse)} - ({len(parsed_files) / len(unzipped_files_to_parse) * 100:.2f}%)...")
    parser.print_article_type_distribution()
    return parsed_files, failed_files


def pipeline_download_pubmed(
        raw_dir: Path = Path("data/pipelines/pubmed/raw/"),
    ) -> List[Path]:
    # Create directories
    raw_dir.mkdir(parents=True, exist_ok=True)

    # Download data
    pubmed_downloader = PubMedFTPClient()
    print("Downloading Pubmed baselines...")
    baseline_files_downloaded, baseline_date = pubmed_downloader._download_pubmed_baselines(raw_dir)
    print(f"Downloaded {len(baseline_files_downloaded)} files...")
    print("DONE: Download Pubmed data")
    return baseline_files_downloaded, baseline_date


def pipeline_unzip_pubmed(
    files_for_unzipping: List[Path],
    unzipped_dir: Path = Path("data/pipelines/pubmed/unzipped/"),
):
    # Create directories
    unzipped_dir.mkdir(parents=True, exist_ok=True)

    print(f"Unzipping {len(files_for_unzipping)} files...")
    for file in files_for_unzipping:
        print(f"Unzipping {file}...")
        unzipped_files_list = unzip_and_filter(file, unzipped_dir, extension=".xml", use_gsutil=False, overwrite=True)
        print(f"Unzipped {len(unzipped_files_list)} files...")
    print(f"Unzipped {unzipped_dir}, to the total of {len(list(unzipped_dir.glob('*.xml')))} XML files...")

    unzipped_files_list = list(unzipped_dir.glob("*.xml"))
    return unzipped_files_list
