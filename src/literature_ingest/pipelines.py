from collections import defaultdict
from pathlib import Path
from typing import List
import asyncio

from literature_ingest.data_engineering import unzip_and_filter
from literature_ingest.models import ArticleType, Document
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

async def pipeline_parse_missing_files_in_pmc(
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
    parsed_files = await parser.parse_docs(unzipped_files_to_parse, parsed_dir)

    actual_parsed_files = set(parsed_files)
    failed_files = []

    for file in unzipped_files_to_parse:
        if file.stem + ".json" not in actual_parsed_files:
            failed_files.append(file)

    print(f"Parsed {len(parsed_files)} files, out of intended {len(unzipped_files_to_parse)} - ({len(parsed_files) / len(unzipped_files_to_parse) * 100:.2f}%)...")
    parser.print_article_type_distribution()
    return parsed_files, failed_files


async def pipeline_parse_pmc(unzipped_files: List[Path], parsed_dir: Path = Path("data/pipelines/pmc/parsed/")):
    parser = PMCParser()
    parsed_dir.mkdir(parents=True, exist_ok=True)

    print(f"Parsing {len(unzipped_files)} files...")
    parsed_files = await parser.parse_docs(unzipped_files, parsed_dir)

    print(f"Parsed {len(parsed_files)} files...")
    parser.print_article_type_distribution()
    return parsed_files


async def pipeline_ingest_pmc_sample(
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

    unzipped_files_list = list(unzipped_dir.glob("*.xml"))
    print("Parsing PMC data..." )
    parsed_files = await pipeline_parse_pubmed(unzipped_files_list, parsed_dir)
    print(f"Parsed {len(parsed_files)} files...")
    print("DONE: Parse PMC data")


async def pipeline_ingest_pubmed_sample(
        raw_dir: Path = Path("data/pipelines/sample_pubmed/raw/"),
        unzipped_dir: Path = Path("data/pipelines/sample_pubmed/unzipped/"),
        parsed_dir: Path = Path("data/pipelines/sample_pubmed/parsed/"),
        file_names: List[str] = ['pubmed25n0001.xml.gz']
    ):
    # Create directories
    raw_dir.mkdir(parents=True, exist_ok=True)
    unzipped_dir.mkdir(parents=True, exist_ok=True)
    parsed_dir.mkdir(parents=True, exist_ok=True)

    # Download data
    pubmed_downloader = PubMedFTPClient()
    print("Downloading Pubmed baselines...")
    baseline_files_downloaded = pubmed_downloader._download_pubmed_baselines_sample(raw_dir, file_names=file_names)
    print(f"Downloaded {len(baseline_files_downloaded)} files...")
    print("DONE: Download Pubmed data")

    # Unzip data
    print(f"Unzipping {raw_dir}...")
    for file in baseline_files_downloaded:
        print(f"Unzipping {file}...")
        unzipped_files_list = unzip_and_filter(file, unzipped_dir, extension=".xml", use_gsutil=False, overwrite=True)
        print(f"Unzipped {len(unzipped_files_list)} files...")
    print(f"Unzipped {unzipped_dir}, to the total of {len(list(unzipped_dir.glob('*.xml')))} files...")

    unzipped_files_list = list(unzipped_dir.glob("*.xml"))
    print("Parsing PMC data..." )
    parsed_files = await pipeline_parse_pubmed(unzipped_files_list, parsed_dir)
    print(f"Parsed {len(parsed_files)} files...")
    print("DONE: Parse PMC data")


async def pipeline_download_pubmed(
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

async def pipeline_unzip_pubmed(
        files_for_unzipping: List[Path],
        unzipped_dir: Path = Path("data/pipelines/pubmed/unzipped/"),
    ):
    # Create directories
    unzipped_dir.mkdir(parents=True, exist_ok=True)

    print(f"Unzipping {len(files_for_unzipping)} files...")
    for file in files_for_unzipping:
        print(f"Unzipping {file}...")
        try:
            unzipped_files_list = unzip_and_filter(file, unzipped_dir, extension=".xml", use_gsutil=False, overwrite=True)
            print(f"Unzipped {len(unzipped_files_list)} files...")
        except Exception as e:
            print(f"Error unzipping {file}: {e}")
            continue
    print(f"Unzipped {unzipped_dir}, to the total of {len(list(unzipped_dir.glob('*.xml')))} XML files...")

    unzipped_files_list = list(unzipped_dir.glob("*.xml"))
    return unzipped_files_list


async def pipeline_parse_pubmed(unzipped_files: List[Path], parsed_dir: Path = Path("data/pipelines/pubmed/parsed/")):
    print("Parsing PubMed data..." )

    parser = PubMedParser()
    parsed_dir.mkdir(parents=True, exist_ok=True)

    print(f"Parsing {len(unzipped_files)} files...")
    parsed_files = await parser.parse_docs(unzipped_files, parsed_dir)

    print(f"Parsed {len(parsed_files)} files...")
    print("DONE: Parse PubMed data")
    parser.print_article_type_distribution()
    return parsed_files


async def pipeline_ingest_pubmed(
        raw_dir: Path = Path("data/pipelines/pubmed/raw/"),
        unzipped_dir: Path = Path("data/pipelines/pubmed/unzipped/"),
        parsed_dir: Path = Path("data/pipelines/pubmed/parsed/"),
        unzip_all: bool = False,
        parse_all: bool = False,
    ):
    # Create directories
    raw_dir.mkdir(parents=True, exist_ok=True)
    unzipped_dir.mkdir(parents=True, exist_ok=True)
    parsed_dir.mkdir(parents=True, exist_ok=True)

    # Download data
    baseline_files_downloaded, baseline_date = await pipeline_download_pubmed(raw_dir)

    dated_raw_dir = raw_dir / baseline_date

    # Unzip data
    if unzip_all:
        print("Using unzip_all=True, unzipping all files...")
        # get all files from the raw directory
        files_for_unzipping = list(dated_raw_dir.glob("*.gz"))
        print(f"Found {len(files_for_unzipping)} files to unzip...")
    else:
        files_for_unzipping = baseline_files_downloaded

    unzipped_files_list = await pipeline_unzip_pubmed(files_for_unzipping, unzipped_dir)

    # Unzip data
    if parse_all:
        print("Using parse_all=True, parsing all files...")
        files_for_parsing = list(unzipped_dir.glob("*.xml"))
        print(f"Found {len(files_for_parsing)} files to parse...")
    else:
        files_for_parsing = unzipped_files_list

    parsed_files = await pipeline_parse_pubmed(files_for_parsing, parsed_dir)


async def pipeline_ingest_pmc(
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
    unzipped_files_list = list(unzipped_dir.glob("*.xml"))
    print("Parsing PMC data..." )
    parsed_files = await pipeline_parse_pmc(unzipped_files_list, parsed_dir)
    print(f"Parsed {len(parsed_files)} files...")
    print("DONE: Parse PMC data")
