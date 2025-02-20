import datetime
import json
from pathlib import Path
from typing import List, Optional, Union
from xml.dom.minidom import Document
import click
from literature_ingest.cli import pipelines
from literature_ingest.data_engineering import unzip_and_filter
from literature_ingest.normalization import normalize_document
from literature_ingest.pipelines import pipeline_download_pubmed, pipeline_parse_missing_files_in_pmc, pipeline_parse_pmc, pipeline_parse_pubmed, pipeline_unzip_pubmed
from literature_ingest.pmc import PMC_OPEN_ACCESS_NONCOMMERCIAL_XML_DIR, PUBMED_OPEN_ACCESS_DIR, PMCFTPClient, PMCParser, PubMedFTPClient
from literature_ingest.utils.logging import get_logger
from literature_ingest.utils.config import settings

from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor
from functools import partial

logger = get_logger(__name__, "info")


@pipelines.command()
@click.option(
    "--sample",
    is_flag=True,
    help="Run sample ingestion with specific files",
)
@click.option(
    "--save-space",
    is_flag=True,
    help="Save space by deleting downloaded files after ingestion",
)
def ingest_pmc(sample: bool, save_space: bool):
    """Ingest PMC data. Use --sample flag for sample ingestion."""
    if sample:
        click.echo("Ingesting PMC sample data...")
        base_dir = Path("data/pipelines/sample_pmc")
    else:
        click.echo("Ingesting PMC data...")
        base_dir = Path("data/pipelines/pmc")

    # Define directories
    raw_dir = base_dir / "raw"
    unzipped_dir = base_dir / "unzipped"
    parsed_dir = base_dir / "parsed"

    # Create directories
    raw_dir.mkdir(parents=True, exist_ok=True)
    unzipped_dir.mkdir(parents=True, exist_ok=True)
    parsed_dir.mkdir(parents=True, exist_ok=True)

    # Download data
    pmc_downloader = PMCFTPClient()
    SAMPLE_FILE_NAME = 'oa_noncomm_xml.PMC002xxxxxx.baseline.2024-12-18.tar.gz'

    click.echo("Downloading PMC...")
    if sample:
        click.echo("Downloading PMC Sample Files...")
        baseline_files_downloaded = pmc_downloader._download_pmc_baselines_sample(raw_dir, file_names=[SAMPLE_FILE_NAME])
        incremental_files_downloaded = []

    else:
        click.echo("Downloading PMC Baselines (full)...")
        baseline_files_downloaded = pmc_downloader._download_pmc_baselines(raw_dir)
        click.echo("Downloading PMC incremental...")
        incremental_files_downloaded = pmc_downloader._download_pmc_incremental(raw_dir)
        click.echo(f"Downloaded {len(baseline_files_downloaded) + len(incremental_files_downloaded)} "
                   "files... Files already stored are not downloaded again and counter here.")

    click.echo("DONE: Download PMC data")

    # Get all files from raw directory instead of using download outputs
    if sample:
        # Sample files have a specific pattern
        all_raw_files = [f for f in raw_dir.glob(f"**/{SAMPLE_FILE_NAME}") if f.is_file()]
    else:
        # For full ingestion, get all tar.gz files
        all_raw_files = [f for f in raw_dir.glob("**.tar.gz") if f.is_file()]

    click.echo(f"Found {len(all_raw_files)} files in raw directory for processing")

    # Unzip data
    click.echo(f"Unzipping {raw_dir}...")
    all_unzipped_files = []

    for file in all_raw_files:
        click.echo(f"Unzipping {file}...")
        # Create a subdirectory using the file's stem (name without extension)
        file_unzip_dir = unzipped_dir / Path(file).stem
        file_unzip_dir.mkdir(parents=True, exist_ok=True)

        unzipped_files_list = unzip_and_filter(file, file_unzip_dir, extension=".xml", use_gsutil=False, overwrite=True)
        all_unzipped_files.extend(unzipped_files_list)
        click.echo(f"Unzipped {len(unzipped_files_list)} files to {file_unzip_dir}...")

        click.echo("Parsing PMC data..." )
        updated_unzipped_files_list = list(file_unzip_dir.glob("*.xml"))
        file_parsed_dir = parsed_dir / Path(file).stem
        file_parsed_dir.mkdir(parents=True, exist_ok=True)

        parsed_files = pipeline_parse_pmc(updated_unzipped_files_list, file_parsed_dir)
        click.echo(f"Parsed {len(parsed_files)} files...")
        click.echo("DONE: Parse PMC data")

        if save_space:
            upload_to_gcs_and_save_space(unzipped_dir, parsed_dir)

    click.echo(f"Unzipped all files to {unzipped_dir}, total of {len(all_unzipped_files)} files...")
    click.echo("DONE: Unzip PMC data")
    click.echo(f"DONE: Ingest PMC {'sample ' if sample else ''}data")


@pipelines.command()
@click.option(
    "--sample",
    is_flag=True,
    help="Run sample ingestion with specific files",
)
@click.option(
    "--file-names",
    default=['pubmed25n0001.xml.gz'],
    help="File names to download (only used with --sample)",
    type=str,
    multiple=True,
)
@click.option(
    "--unzip-all",
    is_flag=True,
    help="Unzip all files (only used for full ingestion)",
)
@click.option(
    "--parse-all",
    is_flag=True,
    help="Parse all files (only used for full ingestion)",
)
def ingest_pubmed(sample: bool, file_names: List[str], unzip_all: bool, parse_all: bool):
    """Ingest PubMed data. Use --sample flag for sample ingestion."""
    if sample:
        click.echo("Ingesting PubMed sample data...")
        base_dir = Path("data/pipelines/sample_pubmed")
    else:
        click.echo("Ingesting PubMed data...")
        base_dir = Path("data/pipelines/pubmed")

    # Define directories
    raw_dir = base_dir / "raw"
    unzipped_dir = base_dir / "unzipped"
    parsed_dir = base_dir / "parsed"

    # Create directories
    raw_dir.mkdir(parents=True, exist_ok=True)
    unzipped_dir.mkdir(parents=True, exist_ok=True)
    parsed_dir.mkdir(parents=True, exist_ok=True)

    # Download data
    pubmed_downloader = PubMedFTPClient()
    click.echo("Downloading PubMed baselines...")

    if sample:
        baseline_files_downloaded = pubmed_downloader._download_pubmed_baselines_sample(raw_dir, file_names=file_names)
        baseline_date = None
    else:
        baseline_files_downloaded, baseline_date = pipeline_download_pubmed(raw_dir)
        dated_raw_dir = raw_dir / baseline_date if baseline_date else raw_dir

    click.echo(f"Downloaded {len(baseline_files_downloaded)} files...")
    click.echo("DONE: Download PubMed data")

    # Unzip data
    if not sample and unzip_all:
        click.echo("Using unzip_all=True, unzipping all files...")
        # get all files from the raw directory
        files_for_unzipping = list((dated_raw_dir if baseline_date else raw_dir).glob("*.gz"))
        click.echo(f"Found {len(files_for_unzipping)} files to unzip...")
    else:
        files_for_unzipping = baseline_files_downloaded

    unzipped_files_list = pipeline_unzip_pubmed(files_for_unzipping, unzipped_dir)
    click.echo(f"Unzipped {len(unzipped_files_list)} files...")
    click.echo("DONE: Unzip PubMed data")

    # Parse data
    if not sample and parse_all:
        click.echo("Using parse_all=True, parsing all files...")
        files_for_parsing = list(unzipped_dir.glob("*.xml"))
        click.echo(f"Found {len(files_for_parsing)} files to parse...")
    else:
        files_for_parsing = unzipped_files_list

    click.echo("Parsing PubMed data...")
    parsed_files = pipeline_parse_pubmed(files_for_parsing, parsed_dir)
    click.echo(f"Parsed {len(parsed_files)} files...")
    click.echo("DONE: Parse PubMed data")

    click.echo(f"DONE: Ingest PubMed {'sample ' if sample else ''}data")


@pipelines.command()
@click.option(
    "--file-list",
    default=None,
    help="List of files to parse",
    type=click.Path(exists=True, file_okay=True, dir_okay=False),
)
def parse_missing_files_in_pmc(file_list: Optional[str]):
    """Parse missing files in PMC."""
    unzipped_dir = Path("data/pipelines/pmc/unzipped/")
    parsed_dir = Path("data/pipelines/pmc/parsed/")
    files_in_parsed = set([x.stem for x in parsed_dir.glob("*.json")])
    # retrieve the list of missing files from the unzipped directory

    if file_list is None:
        missing_files = [Path(x) for x in unzipped_dir.glob("*.xml") if x.stem not in files_in_parsed]
        if len(missing_files) == 0:
            click.echo("No missing files found")
            return
        click.echo(f"Found {len(missing_files)} missing files")
        file_list = missing_files
    else:
        file_list = Path(file_list).open().read().splitlines()

    click.echo(f"Parsing {len(file_list)} missing files in PMC...")
    parsed_files, failed_files = pipeline_parse_missing_files_in_pmc(file_list)
    click.echo(f"DONE: Parsed {len(parsed_files)} files, failed {len(failed_files)} files")

    if len(failed_files) > 0:
        click.echo(f"Failed files: {failed_files}")
        if file_list is None or not isinstance(file_list, Path):
            file_list = Path("data/pipelines/pmc/failed_files.txt")
        updated_file_list = file_list.stem + datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d-%H-%M-%S") + ".txt"

        with open(updated_file_list, 'w') as f:
            f.write("\n".join(failed_files))

        raise click.ClickException(f"Failed to parse some files, updated file list: {updated_file_list}")
