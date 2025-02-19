import datetime
from pathlib import Path
from typing import List, Optional, Union
import click
from literature_ingest.data_engineering import unzip_and_filter
from literature_ingest.normalization import normalize_document
from literature_ingest.pipelines import pipeline_download_pubmed, pipeline_parse_missing_files_in_pmc, pipeline_parse_pmc, pipeline_parse_pubmed, pipeline_unzip_pubmed
from literature_ingest.pmc import PMC_OPEN_ACCESS_NONCOMMERCIAL_XML_DIR, PUBMED_OPEN_ACCESS_DIR, PMCFTPClient, PMCParser, PubMedFTPClient
from literature_ingest.utils.logging import get_logger
from literature_ingest.utils.config import settings

import subprocess
import shutil
from google.cloud import storage
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor
from functools import partial
import logging

logger = get_logger(__name__, "info")


def get_client(source: str):
    """Get the appropriate client based on source."""
    if source.upper() == "PMC":
        return PMCFTPClient()
    elif source.upper() == "PUBMED":
        return PubMedFTPClient()
    raise click.ClickException(f"Unknown source: {source}")

@click.group()
def cli():
    """Literature ingest CLI tool for downloading and processing PMC articles."""
    pass

@cli.command()
@click.argument("file", type=str)
@click.argument("target", type=Path)
@click.option(
    "--source",
    default="PMC",
    help="Source to download from (currently supports: PMC)",
    type=str,
)
def get_file(file: str, target: Path, source: str) -> None :
    """Download a file from the specified source."""
    if target.is_dir():
        target = target / file
    client = get_client(source)
    client.connect()
    client.download_file(file, target)
    client.close()
    click.echo(f"Downloaded {file} to {target}")

@cli.command()
@click.argument("input_path", type=click.Path(exists=True, dir_okay=False))
@click.argument("output_path", type=click.Path(dir_okay=False))
@click.option(
    "--format",
    type=click.Choice(["raw", "json"]),
    default="raw",
    help="Output format (raw text or JSON)",
)
def parse_doc(input_path: str, output_path: str, format: str):
    """Parse a single PMC XML document and save the output.

    Normalizes the document.

    INPUT_PATH: Path to the input PMC XML file
    OUTPUT_PATH: Path where the parsed document should be saved
    """
    try:
        # Read input file
        with open(input_path, 'r') as f:
            xml_content = f.read()

        # Parse document
        parser = PMCParser()
        doc = parser.parse_doc(xml_content, Path(input_path))

        # Write output based on format
        with open(output_path, 'w') as f:
            if format == "raw":
                f.write(doc.to_raw_text())
            else:
                f.write(doc.to_json())

        click.echo(f"Successfully parsed {input_path} and saved to {output_path}")

    except Exception as e:
        logger.error(f"Error parsing document: {str(e)}")
        raise click.ClickException(str(e))

@cli.command()
@click.option(
    "--dry-run",
    is_flag=True,
    help="Show what would be downloaded without actually downloading",
)
@click.option(
    "--base-dir",
    default=Path("data/pmc/baselines"),
    help="Directory to store downloaded baseline files",
    type=Path,
)
@click.option(
    "--overwrite",
    is_flag=True,
    help="Overwrite existing files",
)
def download_pmc_baselines(dry_run: bool, base_dir: Path, overwrite: bool):
    """Download baseline files from the PMC FTP server."""
    client = get_client("PMC")
    try:
        logger.info(f"Connecting to PMC FTP server...")
        client.connect()

        logger.info(f"Downloading baselines to {base_dir}")
        client._download_pmc_baselines(base_dir=base_dir, dry_run=dry_run, overwrite=overwrite)

    except Exception as e:
        logger.error(f"Error downloading baselines: {str(e)}")
        raise click.ClickException(str(e))
    finally:
        client.close()

@cli.command()
@click.option(
    "--dry-run",
    is_flag=True,
    help="Show what would be downloaded without actually downloading",
)
@click.option(
    "--base-dir",
    default=Path("data/pmc/incremental"),
    help="Directory to store downloaded incremental files",
    type=Path,
)
@click.option(
    "--overwrite",
    is_flag=True,
    help="Overwrite existing files",
)
def download_pmc_incremental(dry_run: bool, base_dir: Path, overwrite: bool):
    """Download incremental files from the PMC FTP server."""
    client = get_client("PMC")
    try:
        logger.info(f"Connecting to PMC FTP server...")
        client.connect()

        logger.info(f"Downloading incremental to {base_dir}")
        client._download_pmc_incremental(base_dir=base_dir, dry_run=dry_run, overwrite=overwrite)

    except Exception as e:
        logger.error(f"Error downloading incremental: {str(e)}")
        raise click.ClickException(str(e))
    finally:
        client.close()

@cli.command()
@click.argument("input_dir", type=click.Path(exists=True, file_okay=False))
@click.argument("output_dir", type=click.Path(file_okay=False))
@click.option(
    "--format",
    type=click.Choice(["raw", "json"]),
    default="raw",
    help="Output format (raw text or JSON)",
)
@click.option(
    "--pattern",
    default="*.xml",
    help="File pattern to match (e.g. *.xml)",
    type=str,
)
def parse_docs(input_dir: str, output_dir: str, format: str, pattern: str):
    """Parse multiple PMC XML documents from a directory.

    INPUT_DIR: Directory containing PMC XML files
    OUTPUT_DIR: Directory where parsed documents should be saved
    """
    try:
        input_path = Path(input_dir)
        output_path = Path(output_dir)

        # Create output directory if it doesn't exist
        output_path.mkdir(parents=True, exist_ok=True)

        # Get all XML files in input directory
        xml_files = list(input_path.glob(pattern))
        if not xml_files:
            raise click.ClickException(f"No files matching pattern '{pattern}' found in {input_dir}")

        logger.info(f"Found {len(xml_files)} files to process")
        parser = PMCParser()

        documents = parser.parse_docs(xml_files, output_path)

        click.echo(f"Successfully processed {len(documents)} files")

    except Exception as e:
        logger.error(f"Error processing documents: {str(e)}")
        raise click.ClickException(str(e))

@cli.group()
def pipelines():
    """Commands for running various data processing pipelines."""
    pass

@pipelines.command()
@click.argument("unzipped_dir", type=click.Path(exists=True, file_okay=False))
@click.argument("parsed_dir", type=click.Path(exists=True, file_okay=False))
def upload_to_gcs_and_save_space(unzipped_dir: Path, parsed_dir: Path):
    unzipped_dir = Path(unzipped_dir)
    parsed_dir = Path(parsed_dir)
    storage_client = storage.Client()
    bucket = storage_client.bucket(settings.PROD_BUCKET)

    # Upload unzipped files
    click.echo(f"Uploading unzipped files from {unzipped_dir} to GCS...")
    start_time = datetime.datetime.now()
    unzipped_files = list(unzipped_dir.glob('**/*'))
    unzipped_files = [f for f in unzipped_files if f.is_file()]

    max_workers = 60  # Increased for I/O bound operations
    upload_fn = partial(upload_file, bucket, unzipped_dir)

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        results = list(tqdm(
            executor.map(upload_fn, unzipped_files),
            total=len(unzipped_files),
            desc="Uploading unzipped files"
        ))

    unzip_upload_time = datetime.datetime.now() - start_time
    click.echo(f"Uploaded {len(unzipped_files)} unzipped files in {unzip_upload_time}")

    # Upload parsed files
    click.echo(f"Uploading parsed files from {parsed_dir} to GCS...")
    start_time = datetime.datetime.now()
    parsed_files = list(parsed_dir.glob('**/*'))
    parsed_files = [f for f in parsed_files if f.is_file()]

    upload_fn = partial(upload_file, bucket, parsed_dir)

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        results = list(tqdm(
            executor.map(upload_fn, parsed_files),
            total=len(parsed_files),
            desc="Uploading parsed files"
        ))

    parse_upload_time = datetime.datetime.now() - start_time
    click.echo(f"Uploaded {len(parsed_files)} parsed files in {parse_upload_time}")

    # Clean up local directories
    shutil.rmtree(unzipped_dir)
    shutil.rmtree(parsed_dir)
    click.echo(f"Deleted file's {unzipped_dir} and {parsed_dir} processed contents...")


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
        click.echo(f"Downloaded {len(baseline_files_downloaded) + len(incremental_files_downloaded)} files... Files already stored are not downloaded again and counter here.")

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

def upload_file(bucket, file_unzip_dir, unzipped_file):
    try:
        blob_name = f"pmc/unzipped/{file_unzip_dir.name}/{unzipped_file.name}"
        blob = bucket.blob(blob_name)
        blob.upload_from_filename(str(unzipped_file))
        return True
    except Exception as e:
        logging.error(f"Failed to upload {unzipped_file}: {str(e)}")
        return False
