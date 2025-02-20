import datetime
from pathlib import Path
from typing import List, Optional, Union
from xml.dom.minidom import Document
import click
from literature_ingest.data_engineering import unzip_and_filter
from literature_ingest.normalization import normalize_document
from literature_ingest.pipelines import pipeline_download_pubmed, pipeline_parse_missing_files_in_pmc, pipeline_parse_pmc, pipeline_parse_pubmed, pipeline_unzip_pubmed
from literature_ingest.pmc import PMC_OPEN_ACCESS_NONCOMMERCIAL_XML_DIR, PUBMED_OPEN_ACCESS_DIR, PMCFTPClient, PMCParser, PubMedFTPClient
from literature_ingest.utils.logging import get_logger
from literature_ingest.utils.config import settings
import supabase

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
@click.argument("unzipped_dir", type=click.Path(exists=True, dir_okay=False))
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
    upload_fn = partial(upload_file, bucket, "pmc/unzipped/{unzipped_dir.name}")

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

    upload_fn = partial(upload_file, bucket, "pmc/parsed")

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


@cli.command()
def download_pmc():
    """Download PMC data."""
    click.echo("Ingesting PMC data...")
    base_dir = Path("data/pipelines/pmc")

    # Define directories
    raw_dir = base_dir / "raw"

    # Create directories
    raw_dir.mkdir(parents=True, exist_ok=True)

    # Download data
    pmc_downloader = PMCFTPClient()

    click.echo("Downloading PMC...")
    click.echo("Downloading PMC Baselines (full)...")
    baseline_files_downloaded = pmc_downloader._download_pmc_baselines(raw_dir)
    click.echo("Downloading PMC incremental...")
    incremental_files_downloaded = pmc_downloader._download_pmc_incremental(raw_dir)
    click.echo(f"Downloaded {len(baseline_files_downloaded) + len(incremental_files_downloaded)} "
               "files... Files already stored are not downloaded again and counter here.")

@cli.command()
@click.argument("input_dir", type=click.Path(exists=True, file_okay=False))
def unzip_and_parse_pubmed(input_dir: Path):
    """Unzip and parse PubMed data."""
    input_dir = Path(input_dir)
    click.echo("Unzipping and parsing PubMed data...")
    base_dir = Path("data/pipelines/pubmed")

    unzipped_dir = base_dir / "unzipped" / input_dir.name
    parsed_dir = base_dir / "parsed"

    unzipped_dir.mkdir(parents=True, exist_ok=True)
    parsed_dir.mkdir(parents=True, exist_ok=True)

    unzipped_files = unzip_and_filter(input_dir, unzipped_dir, extension=".xml", use_gsutil=False, overwrite=True)
    click.echo(f"Unzipped {len(unzipped_files)} files to {unzipped_dir}...")

    # get all files from the unzipped directory
    unzipped_files = list(unzipped_dir.glob("*.xml"))

    parsed_files = pipeline_parse_pubmed(unzipped_files, parsed_dir)
    click.echo(f"Parsed {len(parsed_files)} files to {parsed_dir}...")


@cli.command()
@click.argument("input_dir", type=click.Path(exists=True, file_okay=False))
@click.option("--batch-size", default=1000, help="Number of records to insert in each batch")
def data_extraction(input_dir: Path, batch_size: int):
    """Extract and batch insert records from JSON files."""
    input_dir = Path(input_dir)
    click.echo("Extracting IDs from data...")
    records = []
    total_inserted = 0

    # Create Supabase client once
    supabase_client = supabase.create_client(
        settings.SUPABASE_URL,
        settings.SUPABASE_KEY,
        options=supabase.ClientOptions(
            postgrest_client_timeout=30,
            storage_client_timeout=30,
            schema="public",
        ),
    )

    for file in input_dir.glob("*.json"):
        with open(file, "r") as f:
            doc = Document.model_validate_json(f.read())
        doc_ids = doc.get_ids()
        records.append({
            "pmid": doc_ids.pmid,
            "pmcid": doc_ids.pmcid,
            "doi": doc_ids.doi,
            "filename": file.name,
            "title": doc.title,
            "year": doc.year,
        })

        # When batch size is reached, insert records
        if len(records) >= batch_size:
            inserted = batch_insert_records(supabase_client, records, "pubmed_records")
            total_inserted += inserted
            records = []  # Clear the records list
            click.echo(f"Inserted batch of {inserted} records. Total: {total_inserted}")

    # Insert any remaining records
    if records:
        inserted = batch_insert_records(supabase_client, records, "pubmed_records")
        total_inserted += inserted
        click.echo(f"Inserted final batch of {inserted} records. Total: {total_inserted}")

    click.echo(f"Successfully inserted {total_inserted} records in total")

def batch_insert_records(client, records: list, table_name: str) -> int:
    """Insert a batch of records and return number of successful inserts."""
    try:
        result = client.table(table_name).insert(records).execute()
        return len(result.data)
    except Exception as e:
        logger.error(f"Error inserting batch: {str(e)}")
        return 0

@cli.command()
def download_pubmed():
    """Download PubMed data."""
    click.echo("Ingesting PubMed data...")
    base_dir = Path("data/pipelines/pubmed")

    # Define directories
    raw_dir = base_dir / "raw"

    click.echo("Ingesting PubMed data...")
    base_dir = Path("data/pipelines/pubmed")

    # Define directories
    raw_dir = base_dir / "raw"

    # Create directories
    raw_dir.mkdir(parents=True, exist_ok=True)

    # Download data
    pubmed_downloader = PubMedFTPClient()
    click.echo("Downloading PubMed baselines...")

    baseline_files_downloaded, baseline_date = pipeline_download_pubmed(raw_dir)
    dated_raw_dir = raw_dir / baseline_date if baseline_date else raw_dir

    click.echo(f"Downloaded {len(baseline_files_downloaded)} files...")
    click.echo("DONE: Download PubMed data")


def upload_file(bucket, directory, unzipped_file):
    try:
        blob_name = f"{directory}/{unzipped_file.name}"
        blob = bucket.blob(blob_name)
        blob.upload_from_filename(str(unzipped_file))
        return True
    except Exception as e:
        logging.error(f"Failed to upload {unzipped_file}: {str(e)}")
        return False
