import datetime
from pathlib import Path
import sys
from typing import List, Optional, Union

import click
from literature_ingest.data_engineering import unzip_and_filter
from literature_ingest.pipelines import pipeline_download_pubmed
from literature_ingest.pmc import (
    PMC_OPEN_ACCESS_NONCOMMERCIAL_XML_DIR,
    PUBMED_OPEN_ACCESS_DIR,
    PMCFTPClient,
    PMCParser,
    PubMedFTPClient,
)
from literature_ingest.pubmed import PubMedParser
from literature_ingest.utils.logging import get_logger
from literature_ingest.utils.config import settings
from literature_ingest.models import Document

from tenacity import retry, stop_after_attempt, wait_exponential

import supabase

import shutil
from google.cloud import storage
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor
from functools import partial
import logging
import csv
import pandas as pd
from itertools import islice
import json
from tenacity import retry, stop_after_attempt, wait_exponential

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


@cli.group()
def pipelines():
    """Commands for running various data processing pipelines."""
    pass


@cli.command()
@click.argument("input_dir", type=click.Path(exists=True, file_okay=False))
@click.argument("output_dir", type=click.Path(file_okay=False))
def unzip_pmc(input_dir: str, output_dir: str):
    """Unzip PMC files."""
    unzip_and_filter(
        input_dir, output_dir, extension=".xml", use_gsutil=False, overwrite=True
    )


@cli.command()
@click.argument("input_dir", type=click.Path(exists=True, file_okay=False))
@click.argument("output_dir", type=click.Path(file_okay=False))
def parse_pmc(input_dir: str, output_dir: str):
    """Parse multiple PMC XML documents from a directory.

    INPUT_DIR: Directory containing PMC XML files
    OUTPUT_DIR: Directory where parsed documents should be saved
    """
    pattern = "*.xml"

    try:
        input_path = Path(input_dir)
        output_path = Path(output_dir)

        # Create output directory if it doesn't exist
        output_path.mkdir(parents=True, exist_ok=True)

        # Get all XML files in input directory
        xml_files = list(input_path.glob(pattern))
        if not xml_files:
            raise click.ClickException(
                f"No files matching pattern '{pattern}' found in {input_dir}"
            )

        logger.info(f"Found {len(xml_files)} files to process")
        parser = PMCParser()

        documents = parser.parse_docs(
            xml_files, output_path, use_threads=True, max_threads=settings.MAX_WORKERS
        )

        click.echo(f"Successfully processed {len(documents)} files")

    except Exception as e:
        logger.error(f"Error processing documents: {str(e)}")
        raise click.ClickException(str(e))


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
    baseline_files_downloaded = pmc_downloader._download_pmc_baselines(
        raw_dir, dry_run=False, overwrite=False
    )
    click.echo("Downloading PMC incremental...")
    incremental_files_downloaded = pmc_downloader._download_pmc_incremental(
        raw_dir, dry_run=False, overwrite=False
    )
    click.echo(
        f"Downloaded {len(baseline_files_downloaded) + len(incremental_files_downloaded)} "
        "files... Files already stored are not downloaded again and counter here."
    )


@cli.command()
@click.argument("input_dir", type=click.Path(exists=True, file_okay=False))
@click.option(
    "--batch-size", default=1000, help="Number of records to insert in each batch"
)
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

    def get_id_by_type(ids, id_type):
        """Helper function to get ID value by type"""
        for doc_id in ids:
            if doc_id.type == id_type:
                return doc_id.id
        return None

    for file in input_dir.glob("*.json"):
        with open(file, "r") as f:
            doc = Document.model_validate_json(f.read())

        records.append(
            {
                "pmid": get_id_by_type(doc.ids, "pubmed"),
                "pmcid": get_id_by_type(doc.ids, "pmc"),
                "doi": get_id_by_type(doc.ids, "doi"),
                "filename": file.name,
                "title": doc.title,
                "year": doc.year,
            }
        )

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
        click.echo(
            f"Inserted final batch of {inserted} records. Total: {total_inserted}"
        )

    click.echo(f"Successfully inserted {total_inserted} records in total")


def batch_insert_records(client, records: list, table_name: str) -> int:
    """Insert a batch of records and return number of successful inserts."""
    try:
        result = client.table(table_name).insert(records).execute()
        return len(result.data)
    except Exception as e:
        sys.exit(1)
        logger.error(f"Error inserting batch: {str(e)}")
        return 0


@cli.command()
def download_pubmed():
    """Download PubMed data."""
    click.echo("Ingesting PubMed data...")
    base_dir = Path("data/pipelines/pubmed")

    # Define directories
    raw_dir = base_dir / "raw"

    # Create directories
    raw_dir.mkdir(parents=True, exist_ok=True)

    # Add retry decorator to handle transient failures
    @retry(
        stop=stop_after_attempt(5),  # Try 5 times
        wait=wait_exponential(
            multiplier=1, min=4, max=60
        ),  # Wait between 4 and 60 seconds, exponentially increasing
        reraise=True,
    )
    def download_with_retry(downloader, raw_dir):
        return pipeline_download_pubmed(raw_dir)

    # Download data
    pubmed_downloader = PubMedFTPClient()
    click.echo("Downloading PubMed baselines...")

    try:
        baseline_files_downloaded, baseline_date = download_with_retry(
            pubmed_downloader, raw_dir
        )
        dated_raw_dir = raw_dir / baseline_date if baseline_date else raw_dir

        click.echo(f"Downloaded {len(baseline_files_downloaded)} files...")
        click.echo("DONE: Download PubMed data")
    except Exception as e:
        logger.error(f"Failed to download PubMed data after retries: {str(e)}")
        raise click.ClickException(f"Failed to download PubMed data: {str(e)}")


def upload_file(bucket, directory, unzipped_file):
    try:
        blob_name = f"{directory}/{unzipped_file.name}"
        blob = bucket.blob(blob_name)
        blob.upload_from_filename(str(unzipped_file))
        return True
    except Exception as e:
        logging.error(f"Failed to upload {unzipped_file}: {str(e)}")
        return False


@cli.command()
@click.argument("input_dir", type=click.Path(exists=True, file_okay=False))
@click.argument("batch_size", type=int, default=1)
@click.option(
    "--test-run",
    is_flag=True,
    default=False,
    help="Run in test mode (only process first batch)",
)
def process_pmc(input_dir: str, batch_size: int, test_run: bool):
    """Process PMC data in batches and extract metadata.

    INPUT_DIR: Directory containing raw PMC .tar.gz files
    """
    click.echo("Processing PMC data...")
    input_dir = Path(input_dir)
    base_dir = Path("data/pipelines/pmc")
    metadata_dir = base_dir / "metadata"
    metadata_dir.mkdir(parents=True, exist_ok=True)

    metadata_file_stem = "pmc_metadata"

    # Process files in batches
    archive_files = list(input_dir.glob("**/*.tar.gz"))
    if not archive_files:
        raise click.ClickException(f"No .tar.gz files found in {input_dir}")

    click.echo(f"Found {len(archive_files)} archive files to process")

    def get_id_by_type(ids, id_type):
        """Helper function to get ID value by type"""
        for doc_id in ids:
            if doc_id.type == id_type:
                return doc_id.id
        return None

    # Initialize GCS client
    storage_client = storage.Client()
    bucket = storage_client.bucket(settings.PROD_BUCKET)
    max_workers = settings.MAX_WORKERS  # Increased for I/O bound operations

    # Get bucket name for constructing gs:// paths
    bucket_name = settings.PROD_BUCKET

    for i in range(0, len(archive_files), batch_size):
        metadata_records = []
        batch = archive_files[i : i + batch_size]
        click.echo(
            f"\nProcessing batch {i//batch_size + 1}/{(len(archive_files) + batch_size - 1)//batch_size}"
        )

        for archive_file in batch:
            click.echo(f"\nProcessing {archive_file.name}")

            # Create batch-specific directories
            batch_dir = base_dir / "batches" / archive_file.stem
            unzipped_dir = batch_dir / "unzipped"
            parsed_dir = batch_dir / "parsed"

            unzipped_dir.mkdir(parents=True, exist_ok=True)
            parsed_dir.mkdir(parents=True, exist_ok=True)

            # Unzip
            click.echo("Unzipping...")
            unzipped_files = unzip_and_filter(
                archive_file,
                unzipped_dir,
                extension=".xml",
                use_gsutil=False,
                overwrite=True,
            )
            click.echo(f"Unzipped {len(unzipped_files)} files")

            # Parse
            click.echo("Parsing...")
            xml_files = list(unzipped_dir.glob("*.xml"))

            # Parse
            parser = PMCParser()
            parsed_dir.mkdir(parents=True, exist_ok=True)

            click.echo(f"Parsing {len(unzipped_files)} files...")
            parsed_files = parser.parse_docs(
                unzipped_files,
                parsed_dir,
                use_threads=True,
                max_threads=settings.MAX_WORKERS,
            )
            click.echo(f"Parsed {len(parsed_files)} files...")

            # Extract metadata and store GCS paths
            click.echo("Extracting metadata...")
            for json_file in parsed_dir.glob("*.json"):
                with open(json_file, "r") as f:
                    doc = Document.model_validate_json(f.read())

                # Construct GCS paths
                parsed_gcs_path = f"gs://{bucket_name}/pmc/parsed/{json_file.name}"
                xml_name = json_file.stem + ".xml"  # Original XML file name
                unzipped_gcs_path = (
                    f"gs://{bucket_name}/pmc/unzipped/{archive_file.stem}/{xml_name}"
                )

                metadata_records.append(
                    {
                        "pmid": get_id_by_type(doc.ids, "pubmed"),
                        "pmcid": get_id_by_type(doc.ids, "pmc"),
                        "doi": get_id_by_type(doc.ids, "doi"),
                        "filename": json_file.name,
                        "title": doc.title,
                        "year": doc.year,
                        "archive_file": archive_file.name,
                        "parsed_gcs_path": parsed_gcs_path,
                        "unzipped_gcs_path": unzipped_gcs_path,
                    }
                )

            # Upload to GCS
            click.echo("Uploading files to GCS...")
            start_time = datetime.datetime.now()

            # Upload unzipped files - maintain archive structure
            unzipped_files = list(unzipped_dir.glob("**/*"))
            unzipped_files = [f for f in unzipped_files if f.is_file()]

            # Create partial function for unzipped files with archive-specific directory
            unzipped_upload_fn = partial(
                upload_file, bucket, f"pmc/unzipped/{archive_file.stem}"
            )

            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                results = list(
                    tqdm(
                        executor.map(unzipped_upload_fn, unzipped_files),
                        total=len(unzipped_files),
                        desc="Uploading unzipped files",
                    )
                )

            unzip_upload_time = datetime.datetime.now() - start_time
            click.echo(
                f"Uploaded {len(unzipped_files)} unzipped files in {unzip_upload_time}"
            )

            # Upload parsed files - flat directory
            start_time = datetime.datetime.now()
            parsed_files = list(parsed_dir.glob("**/*"))
            parsed_files = [f for f in parsed_files if f.is_file()]

            parsed_upload_fn = partial(upload_file, bucket, "pmc/parsed")

            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                results = list(
                    tqdm(
                        executor.map(parsed_upload_fn, parsed_files),
                        total=len(parsed_files),
                        desc="Uploading parsed files",
                    )
                )

            parse_upload_time = datetime.datetime.now() - start_time
            click.echo(
                f"Uploaded {len(parsed_files)} parsed files in {parse_upload_time}"
            )

            if test_run:
                break

            # Cleanup local directories
            shutil.rmtree(unzipped_dir)
            shutil.rmtree(parsed_dir)
            click.echo(f"Cleaned up local directories: {unzipped_dir} and {parsed_dir}")

        # Save metadata after each batch
        df = pd.DataFrame(metadata_records)
        metadata_file = metadata_dir / f"{metadata_file_stem}_{i}.csv"
        df.to_csv(metadata_file, index=False)
        click.echo(
            f"Saved metadata for {len(metadata_records)} documents to {metadata_file}"
        )

    click.echo("\nAll processing complete!")


@cli.command()
@click.argument("input_dir", type=click.Path(exists=True, file_okay=False))
@click.argument("batch_size", type=int, default=1)
@click.option(
    "--test-run",
    is_flag=True,
    default=False,
    help="Run in test mode (only process first batch)",
)
def process_pubmed(input_dir: str, batch_size: int, test_run: bool):
    """Process PubMed data in batches and extract metadata.

    INPUT_DIR: Directory containing raw PubMed .xml.gz files
    """
    click.echo("Processing PubMed data...")
    input_dir = Path(input_dir)
    base_dir = Path("data/pipelines/pubmed")
    metadata_dir = base_dir / "metadata"
    metadata_dir.mkdir(parents=True, exist_ok=True)

    metadata_file_stem = "pubmed_metadata"

    # Process files in batches
    archive_files = list(input_dir.glob("**/*.xml.gz"))
    if not archive_files:
        raise click.ClickException(f"No .xml.gz files found in {input_dir}")

    click.echo(f"Found {len(archive_files)} archive files to process")

    def get_id_by_type(ids, id_type):
        """Helper function to get ID value by type"""
        for doc_id in ids:
            if doc_id.type == id_type:
                return doc_id.id
        return None

    # Initialize GCS client
    storage_client = storage.Client()
    bucket = storage_client.bucket(settings.PROD_BUCKET)
    max_workers = settings.MAX_WORKERS

    # Get bucket name for constructing gs:// paths
    bucket_name = settings.PROD_BUCKET

    for i in range(0, len(archive_files), batch_size):
        metadata_records = []
        batch = archive_files[i : i + batch_size]
        click.echo(
            f"\nProcessing batch {i//batch_size + 1}/{(len(archive_files) + batch_size - 1)//batch_size}"
        )

        for archive_file in batch:
            click.echo(f"\nProcessing {archive_file.name}")

            # Create batch-specific directories
            batch_dir = base_dir / "batches" / archive_file.stem
            unzipped_dir = batch_dir / "unzipped"
            parsed_dir = batch_dir / "parsed"

            unzipped_dir.mkdir(parents=True, exist_ok=True)
            parsed_dir.mkdir(parents=True, exist_ok=True)

            # Unzip
            click.echo("Unzipping...")
            unzipped_files = unzip_and_filter(
                archive_file,
                unzipped_dir,
                extension=".xml",
                use_gsutil=False,
                overwrite=True,
            )
            click.echo(f"Unzipped {len(unzipped_files)} files")

            # Parse
            click.echo("Parsing...")
            xml_files = list(unzipped_dir.glob("*.xml"))
            click.echo(f"Parsing {len(xml_files)} files...")

            parser = PubMedParser()
            parsed_dir.mkdir(parents=True, exist_ok=True)

            print(f"Parsing {len(unzipped_files)} files...")
            parsed_files = parser.parse_docs(
                unzipped_files,
                parsed_dir,
                use_threads=True,
                max_threads=settings.MAX_WORKERS,
            )

            print(f"Parsed {len(parsed_files)} files...")
            print("DONE: Parse PubMed data")

            click.echo(f"Parsed {len(parsed_files)} files...")

            # Extract metadata and store GCS paths
            click.echo("Extracting metadata...")
            for json_file in parsed_dir.glob("*.json"):
                with open(json_file, "r") as f:
                    doc = Document.model_validate_json(f.read())

                # Construct GCS paths
                parsed_gcs_path = f"gs://{bucket_name}/pubmed/parsed/{json_file.name}"
                xml_name = json_file.stem + ".xml"  # Original XML file name
                unzipped_gcs_path = (
                    f"gs://{bucket_name}/pubmed/unzipped/{archive_file.stem}/{xml_name}"
                )

                metadata_records.append(
                    {
                        "pmid": get_id_by_type(doc.ids, "pubmed"),
                        "pmcid": get_id_by_type(doc.ids, "pmc"),
                        "doi": get_id_by_type(doc.ids, "doi"),
                        "filename": json_file.name,
                        "title": doc.title,
                        "year": doc.year,
                        "archive_file": archive_file.name,
                        "parsed_gcs_path": parsed_gcs_path,
                        "unzipped_gcs_path": unzipped_gcs_path,
                    }
                )

            # Upload to GCS
            click.echo("Uploading files to GCS...")
            start_time = datetime.datetime.now()

            # Upload unzipped files - maintain archive structure
            unzipped_files = list(unzipped_dir.glob("**/*"))
            unzipped_files = [f for f in unzipped_files if f.is_file()]

            # Create partial function for unzipped files with archive-specific directory
            unzipped_upload_fn = partial(
                upload_file, bucket, f"pubmed/unzipped/{archive_file.stem}"
            )

            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                results = list(
                    tqdm(
                        executor.map(unzipped_upload_fn, unzipped_files),
                        total=len(unzipped_files),
                        desc="Uploading unzipped files",
                    )
                )

            unzip_upload_time = datetime.datetime.now() - start_time
            click.echo(
                f"Uploaded {len(unzipped_files)} unzipped files in {unzip_upload_time}"
            )

            # Upload parsed files - flat directory
            start_time = datetime.datetime.now()
            parsed_files = list(parsed_dir.glob("**/*"))
            parsed_files = [f for f in parsed_files if f.is_file()]

            parsed_upload_fn = partial(upload_file, bucket, "pubmed/parsed")

            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                results = list(
                    tqdm(
                        executor.map(parsed_upload_fn, parsed_files),
                        total=len(parsed_files),
                        desc="Uploading parsed files",
                    )
                )

            parse_upload_time = datetime.datetime.now() - start_time
            click.echo(
                f"Uploaded {len(parsed_files)} parsed files in {parse_upload_time}"
            )

            if test_run:
                break

            # Cleanup local directories
            shutil.rmtree(unzipped_dir)
            shutil.rmtree(parsed_dir)
            click.echo(f"Cleaned up local directories: {unzipped_dir} and {parsed_dir}")

        # Save metadata after each batch
        df = pd.DataFrame(metadata_records)
        metadata_file = metadata_dir / f"{metadata_file_stem}_{i}.csv"
        df.to_csv(metadata_file, index=False)
        click.echo(
            f"Saved metadata for {len(metadata_records)} documents to {metadata_file}"
        )

    click.echo("\nAll processing complete!")


@cli.command()
@click.argument("metadata_dir", type=click.Path(exists=True, file_okay=False))
@click.option(
    "--table-name",
    default="literature_metadata",
    help="Name of the Supabase table to insert records into",
)
@click.option(
    "--batch-size", default=1000, help="Number of records to insert in each batch"
)
@click.option(
    "--source",
    type=click.Choice(["PMC", "PUBMED", "ALL"], case_sensitive=False),
    default="ALL",
    help="Source of metadata files to process (PMC, PUBMED, or ALL)",
)
@click.option(
    "--dry-run",
    is_flag=True,
    default=False,
    help="Run in dry-run mode (load files but don't upload to Supabase)",
)
def upload_metadata(metadata_dir: str, table_name: str, batch_size: int, source: str, dry_run: bool):
    """Upload metadata from CSV files to Supabase.

    METADATA_DIR: Directory containing metadata CSV files
    """
    metadata_dir = Path(metadata_dir)

    if dry_run:
        click.echo(f"DRY RUN: Processing metadata files from {metadata_dir} (no data will be sent to Supabase)")
    else:
        click.echo(f"Processing metadata files from {metadata_dir}")

    # Determine which files to process based on source
    if source.upper() == "PMC":
        file_pattern = "pmc_metadata_*.csv"
    elif source.upper() == "PUBMED":
        file_pattern = "pubmed_metadata_*.csv"
    else:  # ALL
        file_pattern = "*metadata_*.csv"

    # Find all matching CSV files
    csv_files = list(metadata_dir.glob(file_pattern))
    if not csv_files:
        raise click.ClickException(
            f"No metadata CSV files found matching pattern '{file_pattern}' in {metadata_dir}"
        )

    click.echo(f"Found {len(csv_files)} metadata files to process")

    # Create Supabase client (only if not in dry run mode)
    supabase_client = None
    if not dry_run:
        supabase_client = supabase.create_client(
            settings.SUPABASE_URL,
            settings.SUPABASE_KEY,
            options=supabase.ClientOptions(
                postgrest_client_timeout=30,
                storage_client_timeout=30,
                schema="public",
            ),
        )

    # Process all files and concatenate data
    all_records = []
    total_records = 0

    # Track statistics for each file
    file_stats = {}
    nan_counts = {}

    for csv_file in csv_files:
        click.echo(f"Reading {csv_file.name}...")
        df = pd.read_csv(csv_file)

        # Count NaN values before replacing them
        nan_counts[csv_file.name] = {col: df[col].isna().sum() for col in df.columns}

        # Replace NaN values with None for JSON compatibility
        df = df.replace({pd.NA: None, float('nan'): None})

        # Replace None with empty string specifically for ID fields
        id_columns = ['pmid', 'pmcid', 'doi']
        for col in id_columns:
            if col in df.columns:
                df[col] = df[col].fillna('')

        records = df.to_dict(orient="records")

        # Collect statistics for this file
        file_stats[csv_file.name] = {
            "record_count": len(records),
            "columns": list(df.columns),
            "non_null_counts": {col: df[col].count() for col in df.columns},
            "null_counts": {col: df[col].isna().sum() for col in df.columns}
        }

        total_records += len(records)

        # Only add to all_records if not in dry run mode
        if not dry_run:
            all_records.extend(records)

    click.echo(f"Loaded {total_records} records from {len(csv_files)} files")

    # Print detailed statistics in dry run mode
    if dry_run:
        click.echo("\n=== DRY RUN SUMMARY ===")
        click.echo(f"Total files: {len(csv_files)}")
        click.echo(f"Total records: {total_records}")

        # Print per-file statistics
        click.echo("\nPer-file statistics:")
        for filename, stats in file_stats.items():
            click.echo(f"\n  {filename}:")
            click.echo(f"    Records: {stats['record_count']}")

            # Print column statistics
            click.echo("    Column statistics:")
            for col in stats['columns']:
                non_null = stats['non_null_counts'][col]
                null = stats['null_counts'][col]
                percentage = (non_null / stats['record_count']) * 100 if stats['record_count'] > 0 else 0
                click.echo(f"      {col}: {non_null} non-null values ({percentage:.1f}%), {null} null values")

            # Print NaN counts
            click.echo("    NaN values found (replaced with None):")
            for col, count in nan_counts[filename].items():
                if count > 0:
                    click.echo(f"      {col}: {count} NaN values")

        click.echo("\nDRY RUN COMPLETE - No data was sent to Supabase")
        return

    # Upload data in batches (only if not in dry run mode)
    total_inserted = 0

    for i in range(0, len(all_records), batch_size):
        batch = all_records[i : i + batch_size]
        click.echo(
            f"Uploading batch {i//batch_size + 1}/{(len(all_records) + batch_size - 1)//batch_size} ({len(batch)} records)"
        )

        try:
            inserted = batch_insert_records(supabase_client, batch, table_name)
            total_inserted += inserted
            click.echo(f"Inserted batch of {inserted} records. Total: {total_inserted}")
        except Exception as e:
            logger.error(f"Error inserting batch: {str(e)}")
            click.echo(f"Error inserting batch: {str(e)}")

            # Print a sample of the problematic batch for debugging
            if len(batch) > 0:
                click.echo(f"Sample record from failed batch: {batch[0]}")

    click.echo(
        f"\nUpload complete! Successfully inserted {total_inserted} out of {total_records} records into {table_name}"
    )
