import json
import tempfile
from pathlib import Path
from typing import Optional, Union

import supabase
from google.cloud import storage
from tenacity import retry, stop_after_attempt, wait_exponential

from literature_ingest.models import Document
from literature_ingest.utils.config import settings
from literature_ingest.utils.logging import get_logger

logger = get_logger(__name__, "info")


def get_supabase_client():
    """Create and return a Supabase client."""
    return supabase.create_client(
        settings.SUPABASE_URL,
        settings.SUPABASE_KEY,
        options=supabase.ClientOptions(
            postgrest_client_timeout=30,
            storage_client_timeout=30,
            schema="public",
        ),
    )


def download_from_gcs(gcs_path: str) -> Optional[Path]:
    """
    Download a file from Google Cloud Storage.

    Args:
        gcs_path: GCS path in the format 'gs://bucket_name/path/to/file'

    Returns:
        Path to the downloaded file or None if download failed
    """
    # Parse GCS path
    if not gcs_path.startswith("gs://"):
        logger.error(f"Invalid GCS path format: {gcs_path}")
        return None

    # Remove 'gs://' prefix and split into bucket and blob path
    path_parts = gcs_path[5:].split("/", 1)
    if len(path_parts) != 2:
        logger.error(f"Invalid GCS path format: {gcs_path}")
        return None

    bucket_name, blob_path = path_parts

    # Initialize GCS client
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_path)

    # Create a temporary file to download to
    temp_file = tempfile.NamedTemporaryFile(delete=False, suffix=".json")
    temp_path = Path(temp_file.name)
    temp_file.close()

    try:
        # Download the blob to the temporary file
        blob.download_to_filename(str(temp_path))
        logger.info(f"Downloaded {gcs_path} to {temp_path}")
        return temp_path
    except Exception as e:
        logger.error(f"Error downloading {gcs_path}: {str(e)}")
        if temp_path.exists():
            temp_path.unlink()
        return None


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
def query_document_by_ids(pmcid: Optional[str] = None, doi: Optional[str] = None, table_name: str = "pmc_records") -> Optional[Document]:
    """
    Query Supabase for a document by PMCID and DOI, download the JSON file from GCS,
    and load it into a Document object.

    Args:
        pmcid: PMC ID to search for
        doi: DOI to search for
        table_name: Supabase table name to query (default: "pmc_records")

    Returns:
        Document object if found and successfully loaded, None otherwise
    """
    if not pmcid and not doi:
        logger.error("At least one of PMCID or DOI must be provided")
        return None

    # Get Supabase client
    client = get_supabase_client()

    # Build query
    query = client.table(table_name).select("*")

    # Search by PMCID first if provided
    if pmcid:
        query = query.eq("pmcid", pmcid)
    # If no PMCID or no results found with PMCID, search by DOI
    elif doi:
        query = query.eq("doi", doi)

    # Execute query
    try:
        result = query.execute()

        if not result.data:
            # If no results with PMCID, try DOI if it was provided but not used yet
            if pmcid and doi:
                logger.info(f"No results found for PMCID {pmcid}, trying DOI {doi}")
                result = client.table(table_name).select("*").eq("doi", doi).execute()

            # If still no results
            if not result.data:
                logger.info(f"No document found with PMCID={pmcid}, DOI={doi} in table {table_name}")
                return None

        # Get the first matching record
        record = result.data[0]

        # Get the GCS path
        gcs_path = record.get("parsed_gcs_path")
        if not gcs_path:
            logger.error(f"No parsed_gcs_path found in record: {record}")
            return None

        # Download the file from GCS
        local_path = download_from_gcs(gcs_path)
        if not local_path:
            return None

        try:
            # Load the JSON file into a Document object
            with open(local_path, "r") as f:
                doc_json = json.load(f)

            # Create Document object
            document = Document.model_validate(doc_json)

            # Clean up the temporary file
            local_path.unlink()

            return document
        except Exception as e:
            logger.error(f"Error loading document from {local_path}: {str(e)}")
            if local_path.exists():
                local_path.unlink()
            return None

    except Exception as e:
        logger.error(f"Error querying Supabase: {str(e)}")
        return None
