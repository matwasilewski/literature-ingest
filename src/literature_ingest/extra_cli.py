from pathlib import Path
from typing import List, Optional, Union
from xml.dom.minidom import Document
import click
from literature_ingest.pmc import PMC_OPEN_ACCESS_NONCOMMERCIAL_XML_DIR, PUBMED_OPEN_ACCESS_DIR, PMCFTPClient, PMCParser, PubMedFTPClient
from literature_ingest.utils.logging import get_logger
from literature_ingest.utils.config import settings

from google.cloud import storage
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor
from functools import partial
from literature_ingest.cli import cli, get_client


logger = get_logger(__name__, "info")
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
