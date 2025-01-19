from pathlib import Path
from typing import Union
import click
from cloudpathlib import CloudPath
from literature_ingest.pipelines import parse_pmc_docs_sample
from literature_ingest.pmc import PMCFTPClient, PMCParser
from literature_ingest.utils.logging import get_logger

logger = get_logger(__name__, "info")

def convert_to_cloudpath(path: Path) -> Union[CloudPath, Path]:
    if str(path).startswith(("s3://", "gs://", "az://")):
        return CloudPath(str(path))
    return path

def get_client(source: str):
    """Get the appropriate client based on source."""
    if source.upper() == "PMC":
        return PMCFTPClient()
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
    target = convert_to_cloudpath(target)
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

    INPUT_PATH: Path to the input PMC XML file
    OUTPUT_PATH: Path where the parsed document should be saved
    """
    try:
        # Read input file
        with open(input_path, 'r') as f:
            xml_content = f.read()

        # Parse document
        parser = PMCParser()
        doc = parser.parse_doc(xml_content)

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
    base_dir = convert_to_cloudpath(base_dir)

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
    base_dir = convert_to_cloudpath(base_dir)

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

        # Process each file
        for xml_file in xml_files:
            try:
                # Read input file
                with open(xml_file, 'r') as f:
                    xml_content = f.read()

                # Parse document
                doc = parser.parse_doc(xml_content)

                # Create output filename with appropriate extension
                output_ext = ".json" if format == "json" else ".txt"
                output_file = output_path / (xml_file.stem + output_ext)

                # Write output
                with open(output_file, 'w') as f:
                    if format == "raw":
                        f.write(doc.to_raw_text())
                    else:
                        f.write(doc.to_json())

                logger.info(f"Processed {xml_file.name} -> {output_file.name}")

            except Exception as e:
                logger.error(f"Error processing {xml_file.name}: {str(e)}")
                continue

        click.echo(f"Successfully processed {len(xml_files)} files")

    except Exception as e:
        logger.error(f"Error processing documents: {str(e)}")
        raise click.ClickException(str(e))

@cli.command()
@click.option(
    "--start_from_parse",
    is_flag=True,
    help="Show what would be downloaded without actually downloading",
)
def parse_sample(start_from_parse: bool):
    parse_pmc_docs_sample(start_from_parsed=start_from_parse)

if __name__ == "__main__":
    cli()
