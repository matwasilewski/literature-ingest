from pathlib import Path
from typing import Union
import click
from cloudpathlib import CloudPath
from literature_ingest.pmc import PMCFTPClient
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
@click.option(
    "--dry-run",
    is_flag=True,
    help="Show what would be downloaded without actually downloading",
)
@click.option(
    "--base-dir",
    default=Path("data/baselines"),
    help="Directory to store downloaded baseline files",
    type=Path,
)
def download_pmc_baselines(dry_run: bool, base_dir: Path):
    """Download baseline files from the PMC FTP server."""
    base_dir = convert_to_cloudpath(base_dir)

    client = get_client("PMC")
    try:
        logger.info(f"Connecting to PMC FTP server...")
        client.connect()

        logger.info(f"Downloading baselines to {base_dir}")
        client.download_baselines(base_dir=base_dir, dry_run=dry_run)

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
    default=Path("data/incremental"),
    help="Directory to store downloaded incremental files",
    type=Path,
)
def download_pmc_incremental(dry_run: bool, base_dir: Path):
    """Download incremental files from the PMC FTP server."""
    base_dir = convert_to_cloudpath(base_dir)

    client = get_client("PMC")
    try:
        logger.info(f"Connecting to PMC FTP server...")
        client.connect()

        logger.info(f"Downloading incremental to {base_dir}")
        client.download_incremental(base_dir=base_dir, dry_run=dry_run)

    except Exception as e:
        logger.error(f"Error downloading incremental: {str(e)}")
        raise click.ClickException(str(e))
    finally:
        client.close()

if __name__ == "__main__":
    cli()
