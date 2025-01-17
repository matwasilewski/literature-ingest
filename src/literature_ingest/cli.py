from pathlib import Path
import click
from cloudpathlib import CloudPath
from literature_ingest.pmc import PMCFTPClient
from literature_ingest.utils.logging import get_logger

logger = get_logger(__name__, "info")

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
    default=Path("data/baselines"),
    help="Directory to store downloaded baseline files",
    type=Path,
)
def download_baselines(dry_run: bool, base_dir: Path):
    """Download baseline files from PMC FTP server."""
    # Convert to CloudPath if it's a cloud storage path
    if str(base_dir).startswith(("s3://", "gs://", "az://")):
        base_dir = CloudPath(str(base_dir))

    client = PMCFTPClient()
    try:
        logger.info("Connecting to PMC FTP server...")
        client.connect()

        logger.info(f"Downloading baselines to {base_dir}")
        client.download_baselines(base_dir=base_dir, dry_run=dry_run)

    except Exception as e:
        logger.error(f"Error downloading baselines: {str(e)}")
        raise click.ClickException(str(e))
    finally:
        client.close()

if __name__ == "__main__":
    cli()
