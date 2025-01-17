import click
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
    default="data/baselines",
    help="Directory to store downloaded baseline files",
    type=click.Path(),
)
def download_baselines(dry_run: bool, base_dir: click.Path):
    """Download baseline files from PMC FTP server."""
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
