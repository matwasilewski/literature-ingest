from pathlib import Path
import tarfile
from typing import List

from functools import wraps
import gzip

from tenacity import retry, stop_after_attempt, wait_exponential


def resolve_file_or_dir(target: Path, source: Path) -> Path:
    if target.is_dir():
        return target / source.name
    else:
        return target


def unzip_and_filter(
    archive_file: Path,
    target_dir: Path,
    extension=".xml",
    use_gsutil=False,
    overwrite=False,
) -> List[Path]:
    return unzip_to_local(archive_file, target_dir, extension)


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=15))
def unzip_to_local(
    archive_file: Path, target_dir: Path, extension=".xml"
) -> List[Path]:
    files = []

    # Handle .gz files (non-tar archives)
    if str(archive_file).endswith(".gz") and not str(archive_file).endswith(".tar.gz"):
        # Extract filename without .gz extension
        output_filename = archive_file.stem
        target_file_path = target_dir / output_filename

        with gzip.open(archive_file, "rb") as f_in:
            with open(target_file_path, "wb") as f_out:
                f_out.write(f_in.read())
        files.append(target_file_path)
        return files

    # Handle .tar.gz files
    if str(archive_file).endswith(".tar.gz"):
        with tarfile.open(archive_file, "r:gz") as tar:
            for member in tar.getmembers():
                if member.isfile() and member.name.endswith(extension):
                    # Get just the filename without the path
                    filename = Path(member.name).name
                    # Create new destination path in target directory
                    target_file_path = target_dir / filename
                    # Extract member to the target directory, renaming it
                    member.name = filename
                    tar.extract(member, target_dir)
                    files.append(target_file_path)
    return files
