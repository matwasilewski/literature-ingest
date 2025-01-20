from pathlib import Path
import tarfile
import tempfile
from typing import List

from cloudpathlib import CloudPath
from functools import wraps
import time
import subprocess
import gzip

from tenacity import retry, stop_after_attempt, wait_exponential


def resolve_file_or_dir(target: Path, source: Path) -> Path:
    if target.is_dir():
        return target / source.name
    else:
        return target

def unzip_and_filter(archive_file: Path, target_dir: Path, extension = ".xml", use_gsutil=False, overwrite=False) -> List[Path]:
    if isinstance(target_dir, CloudPath):
        # unzip locally and upload to target_dir
        with tempfile.TemporaryDirectory() as local_dir:
            local_files = unzip_to_local(archive_file, Path(local_dir), extension)
            if use_gsutil:
                files = upload_to_cloud_with_gsutil(target_dir, Path(local_dir), overwrite=overwrite)
            else:
                files = upload_to_cloud(target_dir, local_files, overwrite=overwrite)
    else:
        files = unzip_to_local(archive_file, target_dir, extension)
    return files

def upload_to_cloud(target_dir, local_files, overwrite=False):
    files = []
    for file in local_files:
        target_file_path = target_dir / file.name
        if target_file_path.exists() and not overwrite:
            continue
        target_file_path.upload_from(file, overwrite=overwrite)
        files.append(target_file_path)
    return files


def upload_to_cloud_with_gsutil(target_dir, local_dir, overwrite=True):
    if not overwrite:
        raise ValueError("overwrite must be True - this command will always overwrite files in the target directory")

    # Construct gsutil command for parallel upload of all files in local_dir
    cmd = ["gsutil", "-m", "cp", f"{local_dir}/*", str(target_dir)]
    print(f"Executing command: {cmd}")

    # Execute the gsutil command
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        raise RuntimeError(f"gsutil upload failed: {result.stderr}")

    # Return list of files in source directory
    return list(local_dir.glob("*"))

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=15))
def unzip_to_local(archive_file: Path, target_dir: Path, extension = ".xml") -> List[Path]:
    files = []

    # Handle .gz files (non-tar archives)
    if str(archive_file).endswith('.gz') and not str(archive_file).endswith('.tar.gz'):
        # Extract filename without .gz extension
        output_filename = archive_file.stem
        target_file_path = target_dir / output_filename

        with gzip.open(archive_file, 'rb') as f_in:
            with open(target_file_path, 'wb') as f_out:
                f_out.write(f_in.read())
        files.append(target_file_path)
        return files

    # Handle .tar.gz files
    if str(archive_file).endswith('.tar.gz'):
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
