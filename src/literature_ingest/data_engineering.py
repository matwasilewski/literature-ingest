from pathlib import Path
import tarfile
import tempfile
from typing import List

from cloudpathlib import CloudPath

def resolve_file_or_dir(target: Path, source: Path) -> Path:
    if target.is_dir():
        return target / source.name
    else:
        return target

def unzip_and_filter(archive_file: Path, target_dir: Path, extension = ".xml") -> List[Path]:

    files = []

    if isinstance(target_dir, CloudPath):
        target_dir = Path(target_dir)
        # unzip locally and upload to target_dir
        with tempfile.TemporaryDirectory() as local_dir:
            with tarfile.open(archive_file, "r:gz") as tar:
                for member in tar.getmembers():
                    if member.isfile() and member.name.endswith(extension):
                        # Get just the filename without the path
                        filename = Path(member.name).name
                        # Create new destination path in target directory
                        local_file_path = local_dir / filename
                        target_file_path = target_dir / filename
                        # Extract member to the target directory, renaming it
                        member.name = filename
                        tar.extract(member, target_dir)
                        files.append(target_file_path)
                        local_file_path.copy(target_file_path)
    else:
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
