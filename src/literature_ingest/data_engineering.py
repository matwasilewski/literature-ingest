from pathlib import Path
import tarfile
from typing import List

def resolve_file_or_dir(target: Path, source: Path) -> Path:
    if target.is_dir():
        return target / source.name
    else:
        return target

def unzip_and_filter(archive_file: Path, target_dir: Path, extension = ".xml") -> List[Path]:
    files = []

    with tarfile.open(archive_file, "r:gz") as tar:
        for member in tar.getmembers():
            if member.isfile() and member.name.endswith(extension):
                # Get just the filename without the path
                filename = Path(member.name).name
                # Create new destination path in target directory
                file_path = target_dir / filename
                # Extract member to the target directory, renaming it
                member.name = filename
                tar.extract(member, target_dir)
                files.append(file_path)
    return files
