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
                file_path = target_dir / member.name
                tar.extract(member, file_path)
                files.append(file_path)
    return files
