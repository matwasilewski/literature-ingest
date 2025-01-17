from pathlib import Path


def resolve_file_or_dir(target: Path, source: Path) -> Path:
    if target.is_dir():
        return target / source.name
    else:
        return target

def unzip(file: Path, target: Path) -> None:
    pass
