import time
from functools import lru_cache
from importlib import metadata
from pathlib import Path
from typing import Dict, Optional, Union

import tomlkit
from pydantic_settings import BaseSettings


def _get_project_meta(name: str = "unknown") -> Dict:
    """
    Get name and version from pyproject metadata.
    """
    version = "unknown"
    description = ""
    try:
        with Path("./pyproject.toml").open() as pyproject:
            file_contents = pyproject.read()
        parsed = dict(tomlkit.parse(file_contents))['project']
        name = parsed["name"]
        version = parsed.get("version", "unknown")
        description = parsed.get("description", "")
    except FileNotFoundError:
        # If cannot read the contents of pyproject directly (i.e. in Docker),
        # check installed package using importlib.metadata:
        try:
            dist = metadata.distribution(name)
            name = dist.metadata["Name"]
            version = dist.version
            description = dist.metadata.get("Summary", "")
        except metadata.PackageNotFoundError:
            pass
    return {"name": name, "version": version, "description": description}


PKG_META = _get_project_meta()


class Settings(BaseSettings):
    """
    Settings. Environment variables always take priority over values loaded
    from the dotenv file.
    """

    current_timestamp: int = int(time.time())

    # Meta
    APP_NAME: str = str(PKG_META["name"])
    APP_VERSION: str = str(PKG_META["version"])
    PUBLIC_NAME: str = APP_NAME
    DESCRIPTION: str = str(PKG_META["description"])

    # Logger
    LOGGER_NAME: str = "literature_ingest"
    LOG_LEVEL: str = "info"
    VERBOSE_LOGS: Union[bool, int, str] = True
    JSON_LOGS: Union[bool, int, str] = False
    LOG_DIR: Path = (
        Path("logs") / f"{current_timestamp}-{LOGGER_NAME}-{LOG_LEVEL}.log"
    )

    TEST_BUCKET: Optional[str] = None
    PROD_BUCKET: Optional[str] = None
    SYSLOG_ADDR: Optional[Path] = None

    OPENAI_API_KEY: Optional[str] = None

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        case_sensitive = True
        secrets_dir = "secrets"

def get_project_root() -> Path:
    """Find the project root directory by looking for pyproject.toml"""
    current = Path(__file__).resolve().parent
    while current != current.parent:
        if (current / "pyproject.toml").exists():
            return current
        current = current.parent
    print(current)
    raise FileNotFoundError("Could not find project root (pyproject.toml)")

@lru_cache
def get_settings() -> Settings:
    project_root = get_project_root()
    return Settings(
        _env_file=str(project_root / ".env"),
        _secrets_dir=str(project_root / "secrets")
    )

settings = get_settings()
