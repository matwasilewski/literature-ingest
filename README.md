# Literature Ingest

Tool for ingesting and processing literature data.

Contains utilities for downloading, parsing and uploading biomedical literature data.

## Setup

```bash
make install  # Creates venv and installs dependencies
```

## Usage

The primary way to use this project is:

```bash
uv run ingest  # Run the main ingestion pipeline
```

## Pipelines

Currently, all pipelines (predefined workflows) are codified in the `pipelines.py` file.
The project is in the alpha stage, and we expect to refactor it later and perhaps move the pipelines to a framework such as Kedro.


## Development

```bash
make format    # Format code
make lint      # Run linters
make unit      # Run unit tests
make integration  # Run integration tests
make full_test    # Run all tests
```

## Requirements

- Python 3.11+
- uv (Python package manager)
- Docker
- gcloud CLI
