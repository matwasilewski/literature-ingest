# Literature Ingest

Tool for ingesting and processing literature data.

## Setup

```bash
make install  # Creates venv and installs dependencies
```

## Usage

The primary way to use this project is:

```bash
uv run ingest  # Run the main ingestion pipeline
```

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
