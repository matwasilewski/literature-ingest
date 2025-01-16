
.PHONY: help check-bootstrap-dependencies check-tools check-docker bootstrap precommit install format type-check unit gcp-authenticate init-db install lint test load-data

GIT_SHA ?= $(shell git rev-parse --short HEAD)
export UV_PYTHON = .venv/bin/python
export TQDM_DISABLE ?= 1
TARGET_PLATFORM ?= linux/amd64
PREREQUISITES := docker gcloud python3 uv

# ensures all make targets run in one shell (rather than line by line in new shell)
.ONESHELL:

default: install full_test compose_down integration_test
	echo "done!"

prerequisites:
	$(info Checking if prerequisites are installed...)
	$(foreach exec,$(PREREQUISITES),\
        $(if $(shell command -v $(exec) 2>/dev/null),,$(error "$(exec) is not installed.")))
	$(info All prerequisites are installed.)

venv: prerequisites
	if [ ! -d .venv ]; then uv venv -p 3.11; fi
	uv run python -m ensurepip --upgrade || true

install: venv precommit-hooks
	uv pip install -r requirements.txt #force it to install in our venv
	uv pip install -e .  # Install the package in development mode

precommit: precommit-hooks
	git fetch origin
	uv run pre-commit run --from-ref origin/main --to-ref HEAD

precommit-hooks:
	uv pip install pre-commit
	uv run pre-commit install --install-hooks

full_test:
	# activate venv to ensure spark doesn't have python driver mismatches
	uv run pytest -v tests/

format:
	uv run ruff check . --fix


lock:
	uv pip compile requirements.in > requirements.txt

clean:
	@echo "cleaning various cache locations to ensure clean installation is possible"
	@echo "this may be necessary e.g. when updating one of our local packages"
	uv run pre-commit clean
	rm -rf .pytest_cache .venv
	uv cache clean
	docker volume prune -f

## gcp-authenticate: Authenticate with GCP, use this before running composer commands
gcp-authenticate:
	gcloud auth application-default login
	gcloud auth login

lint:
	ruff check .
	mypy .

unit: $(VENV_ACTIVATE)
	uv run pytest tests/unit/

integration: $(VENV_ACTIVATE)
	uv run pytest tests/integration/
