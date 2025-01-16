# literature-ingest

# Dev
## Add Dependencies
uv add pytest 

New dependency will now be visible in pyproject.toml. 

To install the dependency, run `make install`.

This will generate a locked requirements.txt file in the root of the project, and install it.

## Run pre-commit hooks

To run the pre-commit hooks, run `make precommit`.

This will run the pre-commit hooks on the current branch and the main branch.

