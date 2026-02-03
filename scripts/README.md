# Meta-hq Scripts

This directory contains scripts for managing meta-hq's data dependencies and other tasks.

## Available Scripts

- `build_onto_duckdb.py`: This script builds the ontology DuckDB database from provided ontology name/synonym dictionaries. It creates the necessary tables and full-text search indices to support the `metahq search`
command.

## Usage

Ideally you should be in the metahq environment to run these scripts.
See the repo root README for instructions on setting up the environment.

To run the `build_onto_duckdb.py` script, from the repo root run the following command:

```bash
python scripts/build_onto_duckdb.py
```

This will result in a DuckDB database file named `ontology/ontology_search.duckdb` under the `data_dir` folder specified in your meta-hq `config.yaml`.