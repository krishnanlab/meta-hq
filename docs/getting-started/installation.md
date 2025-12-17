We recommend using the `uv` package manager to install `metahq` packages. It is a modern, simple, and fast Rust-based package manager.
See the following link to install `uv`: [https://docs.astral.sh/uv/getting-started/installation](https://docs.astral.sh/uv/getting-started/installation)

## Prerequisites

- `python>=3.12`
- `pip` or `uv`

## Install from PyPI (Recommended)

Run the following to install the CLI. This will also install `metahq-core` by default.

```bash
pip install metahq-cli
```

## Install from Source

### 1) Clone the repository

```bash
git clone https://github.com/krishnanlab/meta-hq.git
cd meta-hq
```

### 2) Install packages

Some `make` commands require `uv` (e.g., `make uv_install` or `make uv_dev`). These will automatically create a `uv venv`. Run `source .venv/bin/activate` to activate it.

Using `make`:

=== "make (uv)"

    ```bash
    make uv_install
    ```

=== "make"

    ```bash
    make install
    ```

Or with `pip`:

=== "pip"

    ```bash
    pip install packages/core packages/cli
    ```

=== "pip (from tar)"

    ```bash
    pip install dist/metahq_core-0.1.2.tar.gz dist/metahq_cli-0.1.1.tar.gz
    ```

## Development Installation

For contributing or development with `make`:

=== "make (uv)"

    ```bash
    make uv_dev
    ```

=== "make"

    ```bash
    make dev
    ```

Or manually:

```bash
cd packages/core
pip install -e ".[dev]"

cd ../cli
pip install -e ".[dev]"
```
