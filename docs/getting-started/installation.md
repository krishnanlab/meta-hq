We recommend using the `uv` to install `metahq` packages. It is a modern, simple, and fast Rust-based package manager.
See the following link to install `uv`: [https://docs.astral.sh/uv/getting-started/installation](https://docs.astral.sh/uv/getting-started/installation)

## Prerequisites

- `python>=3.12`
- `pip` or `uv`

### CLI (Recommended)

Run the following to install the CLI. This will also install `metahq-core` by default.

=== "pip"

    ``` bash
    pip install metahq-cli
    ```

=== "uv"

    ``` bash
    uv add metahq-cli
    ```

### Core only

Run the following to install `metahq-core`.

=== "pip"

    ``` bash
    pip install metahq-core
    ```

=== "uv"

    ``` bash
    uv add metahq-core
    ```

## Install from Source

### 1) Clone the repository

```bash
git clone https://github.com/krishnanlab/meta-hq.git
cd meta-hq
```

### 2) Install packages

Using `make` requires `uv` and will automatically create a `uv venv`. run `source .venv/bin/activate` to activate it.

=== "Make (Recommended)"

    ```bash
    make install
    ```

=== "pip"

    ```bash
    pip install packages/core packages/cli
    ```

=== "pip (from tar)"

    ```bash
    pip install dist/metahq_core-0.1.2.tar.gz dist/metahq_cli-0.1.1.tar.gz
    ```

## Development Installation

For contributing or development:

=== "Make (Recommended)"

    ```bash
    make dev
    ```

=== "uv"

    ```bash
    uv pip install -e . ".[dev]"
    ```

=== "Manual"

    ```bash
    pip install -e packages/core[dev]
    pip install -e packages/cli[dev]
    ```
