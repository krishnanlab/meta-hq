## Prerequisites

- `python>=3.12`
- `pip` or `uv`

## 1) Install

### Install from PyPI (Recommended)

Run the following to install the CLI:

```bash
pip install metahq-cli
```

### Install from Source

**Clone the repository**

```bash
git clone https://github.com/krishnanlab/meta-hq.git
cd meta-hq
```

<br>

**Install packages**

Some `make` commands require `uv` (e.g., `make uv_install` or `make uv_dev`). These will automatically create a `uv venv`. Run `source .venv/bin/activate` to activate it.

We recommend using the `uv` package manager to install MetaHQ. It is a modern, simple, and fast Rust-based package manager.
See the following link to install `uv`: [https://docs.astral.sh/uv/getting-started/installation](https://docs.astral.sh/uv/getting-started/installation)

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
    pip install metahq_core-1.0.0.tar.gz metahq_cli-1.0.0.tar.gz
    ```

## 2) Download the Database

Download the MetaHQ database. See the the [setup guide](../user-guide/setup.md) for more details.

```bash
metahq setup
```
