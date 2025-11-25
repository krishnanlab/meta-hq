## Installation

## Prerequisites

- Python 3.12 or higher.
- pip, uv, or conda

### CLI

Run the following to install the CLI. This will also install `metahq-core` by default.

=== "pip"

```bash
    pip install metahq-cli
```

=== "uv"

```bash
    uv add metahq-cli
```

=== "conda"

```bash
    conda env create -n metahq "python>=3.12"
    conda activate metahq

    pip install metahq-cli
```

### Core only

=== "pip"

```bash
    pip install metahq-core
```

=== "uv"

```bash
    uv add metahq-core
```

=== "conda"

```bash
    conda env create -n metahq "python>=3.12"
    conda activate metahq

    pip install metahq-core
```

## Install from Source

### Clone the repository

```bash
    git clone https://github.com/krishnanlab/meta-hq.git
    cd meta-hq
```

### Install packages

=== "Make (Recommended)"

```bash
    make install
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
