# Dev Setup

Clone the `meta-hq` repository:

=== "https"

    ```bash
    git clone https://github.com/krishnanlab/meta-hq.git
    ```

=== "ssh"

    ```bash
    git clone git@github.com:krishnanlab/meta-hq.git
    ```

## Install packages

We highly recommend using `uv` for install. The `meta-hq` repository is a structured as a
monorepo `uv workspace` with the `metahq-cli` and `metahq-core` packages in `packages/cli`
and `packages/core`, respectively.

Install with:

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

## Configure the package

Download the database and configure both packages.

```bash
metahq setup
```
