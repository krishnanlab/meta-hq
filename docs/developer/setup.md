# Dev Setup

Clone the `meta-hq` repository:

Through `https`:
```bash
git clone https://github.com/krishnanlab/meta-hq.git
```

Through `ssh`:
```bash
git clone git@github.com:krishnanlab/meta-hq.git
```

## Install packages

We highly recommend using `uv` for install. The `meta-hq` repository is a structured as a
monorepo `uv workspace` with the `metahq-cli` and `metahq-core` packages in `packages/cli`
and `packages/core`, respectively.


**With Make:**

Through `uv`:
```bash
make uv_dev
```

Through `pip`:
```bash
make dev
```

**Manually:**

Through `pip`:
```bash
cd packages/core
pip install -e ".[dev]"

cd ../cli
pip install -e ".[dev]"
```

## Configure the package

Download the database and configure both packages.

```bash
metahq setup
```

## Local documentation build

To build and render the documentation page locally run:

```bash
mkdocs serve
```
