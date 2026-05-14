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

## Install Git LFS

The data packages and some of the raw annotation sources are too large to be tracked through native git.
We use Git LFS to track these large files. You can install Git-LFS at [https://git-lfs.com](https://git-lfs.com).

To download lfs tracked files run:

```bash
git lfs install
git-lfs fetch --all
git lfs checkout
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
