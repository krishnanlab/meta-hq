<div align="left">
    <img src="https://raw.githubusercontent.com/krishnanlab/meta-hq/media/metahq_logo.png" alt="MetaHQ Logo" width="400" height="200" />
</div>

![Python](https://img.shields.io/badge/python-3.12+-blue.svg)
![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)
[![License](https://img.shields.io/badge/License-BSD_3--Clause-blue.svg)](https://opensource.org/licenses/BSD-3-Clause)
[![Latest database: 17666183](https://zenodo.org/badge/DOI/10.5281/zenodo.17666183.svg)](https://doi.org/10.5281/zenodo.17666183)

A platform for harmonizing and distributing community-curated high-quality metadata of public omics samples and datasets.

## Status

| Package | Version                                                            | Tests                                                                                  |
| ------- | ------------------------------------------------------------------ | -------------------------------------------------------------------------------------- |
| core    | ![metahq-core pypi](https://img.shields.io/pypi/v/metahq-core.svg) | ![Core Tests](https://github.com/krishnanlab/meta-hq/workflows/Core%20Tests/badge.svg) |
| cli     | ![metahq-cli pypi](https://img.shields.io/pypi/v/metahq-cli.svg)   | ![Core Tests](https://github.com/krishnanlab/meta-hq/workflows/CLI%20Tests/badge.svg)  |

## Setup

### 1. Install metahq-cli

```bash
pip install metahq-cli
```

### 2. Download the MetaHQ database and configure the package

```bash
metahq setup
```

## Development

See dev install instructions [here](https://github.com/krishnanlab/meta-hq/blob/gh-pages/docs/getting-started/installation.md#development-installation).
