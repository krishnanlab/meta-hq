<div align="left">
    <img src="https://raw.githubusercontent.com/krishnanlab/meta-hq/main/media/metahq_logo.png" alt="MetaHQ Logo" width="400" />
</div>

![Python](https://img.shields.io/badge/python-3.12+-blue.svg)
[![Documentation Status](https://app.readthedocs.org/projects/meta-hq/badge/?version=latest)](https://app.readthedocs.org/projects/meta-hq/badge/?version=latest)
![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)
[![Latest database: 17663086](https://zenodo.org/badge/DOI/10.5281/zenodo.17663086.svg)](https://doi.org/10.5281/zenodo.17663086)

A platform for harmonizing and distributing community-curated high-quality metadata of public omics samples and datasets.
See our preprint at [https://arxiv.org/abs/2602.07805](https://arxiv.org/abs/2602.07805).

## License and Attribution

- **Database:** [CC BY-NC 4.0](https://creativecommons.org/licenses/by-nc/4.0/) ![License: CC BY-NC 4.0](https://img.shields.io/badge/License-CC%20BY--NC%204.0-lightgrey.svg)
- **Software:** [BSD 3-Clause License](LICENSE-CODE) ![License: BSD 3-Clause](https://img.shields.io/badge/License-BSD%203--Clause-blue.svg)

The MetaHQ annotation database is licensed under CC BY-NC 4.0 because it integrates
community-curated annotations from multiple sources with NonCommercial restrictions.
The command-line tool is licensed under BSD 3-Clause.

**For commercial use:** Obtain permissions from NC-restricted sources OR use only commercial-compatible annotations.
See [Terms and Conditions](docs/about/terms_conditions.md).

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

## Documentation

Visit the MetaHQ documentation page at [meta-hq.readthedocs.io/en/latest](https://meta-hq.readthedocs.io/en/latest/).

## Demo

We provide a demo for the CLI [here](demo/README.md).

## Development

See dev install instructions [here](docs/developer/setup.md).
