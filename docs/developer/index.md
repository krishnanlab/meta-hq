# MetaHQ Packages

MetaHQ is a monorepo that contains three main packages:

### metahq-core

The foundational library for the CLI providing data structures and utilities for annotation querying and manipulation.

### metahq-cli

A command-line interface built on top of `metahq-core`, providing easy-to-use commands for retrieving biomedical sample and study annotations.

### metahq-build

Data pipeline package for building the MetaHQ database from raw biomedical annotations. The
`metahq-build` package processes annotations from multiple biomedical data sources and assembles
them into the MetaHQ data package — a set of BSON annotation databases, ontology relation matrices,
and sample/series metadata Parquets consumed by `metahq-core` and `metahq-cli`.

**Note:** This package is designed to only be used by MetaHQ developers, and installed and run from
a cloned version of the repository. There is no PyPI release for `metahq-build`.


## Project Status

| Package | Version                                                            | Tests                                                                                  |
| ------- | ------------------------------------------------------------------ | -------------------------------------------------------------------------------------- |
| core    | ![metahq-core pypi](https://img.shields.io/pypi/v/metahq-core.svg) | ![Core Tests](https://github.com/krishnanlab/meta-hq/workflows/Core%20Tests/badge.svg) |
| cli     | ![metahq-cli pypi](https://img.shields.io/pypi/v/metahq-cli.svg)   | ![CLI Tests](https://github.com/krishnanlab/meta-hq/workflows/CLI%20Tests/badge.svg)   |
| build   | ![metahq-build local](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/krishnanlab/meta-hq/refs/heads/setup-package/data/shields/metahq_build__version.json)                                                              | ![DB-Build Tests](https://github.com/krishnanlab/meta-hq/workflows/DB%20Build%20Tests/badge.svg)  |


Open an issue on [GitHub](https://github.com/krishnan/meta-hq/issues) if you encounter any issues or have questions.
