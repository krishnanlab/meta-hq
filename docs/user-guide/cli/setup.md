# Setup

Every installation of `metahq-cli` requires an initial setup step. The purpose of this step is to
download the MetaHQ database from Zenodo and configure the package.

The MetaHQ configuration will always be stored in `/path/to/home/MetaHQ`.

## Options

- `--doi`: The Zenodo DOI for a particular MetaHQ database version. Default: `latest`.
- `--data_dir`: The Zenodo DOI for a particular MetaHQ database version. Default: `/path/to/home/.metahq_data`.

## Usage

```bash
metahq setup [OPTIONS]
```

## Examples

Download the latest MetaHQ database and save to a custom directory.

```bash
metahq setup --data_dir "/path/to/custom_dir"
```

Download MetaHQ database v1.0.0

```bash
metahq setup --doi 17666183
```
