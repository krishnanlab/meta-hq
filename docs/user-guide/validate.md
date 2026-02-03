# Validate

This command validates the health and integrity of the MetaHQ data package. Warnings are raised
for every file in the data package that may be altered or corrupted. If warnings are raised, we
recommend running `metahq setup` to reset the configuration.

## Options
- `--log-dir`: Path to directory to send log files.
- `--log-level`: Logging level (`debug`, `info`, `warning`, `error`). Default: `info`.
- `--quiet`: Supress console output (flag)

## Usage
```bash
metahq validate
```
