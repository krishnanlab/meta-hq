# Delete

This command deletes the MetaHQ data package.

## Options
- `--all`: Delete everything in the MetaHQ data package and the configuration file (flag)
- `--log-dir`: Path to directory to send log files.
- `--log-level`: Logging level (`debug`, `info`, `warning`, `error`). Default: `info`.


# Usage

To only delete the MetaHQ data package:

```bash
metahq delete
```

To delete the data package and any MetaHQ configuration files and logs in `/path/to/home/MetaHQ`:

```bash
metahq delete --all
```

To use MetaHQ after running this command, you will need to run `metahq setup` to redownload the data
package and configure the CLI.
