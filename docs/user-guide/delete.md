# Delete

This command deletes the MetaHQ data package or removes all traces of MetaHQ entirely. Configuration files
and logs for MetaHQ are stored in `/path/to/home/MetaHQ`. The data package is stored in
`/path/to/home/.metahq_data` by default, or in the location specified in 
`metahq setup --data-dir </path/to/data>`.

## Options
- `--all`: Delete everything in the MetaHQ data package and the configuration file (flag).
- `--log-dir`: Path to directory to send log files.
- `--log-level`: Logging level (`debug`, `info`, `warning`, `error`). Default: `info`.


## Usage

To delete the MetaHQ data package and configuration files run:

```bash
metahq delete
```

To delete the data package and the MetaHQ home directory (storing the configuration and logs) run:

```bash
metahq delete --all
```

To use MetaHQ after running `metahq delete`, you will need to run `metahq setup` to redownload the data
package and configure the CLI.
