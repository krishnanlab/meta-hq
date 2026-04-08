"""
Command-line interface for metahq-setup.

Provides commands for building the MetaHQ database, processing individual
sources, and managing the pipeline.
"""

import sys
from pathlib import Path

import click

from metahq_setup import __version__
from metahq_setup.config import PipelineConfig, load_config, save_config
from metahq_setup.processors import ProcessorRegistry
from metahq_setup.util.checkpointing import CheckpointManager


@click.group()
@click.version_option(version=__version__, prog_name="metahq-setup")
@click.pass_context
def main(ctx):
    """
    MetaHQ database setup and build tool.

    Processes biomedical annotations from multiple sources and builds
    the MetaHQ database for use with metahq-cli.
    """
    ctx.ensure_object(dict)


@main.command()
@click.option(
    "--config",
    "-c",
    type=click.Path(exists=True, path_type=Path),
    help="Path to configuration file",
)
@click.option(
    "--data-dir",
    type=click.Path(path_type=Path),
    help="Override data directory",
)
@click.option(
    "--output-dir",
    type=click.Path(path_type=Path),
    help="Override output directory",
)
@click.option(
    "--start-from",
    type=str,
    help="Resume from specific stage",
)
@click.option(
    "--end-at",
    type=str,
    help="Stop at specific stage",
)
@click.option(
    "--num-workers",
    "-j",
    type=int,
    help="Number of parallel workers",
)
@click.option(
    "--verbose",
    "-v",
    is_flag=True,
    help="Enable verbose output",
)
def build(config, data_dir, output_dir, start_from, end_at, num_workers, verbose):
    """
    Build the complete MetaHQ database.

    Runs the full pipeline from fetching metadata to building the final
    database files. Supports resuming from checkpoints.

    Examples:
        # Build with default configuration
        metahq-setup build

        # Build with custom config
        metahq-setup build --config my_config.yaml

        # Resume from a specific stage
        metahq-setup build --start-from propagate

        # Build with more workers
        metahq-setup build --num-workers 16
    """
    try:
        # Load configuration
        overrides = {}
        if data_dir:
            overrides["data_dir"] = data_dir
        if output_dir:
            overrides["output_dir"] = output_dir
        if num_workers:
            overrides["parallel"] = {"num_workers": num_workers}
        if verbose:
            overrides["verbose"] = verbose

        pipeline_config = load_config(config_file=config, overrides=overrides)

        # Create directories
        pipeline_config.create_directories()

        click.echo(f"Starting MetaHQ database build...")
        click.echo(f"Data directory: {pipeline_config.data_dir}")
        click.echo(f"Output directory: {pipeline_config.output_dir}")
        click.echo("")

        # Import and run pipeline (this will be implemented later)
        click.echo("Pipeline execution not yet implemented.")
        click.echo("This will be connected to the PipelineOrchestrator.")

    except Exception as e:
        click.secho(f"Error: {e}", fg="red", err=True)
        sys.exit(1)


@main.command()
@click.argument("source_name")
@click.option(
    "--output-dir",
    "-o",
    type=click.Path(path_type=Path),
    default=None,
    help="Output directory for processed data (default: data/processed/)",
)
@click.option(
    "--no-validate",
    is_flag=True,
    help="Skip validation of processed data",
)
def process(source_name, output_dir, no_validate):
    """
    Process a single data source.

    SOURCE_NAME is the name of the data source processor (e.g., gemma, ale, cello).

    Examples:
        # Process with default output directory (data/processed/)
        metahq-setup process disign_atlas

        # Override output directory
        metahq-setup process disign_atlas --output-dir /custom/output
    """
    try:
        # Get processor
        if not ProcessorRegistry.is_registered(source_name):
            click.secho(f"Error: Unknown processor '{source_name}'", fg="red")
            click.echo(
                f"Available processors: {', '.join(ProcessorRegistry.list_processors())}"
            )
            sys.exit(1)

        processor = ProcessorRegistry.get(source_name)

        run_kwargs = {"validate_output": not no_validate}
        if output_dir is not None:
            output_dir.mkdir(parents=True, exist_ok=True)
            run_kwargs["output_dir"] = output_dir

        click.echo(f"Processing {source_name} (version {processor.version})")
        click.echo(f"Output directory: {output_dir or 'data/processed/'}")
        click.echo("")

        data = processor.run(**run_kwargs)

        click.secho(f"✓ Successfully processed {len(data)} annotations", fg="green")

    except Exception as e:
        click.secho(f"Error: {e}", fg="red", err=True)
        sys.exit(1)


@main.command(name="list-sources")
def list_sources():
    """
    List all available data source processors.

    Shows the name, version, and description of each registered processor.
    """
    processors_info = ProcessorRegistry.get_all_info()

    if not processors_info:
        click.echo("No processors registered.")
        return

    click.echo("Available data source processors:\n")

    for source_name in sorted(processors_info.keys()):
        info = processors_info[source_name]
        click.echo(f"  {source_name} (v{info['version']})")
        if info["description"]:
            click.echo(f"    {info['description']}")


@main.group()
def download():
    """
    Download raw data from external sources.

    Downloads must be run before processing the corresponding source.
    """
    pass


@download.command(name="gemma")
@click.option(
    "--output",
    "-o",
    type=click.Path(path_type=Path),
    default=None,
    help="Override output file path (default: data/unprocessed/gemma.json)",
)
@click.option(
    "--query",
    "-q",
    default="sort=-id",
    show_default=True,
    help="Gemma API query string",
)
@click.option(
    "--max-studies",
    "-m",
    default=30_000,
    show_default=True,
    type=int,
    help="Maximum number of studies to download. Used to tell the GemmaFetcher when to stop fetching.",
)
def download_gemma(output, query, max_studies):
    """
    Download raw annotations from the Gemma database.

    Fetches study annotations from the Gemma REST API in batches and saves
    them to a single JSON file. This file is required before running
    'metahq-setup process gemma'.

    Examples:
        # Download with defaults
        metahq-setup download gemma

        # Download to a custom path
        metahq-setup download gemma --output /data/gemma.json

    """
    from metahq_setup.config.config import GEMMA_RAW
    from metahq_setup.fetchers.gemma import GemmaFetcher

    try:
        fetcher = GemmaFetcher()
        output_path = Path(output) if output else GEMMA_RAW
        click.echo(f"Downloading Gemma annotations (max {max_studies} studies)...")
        click.echo(f"Output: {output_path}")
        click.echo("")
        saved = fetcher.fetch(
            output_path=output_path, query=query, max_studies=max_studies
        )
        click.secho(f"✓ Saved to {saved}", fg="green")
    except Exception as e:
        click.secho(f"Error: {e}", fg="red", err=True)
        sys.exit(1)


@main.command()
@click.option(
    "--checkpoint-dir",
    "-c",
    type=click.Path(path_type=Path),
    default=Path(".checkpoints"),
    help="Checkpoint directory",
)
def status(checkpoint_dir):
    """
    Show pipeline execution status.

    Displays information about completed stages and checkpoints.
    """
    manager = CheckpointManager(checkpoint_dir)

    if not manager.checkpoints:
        click.echo("No pipeline checkpoints found.")
        click.echo(f"Checkpoint directory: {checkpoint_dir}")
        return

    click.echo(manager.get_summary())


@main.command(name="clear-checkpoints")
@click.option(
    "--checkpoint-dir",
    "-c",
    type=click.Path(path_type=Path),
    default=Path(".checkpoints"),
    help="Checkpoint directory",
)
@click.option(
    "--from-stage",
    type=str,
    help="Clear checkpoints from this stage onwards",
)
@click.confirmation_option(prompt="Are you sure you want to clear checkpoints?")
def clear_checkpoints(checkpoint_dir, from_stage):
    """
    Clear pipeline checkpoints.

    Use this to restart the pipeline from scratch or from a specific stage.
    """
    try:
        manager = CheckpointManager(checkpoint_dir)

        if from_stage:
            manager.clear_from_stage(from_stage)
            click.secho(f"✓ Cleared checkpoints from stage: {from_stage}", fg="green")
        else:
            manager.clear_checkpoints()
            click.secho("✓ Cleared all checkpoints", fg="green")

    except Exception as e:
        click.secho(f"Error: {e}", fg="red", err=True)
        sys.exit(1)


@main.command(name="init-config")
@click.argument("output_path", type=click.Path(path_type=Path))
@click.option(
    "--data-dir",
    type=click.Path(path_type=Path),
    default=Path("./data"),
    help="Data directory",
)
@click.option(
    "--output-dir",
    type=click.Path(path_type=Path),
    default=Path("./output"),
    help="Output directory",
)
def init_config(output_path, data_dir, output_dir):
    """
    Initialize a new configuration file.

    OUTPUT_PATH is where to save the configuration file.

    Creates a configuration file with default settings that can be
    customized for your pipeline.

    Examples:
        # Create default config
        metahq-setup init-config my_pipeline.yaml

        # Create config with custom directories
        metahq-setup init-config config.yaml --data-dir /data/metahq
    """
    try:
        # Create config with specified directories
        config = PipelineConfig(
            data_dir=data_dir,
            output_dir=output_dir,
        )

        # Save to file
        save_config(config, output_path)

        click.secho(f"✓ Created configuration file: {output_path}", fg="green")
        click.echo("Edit this file to customize your pipeline settings.")

    except Exception as e:
        click.secho(f"Error: {e}", fg="red", err=True)
        sys.exit(1)


@main.command()
@click.option(
    "--config",
    "-c",
    type=click.Path(exists=True, path_type=Path),
    help="Path to configuration file to validate",
)
def validate_config(config):
    """
    Validate a configuration file.

    Checks that the configuration file is valid and can be loaded.
    """
    try:
        if config is None:
            click.secho("Error: --config is required", fg="red")
            sys.exit(1)

        pipeline_config = load_config(config_file=config)
        click.secho("✓ Configuration is valid", fg="green")

        # Show summary
        click.echo("\nConfiguration summary:")
        click.echo(f"  Data directory: {pipeline_config.data_dir}")
        click.echo(f"  Output directory: {pipeline_config.output_dir}")
        click.echo(f"  Workers: {pipeline_config.parallel.num_workers}")
        click.echo(
            f"  Enabled processors: {sum(1 for p in pipeline_config.processors.values() if p.enabled)}"
        )

    except Exception as e:
        click.secho("Error: Invalid configuration", fg="red", err=True)
        click.echo(f"{e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
