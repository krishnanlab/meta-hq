"""
Command-line interface for metahq-setup.

Provides commands for building the MetaHQ database, processing individual
sources, and managing the pipeline.
"""

import sys
from pathlib import Path

import click

from metahq_setup import __version__
from metahq_setup.combiners.sample import SAMPLE_COMBINED_BSON
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
    default=60_000,
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


@main.group()
def combine():
    """
    Combine processed annotations from multiple sources into a BSON file.

    Run 'metahq-setup process' for each source before combining.
    """
    pass


@combine.command(name="geo")
@click.option(
    "--output",
    "-o",
    type=click.Path(path_type=Path),
    default=None,
    help="Output BSON file path (default: data/processed/geo_combined.bson)",
)
def combine_geo(output):
    """
    Combine all GEO-based source annotations into a single BSON file.

    Reads processed parquets from data/processed/ for each GEO source
    (ale, cello, creeds, disign_atlas, gemma, golightly, gu, johnson_2023,
    krishnanlab, sirota_2011, ursa, ursahd). Missing sources are skipped.

    Examples:
        metahq-setup combine geo
        metahq-setup combine geo --output /data/geo_combined.bson
    """
    from metahq_setup.combiners.geo import GEO_COMBINED_BSON, GeoCombiner

    try:
        output_path = Path(output) if output else GEO_COMBINED_BSON
        click.echo("Combining GEO annotations...")
        click.echo(f"Output: {output_path}")
        click.echo("")

        combiner = GeoCombiner()
        combiner.combine().clean().save(output_path)

        click.secho(f"✓ Saved to {output_path}", fg="green")

    except Exception as e:
        click.secho(f"Error: {e}", fg="red", err=True)
        sys.exit(1)


@combine.command(name="sra")
@click.option(
    "--output",
    "-o",
    type=click.Path(path_type=Path),
    default=None,
    help="Output BSON file path (default: data/processed/sra_combined.bson)",
)
@click.option(
    "--metadata-db",
    type=click.Path(exists=True, path_type=Path),
    default=None,
    help="Path to OmicIDX DuckDB file (default: data/omicidx.duckdb)",
)
def combine_sra(output, metadata_db):
    """
    Combine all SRA-based source annotations into a single BSON file.

    Reads processed parquets from data/processed/ for each SRA source
    (bgee, johnson_2023_rnaseq). SRR/SRX accession IDs are mapped to GSM
    IDs via the OmicIDX DuckDB database. Missing sources are skipped.

    Examples:
        metahq-setup combine sra
        metahq-setup combine sra --output /data/sra_combined.bson
        metahq-setup combine sra --metadata-db /data/omicidx.duckdb
    """
    from metahq_setup.combiners.sra import SRA_COMBINED_BSON, SraCombiner
    from metahq_setup.config.config import OMICIDX_DB

    try:
        output_path = Path(output) if output else SRA_COMBINED_BSON
        db_path = Path(metadata_db) if metadata_db else OMICIDX_DB
        click.echo("Combining SRA annotations...")
        click.echo(f"OmicIDX DB: {db_path}")
        click.echo(f"Output: {output_path}")
        click.echo("")

        combiner = SraCombiner()
        combiner.combine(db_path=db_path).clean().save(output_path)

        click.secho(f"✓ Saved to {output_path}", fg="green")

    except Exception as e:
        click.secho(f"Error: {e}", fg="red", err=True)
        sys.exit(1)


@combine.command(name="sample")
@click.option(
    "--output",
    "-o",
    type=click.Path(path_type=Path),
    default=None,
    help="Output BSON file path (default: data/processed/combined__level-sample.bson)",
)
@click.option(
    "--geo",
    type=click.Path(exists=True, path_type=Path),
    default=None,
    help="Path to GEO combined BSON (default: data/processed/geo_combined.bson)",
)
@click.option(
    "--sra",
    type=click.Path(exists=True, path_type=Path),
    default=None,
    help="Path to SRA combined BSON (default: data/processed/sra_combined.bson)",
)
@click.option(
    "--metadata-db",
    type=click.Path(exists=True, path_type=Path),
    default=None,
    help="Path to OmicIDX DuckDB file (default: data/omicidx.duckdb)",
)
def combine_sample(output, geo, sra, metadata_db):
    """
    Merge GEO and SRA combined annotations into a single sample-level BSON.

    Both GEO and SRA BSONs must already be keyed by GSM. Run
    'metahq-setup combine geo' and 'metahq-setup combine sra' first.

    Accession IDs (series, platform, srx, srp) are enriched from OmicIDX
    for every sample in the combined database.

    Examples:
        metahq-setup combine sample
        metahq-setup combine sample --output /data/combined__level-sample.bson
        metahq-setup combine sample --geo /data/geo.bson --sra /data/sra.bson
        metahq-setup combine sample --metadata-db /data/omicidx.duckdb
    """
    from metahq_setup.combiners.geo import GEO_COMBINED_BSON
    from metahq_setup.combiners.sample import SAMPLE_COMBINED_BSON, SampleCombiner
    from metahq_setup.combiners.sra import SRA_COMBINED_BSON
    from metahq_setup.config.config import OMICIDX_DB

    try:
        output_path = Path(output) if output else SAMPLE_COMBINED_BSON
        geo_path = Path(geo) if geo else GEO_COMBINED_BSON
        sra_path = Path(sra) if sra else SRA_COMBINED_BSON
        db_path = Path(metadata_db) if metadata_db else OMICIDX_DB
        click.echo("Merging GEO and SRA annotations into sample-level database...")
        click.echo(f"GEO: {geo_path}")
        click.echo(f"SRA: {sra_path}")
        click.echo(f"OmicIDX DB: {db_path}")
        click.echo(f"Output: {output_path}")
        click.echo("")

        combiner = SampleCombiner()
        combiner.combine(
            geo_bson=geo_path, sra_bson=sra_path, db_path=db_path
        ).clean().save(output_path)

        click.secho(f"✓ Saved to {output_path}", fg="green")

    except Exception as e:
        click.secho(f"Error: {e}", fg="red", err=True)
        sys.exit(1)


@combine.command(name="study")
@click.option(
    "--output",
    "-o",
    type=click.Path(path_type=Path),
    default=None,
    help="Output BSON file path (default: data/processed/combined__level-sample.bson)",
)
@click.option(
    "--sample",
    type=click.Path(exists=True, path_type=Path),
    default=None,
    help="Path to sample combined BSON (default: data/processed/combined__level-sample.bson)",
)
def combine_study(sample, output):
    from metahq_setup.combiners.sample import SAMPLE_COMBINED_BSON
    from metahq_setup.combiners.study import STUDY_COMBINED_BSON, StudyCombiner

    output_path = Path(output) if output else STUDY_COMBINED_BSON
    sample_path = Path(sample) if sample else SAMPLE_COMBINED_BSON
    combiner = StudyCombiner()
    combiner.combine(sample_combined_bson=sample_path).save(output_path)


@main.group()
def metadata():
    """Command group for OmicIDX queries."""
    pass


@metadata.command(name="show-fields")
@click.option(
    "--level",
    "-l",
    type=click.Choice(["sample", "study"]),
    help="Which metadata level for which to show available fields.",
)
@click.option(
    "--metadata-db",
    type=click.Path(path_type=Path),
    default=None,
    help="Path to OmicIDX DuckDB file (default: data/omicidx.duckdb)",
)
def show_fields(level, metadata_db):
    if level == "sample":
        from metahq_setup.config.config import OMICIDX_DB
        from metahq_setup.metadata.sample import SampleMetadataRetriever

        db_path = Path(metadata_db) if metadata_db else OMICIDX_DB

        retriever = SampleMetadataRetriever(db_path=db_path)
        retriever.show_available_fields()


@metadata.command(name="sample")
@click.option(
    "--fields",
    "-f",
    type=str,
    default=None,
    help="A comma-delimited string of fields to query",
)
@click.option(
    "--metadata-db",
    type=click.Path(path_type=Path),
    default=None,
    help="Path to OmicIDX DuckDB file (default: data/omicidx.duckdb)",
)
def retrieve_sample_metadata(fields, metadata_db):
    import bson

    from metahq_setup.combiners.sample import SAMPLE_COMBINED_BSON
    from metahq_setup.config.config import OMICIDX_DB
    from metahq_setup.metadata.sample import SampleMetadataRetriever

    db_path = Path(metadata_db) if metadata_db else OMICIDX_DB
    query_fields = fields.split(",")

    with open(SAMPLE_COMBINED_BSON, "rb") as f:
        samples = list(bson.decode(f.read()).keys())

    retriever = SampleMetadataRetriever(db_path=db_path, table="src_geo_samples")
    retriever.retrieve(fields=query_fields, samples=samples)


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
