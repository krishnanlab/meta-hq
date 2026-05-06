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
from metahq_setup.config.config import SERIES_COMBINED_BSON
from metahq_setup.config.schema import DataPackageConfig
from metahq_setup.processors import ProcessorRegistry
from metahq_setup.util.checkpointing import CheckpointManager


@click.group()
@click.version_option(version=__version__, prog_name="metahq-setup")
@click.pass_context
def main(ctx):
    """
    MetaHQ database harmonization and build tool.

    Processes biomedical annotations from multiple sources and builds
    the MetaHQ database for use with metahq-cli.
    """
    ctx.ensure_object(dict)


@main.command(name="package")
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
    "--verbose",
    "-v",
    is_flag=True,
    help="Enable verbose output",
)
def package(config, data_dir, output_dir, start_from, end_at, verbose):
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
        if config is None:
            click.secho(
                "Error: --config is required. Pass the path to metahq_setup.yaml.",
                fg="red",
            )
            sys.exit(1)

        pkg_config = DataPackageConfig.from_yaml(config)

        # Apply CLI overrides via model_copy so Pydantic re-validates
        updates = {}
        if data_dir:
            updates["data_dir"] = data_dir
        if output_dir:
            updates["output_dir"] = output_dir
        if verbose:
            updates["verbose"] = True
        if updates:
            pkg_config = pkg_config.model_copy(update=updates)

        pkg_config.create_directories()

        click.echo("Starting MetaHQ database build...")
        click.echo(f"Data directory: {pkg_config.data_dir}")
        click.echo(f"Output directory: {pkg_config.output_dir}")
        click.echo("")

        from metahq_setup.pipeline import PipelineOrchestrator

        orchestrator = PipelineOrchestrator(pkg_config)
        orchestrator.run(start_from=start_from, end_at=end_at)
        click.secho("✓ Pipeline completed successfully", fg="green")

    except Exception as e:
        click.secho(f"Error: {e}", fg="red", err=True)
        sys.exit(1)


@main.command(name="shields")
@click.option(
    "--sample-db",
    type=click.Path(path_type=Path, exists=True),
    help="Path to MetaHQ sample BSON database.",
)
@click.option(
    "--series-db",
    type=click.Path(path_type=Path, exists=True),
    help="Path to MetaHQ series BSON database.",
)
@click.option(
    "-o",
    "--outdir",
    type=click.Path(path_type=Path),
    help="Path to directory to save shields.",
)
def build_shields(sample_db, series_db, outdir):
    """Build and save shield.io JSON endpoints storing counts for each source in MetaHQ
    sample and series BSON databases.
    """
    from metahq_setup.builders import ShieldEndpointBuilder
    from metahq_setup.config import (
        SAMPLE_COMBINED_BSON,
        SERIES_COMBINED_BSON,
        SOURCE_COUNT_SHIELD_OUTDIR,
    )

    sample_db = sample_db if sample_db else SAMPLE_COMBINED_BSON
    series_db = series_db if series_db else SERIES_COMBINED_BSON
    outdir = outdir if outdir else SOURCE_COUNT_SHIELD_OUTDIR

    builder = ShieldEndpointBuilder(sample_db=sample_db, series_db=series_db)
    builder.build().save(outdir)


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
    from metahq_setup.combiners.geo import GeoCombiner
    from metahq_setup.config import GEO_COMBINED_BSON

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
    from metahq_setup.combiners.sra import SraCombiner
    from metahq_setup.config.config import OMICIDX_DB, SRA_COMBINED_BSON

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
    from metahq_setup.combiners.sample import SampleCombiner
    from metahq_setup.config import (
        GEO_COMBINED_BSON,
        OMICIDX_DB,
        SAMPLE_COMBINED_BSON,
        SRA_COMBINED_BSON,
    )

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
    from metahq_setup.combiners.study import StudyCombiner
    from metahq_setup.config import SAMPLE_COMBINED_BSON, SERIES_COMBINED_BSON

    output_path = Path(output) if output else SERIES_COMBINED_BSON
    sample_path = Path(sample) if sample else SAMPLE_COMBINED_BSON
    combiner = StudyCombiner()
    combiner.combine(sample_combined_bson=sample_path).save(output_path)


@main.group()
def metadata():
    """Command group for OmicIDX queries."""
    pass


@metadata.command(name="list-fields")
@click.option(
    "--level",
    "-l",
    type=click.Choice(["sample", "series"]),
    help="Which metadata level for which to show available fields.",
)
@click.option(
    "--metadata-db",
    type=click.Path(exists=True, path_type=Path),
    default=None,
    help="Path to OmicIDX DuckDB file (default: data/omicidx.duckdb)",
)
def list_fields(level, metadata_db):
    """Show queriable metadata fields from OmicIDX."""
    from metahq_setup.config import (
        OMICIDX_DB,
        OMICIDX_SAMPLE_TABLE,
        OMICIDX_SERIES_TABLE,
    )
    from metahq_setup.metadata.base import BaseMetadataRetriever

    try:
        if level == "sample":
            table = OMICIDX_SAMPLE_TABLE
        elif level == "series":
            table = OMICIDX_SERIES_TABLE
        else:
            click.secho(
                f"Error: Expected level in [sample, series]. Got {level}.",
                fg="red",
                err=True,
            )
            sys.exit(1)

        db_path = Path(metadata_db) if metadata_db else OMICIDX_DB

        retriever = BaseMetadataRetriever(db_path=db_path, table=table)
        retriever.show_available_fields()

    except Exception as e:
        click.secho(f"Error: {e}", fg="red", err=True)
        sys.exit(1)


@metadata.command(name="sample")
@click.option(
    "--fields",
    "-f",
    type=str,
    default=None,
    help="A comma-delimited string of fields to query",
)
@click.option(
    "--sample-bson",
    type=click.Path(exists=True, path_type=Path),
    default=None,
    help="Path to sample-level BSON database storing MetaHQ sample anntotations.",
)
@click.option(
    "--metadata-db",
    type=click.Path(exists=True, path_type=Path),
    default=None,
    help="Path to OmicIDX DuckDB file (default: data/omicidx.duckdb)",
)
def retrieve_sample_metadata(fields, sample_bson, metadata_db):
    """Retrieve sample-level metadata from OmicIDX for samples in a MetaHQ BSON database."""
    import bson

    from metahq_setup.config.config import OMICIDX_DB, SAMPLE_COMBINED_BSON
    from metahq_setup.metadata.sample import SampleMetadataRetriever

    sample_bson = Path(sample_bson) if sample_bson else SAMPLE_COMBINED_BSON
    db_path = Path(metadata_db) if metadata_db else OMICIDX_DB
    query_fields = fields.split(",")

    with open(sample_bson, "rb") as f:
        samples = list(bson.decode(f.read()).keys())

    retriever = SampleMetadataRetriever(db_path=db_path, table="src_geo_samples")
    retriever.retrieve(fields=query_fields, samples=samples)


@metadata.command(name="series")
@click.option(
    "--fields",
    "-f",
    type=str,
    default="accession,title,summary,overall_design",
    help="A comma-delimited string of fields to query",
)
@click.option(
    "--series-bson",
    type=click.Path(exists=True, path_type=Path),
    default=None,
    help="Path to series-level BSON database storing MetaHQ series anntotations.",
)
@click.option(
    "--metadata-db",
    type=click.Path(exists=True, path_type=Path),
    default=None,
    help="Path to OmicIDX DuckDB file (default: data/omicidx.duckdb)",
)
def retrieve_series_metadata(fields, series_bson, metadata_db):
    """Retrieve series-level metadata from OmicIDX for series in a MetaHQ BSON database."""
    import bson

    from metahq_setup.config import OMICIDX_DB, SERIES_COMBINED_BSON
    from metahq_setup.metadata.series import SeriesMetadataRetriever

    series_bson = Path(series_bson) if series_bson else SERIES_COMBINED_BSON
    db_path = Path(metadata_db) if metadata_db else OMICIDX_DB
    query_fields = fields.split(",")

    with open(series_bson, "rb") as f:
        series = list(bson.decode(f.read()).keys())

    retriever = SeriesMetadataRetriever(db_path=db_path, table="src_geo_series")
    retriever.retrieve(fields=query_fields, series=series)


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


@main.group()
def ontology():
    """Command group for OmicIDX queries."""
    pass


@ontology.command(name="relations")
@click.option(
    "--obo_file",
    "-i",
    type=click.Path(exists=True, path_type=Path),
    help="Path to ontology .obo or .obo.gz file.",
)
@click.option(
    "--outfile",
    "-o",
    type=click.Path(path_type=Path),
    help="Path to .parquet outfile storing ontology relations.",
)
def ontology_relations(obo_file, outfile):
    """Extract a terms x terms ontology relations matrix.

    You may interpret the output matrix as the following: For any row, column pair, if the
    value is 1, then the term representing that particular row is an ancestor of the term
    representing that particular column. If the value is 0, then there is no relationship
    between the terms.
    """
    from metahq_setup.ontology import Graph

    try:
        click.echo("Extracting ontology relations...")
        click.echo(f"OBO file: {obo_file}")
        click.echo(f"Out file: {outfile}")
        click.echo("")

        graph = Graph.from_obo(obo_file)
        graph.relations_matrix().save(outfile)

        click.secho(f"✓ Relations saved to {outfile}", fg="green")
        sys.exit(0)

    except Exception as e:
        click.secho(f"Error: {e}", fg="red", err=True)
        sys.exit(1)


@ontology.command(name="search-db")
@click.option(
    "--mondo",
    type=click.Path(exists=True, dir_okay=False, path_type=Path),
    required=False,
    help="Path to mondo's names_synonyms.json",
)
@click.option(
    "--uberon-cl",
    type=click.Path(exists=True, dir_okay=False, path_type=Path),
    required=False,
    help="Path to uberon/CL's names_synonyms.json",
)
@click.option(
    "--out-db",
    type=click.Path(dir_okay=False, path_type=Path),
    required=False,
    show_default=True,
    help="Output DuckDB database path",
)
def ontology_search_db(mondo, uberon_cl, out_db):
    """Build the ontology search DuckDB database."""
    from metahq_setup.builders import OntologySearchDbBuilder
    from metahq_setup.config import (
        MONDO_NAMES_SYNONYMS,
        ONTOLOGY_SEARCH_DB,
        UBERON_CL_NAMES_SYNONYMS,
    )

    mondo = mondo if mondo else MONDO_NAMES_SYNONYMS
    uberon_cl = uberon_cl if uberon_cl else UBERON_CL_NAMES_SYNONYMS
    out_db = out_db if out_db else ONTOLOGY_SEARCH_DB

    click.echo("Building ontology search database...")
    builder = OntologySearchDbBuilder(mondo=mondo, uberon_cl=uberon_cl, out_db=out_db)
    builder.build()
    click.secho(
        f"✓ Ontology search DuckDB database successfully built: {out_db}", fg="green"
    )


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
