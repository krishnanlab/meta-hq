import marimo

__generated_with = "0.24.0"
app = marimo.App(width="medium", auto_download=["ipynb"])


@app.cell
def _(mo):
    mo.md(r"""
    # Manuscript figures
    This notebook contains code to plot all figures in the MetaHQ manuscript.

    Author: Parker Hicks <br>
    Date: 2026-01-17 <br>
    Last updated: 2026-09-09 by Parker Hicks
    """)
    return


@app.cell
def _():
    import re
    import warnings
    from collections import defaultdict
    from math import ceil
    from pathlib import Path
    from typing import Any, Literal

    import marimo as mo
    import matplotlib.pyplot as plt
    import matplotlib.patches as mpatches
    import matplotlib.colors as mcolors
    from matplotlib.patches import Patch
    import numpy as np
    import pandas as pd
    import polars as pl
    import seaborn as sns
    from bson import BSON
    from matplotlib import ticker
    from upsetplot import UpSet, from_contents

    return (
        Any,
        BSON,
        Literal,
        Patch,
        Path,
        UpSet,
        ceil,
        defaultdict,
        from_contents,
        mcolors,
        mo,
        mpatches,
        np,
        pl,
        plt,
        re,
        sns,
        ticker,
        warnings,
    )


@app.cell
def _(Any, BSON, Path):
    def load_bson(file: Path | str, **kwargs) -> dict[str, Any]:
        """Load dictionary from compressed bson."""
        with open(file, "rb") as bf:
            return BSON(bf.read()).decode(**kwargs)

    def load_txt(file: Path | str, **kwargs) -> list[str]:
        """Load txt file."""
        with open(file, "r", **kwargs) as f:
            return [line.strip() for line in f.readlines()]

    return load_bson, load_txt


@app.cell
def _(Path):
    # constants
    ANNOTATIONS_DIR = Path("data/processed")
    ATTRIBUTES = ["tissue", "disease", "sex", "age"]

    METADATA_DIR = Path("data/metadata")
    PLATFORMS_FILE = METADATA_DIR / "technologies.parquet"

    RESULTS_DIR: Path = Path("results")
    UNIQUE_PROPAGATED_TERMS: Path = RESULTS_DIR / "unique_propagated_tissue_disease_terms.txt"
    POST_HARMONIZATION_RESULTS = RESULTS_DIR / "post_harmonization"
    POST_OVERLAP_RESULTS = list(POST_HARMONIZATION_RESULTS.glob("overlap*"))
    INFORMATION_CONTENT_SAMPLE_RESULTS = RESULTS_DIR / "ic_original_sources_vs_metahq__level-sample.parquet"
    INFORMATION_CONTENT_SERIES_RESULTS = RESULTS_DIR / "ic_original_sources_vs_metahq__level-series.parquet"

    FIGURES_DIR: Path = Path("figures")

    ## source annotation files
    PROCESSED_DIR = Path("data/processed")
    SRA_PROCESSED = PROCESSED_DIR / "sra_combined.bson"
    GEO_PROCESSED = PROCESSED_DIR / "geo_combined.bson"
    SEMI_PROCESSED_SERIES = Path("data/analysis/semi_processed__combined__level-series.bson")

    ## helpers
    SRA2GEO = Path("data/metadata/sra2geo.parquet")

    ATTRIBUTES = ["tissue", "disease", "sex", "age"]

    ## plotting
    COLORS = {'tissue': 'steelblue', 'disease': 'coral', 'sex': 'mediumseagreen', 'age': 'mediumpurple'}
    FMT = "png"
    OVERLAP_ORDER = ["tissue", "disease", "sex", "age"]
    OVERLAP_CMAP = "Blues"
    PMI_CMAP = "vlag"
    return (
        ANNOTATIONS_DIR,
        ATTRIBUTES,
        COLORS,
        FIGURES_DIR,
        FMT,
        GEO_PROCESSED,
        INFORMATION_CONTENT_SAMPLE_RESULTS,
        OVERLAP_CMAP,
        OVERLAP_ORDER,
        PLATFORMS_FILE,
        PMI_CMAP,
        POST_HARMONIZATION_RESULTS,
        SEMI_PROCESSED_SERIES,
        SRA_PROCESSED,
        UNIQUE_PROPAGATED_TERMS,
    )


@app.cell
def _(ANNOTATIONS_DIR, load_bson):
    # load the databases
    sample_db = load_bson(ANNOTATIONS_DIR / "combined__level-sample.bson")
    series_db = load_bson(ANNOTATIONS_DIR / "combined__level-series.bson")

    print("Number of entries in MetaHQ:")
    print(f"Samples: {len(sample_db)}")
    print(f"Studies: {len(series_db)}")
    return sample_db, series_db


@app.cell
def _(mo):
    mo.md(r"""
    # Plot total annotations for each attribute
    """)
    return


@app.cell
def _(COLORS, Path, pl, plt, sns, ticker):
    def plot_total_anno_sample_and_study(
        sample_data: dict,
        study_data: dict,
        attributes: list[str],
        ylabel: str,
        figsize: tuple[int, int]=(5,5),
        save: bool=False,
        outfile: str | Path | None = None,
        dpi: int = 600,
        verbose: bool = False,
        titles: list[str] | None = None,
        order: list[str] | None = None,
        ylim_scale: int = 1,
    ):
        """Plot the total number of entries with each attribute annotation."""
        colors = {k.capitalize(): v for k,v in COLORS.items()}

        dfs = []
        # count attribute anntotations
        for data in [sample_data, study_data]:
            total = {attribute: 0 for attribute in attributes}
            for anno in data.values():
                for attribute in attributes:
                    if attribute in anno:
                        total[attribute] += 1

            df = pl.DataFrame(
                    {"attribute": list(total.keys()), "count": list(total.values())}
            )

            df = (
                df
                    .with_columns(
                        pl.col("attribute").str.to_titlecase().alias("attribute")
                    )
            )
            dfs.append(df)

        fig, axes = plt.subplots(1, 2, figsize=figsize, sharey=True)
        for idx, (df, ax) in enumerate(zip(dfs, axes)):
            sns.barplot(
                df, 
                y="attribute",
                x="count",
                hue="attribute", 
                palette=colors, 
                ax=ax,
                legend=False,
                order=order,
            )

            ax.set_xlabel(ylabel)
            ax.get_xaxis().set_major_formatter(ticker.StrMethodFormatter('{x:,.0f}'))
            ax.tick_params("x", rotation=40)

            # sample
            if idx == 0:
                ax.set_xticks([100_000, 250_000, 500_000])

            # study
            if idx == 1:
                ax.set_xticks([5_000, 10_000, 20_000])

            ax.set_ylabel("" if idx > 0 else "") 
            if titles and idx < len(titles):
                ax.set_title(titles[idx])

            sns.despine(right=True, top=True, ax=ax)

        plt.tight_layout()

        if save and isinstance(outfile, (str, Path)):
            plt.savefig(outfile, dpi=dpi)

        plt.show()

    return (plot_total_anno_sample_and_study,)


@app.cell
def _(mo):
    mo.md(r"""
    ## Samples and studies in MetaHQ with attribute annotations
    """)
    return


@app.cell
def _(plot_total_anno_sample_and_study, sample_db, series_db):
    plot_total_anno_sample_and_study(
        sample_db,
        series_db,
        attributes=["disease", "tissue", "sex", "age"],
        ylabel="",
        figsize=(4, 2),
        titles=["Samples", "Studies"],
        save=True,
        outfile="figures/attribute_sample_and_study_count.png",
        dpi=1000,
        order=["Tissue", "Disease", "Sex", "Age"],
        ylim_scale=1,
    )
    return


@app.cell
def _(mo):
    mo.md(r"""
    # Upset plot
    """)
    return


@app.cell
def _(mo):
    mo.md(r"""
    ### Format data for upset plot
    """)
    return


@app.cell
def _(Literal, PLATFORMS_FILE, pl):
    def count_entries_per_attribute(records: dict[str, list[str]], title: str):
        """For each attribute, count the number of
        entries (samples or studies) that have at least 
        one annotation to that attribute.
        """
        # show number of samples annotated to any attribute
        print(title)
        print("================")
        for attribute, entries in records.items():
            print(f"{attribute.capitalize()}: {len(entries)}")
        print("================\n")


    def record_entries_per_attribute(
        database: dict,
        attributes: list[str],
        tech: Literal["rnaseq", "microarray"],
        verbose: bool = False,
        title: str = "Records",
    ) -> dict[str, list[str]]:
        """Record the samples that 

        Used as input to `upset_plot()`

        """
        platforms = (
            pl.scan_parquet(PLATFORMS_FILE)
                .filter(pl.col("technology") == tech)
                .select("id")
                .collect()
                .to_series()
        ) 
        records = {attribute: [] for attribute in attributes}
        for entry, anno in database.items():

            platform_ok = False
            if "platform" not in anno["accession_ids"]:
                print(anno)

            for platform in anno["accession_ids"]["platform"].split("||"):
                if platform in platforms:
                    platform_ok = True
                    break

            if not platform_ok:
                continue

            for attribute in records:
                if attribute in anno:
                    records[attribute].append(entry)

        if verbose:
            count_entries_per_attribute(records, title)

        return records

    return (record_entries_per_attribute,)


@app.cell
def _(UpSet, from_contents, plt):
    def upset_plot(
        records: dict[str, list[str]],
        title: str | None = None,
        save: bool = False,
        outfile: str | None = None,
        dpi: int=500,
        ylim: int | None = None
    ):
        df = from_contents(
            {attribute.capitalize(): records[attribute] for attribute in records}
        )
        ax_dict = UpSet(df).plot()

        if isinstance(title, str):
            plt.title(title, fontsize=12, fontweight="bold")

        if isinstance(ylim, int):
            plt.ylim(0, ylim)

        if save and isinstance(outfile, str):
            plt.savefig(outfile, dpi=dpi)

        plt.show()

    return (upset_plot,)


@app.cell
def _(ATTRIBUTES, record_entries_per_attribute, sample_db, series_db):
    # get attribute sample/study counts

    # ========== Sample ============
    sample_records_microarray: dict[str, list[str]] = record_entries_per_attribute(
        sample_db, ATTRIBUTES, verbose=True, title="Sample Records (microarray):", tech="microarray"
    )
    sample_records_rnaseq = record_entries_per_attribute(
        sample_db, ATTRIBUTES, verbose=True, title="Sample Records (rnaseq):", tech="rnaseq"
    )

    # ========== Study ============
    study_records_microarray = record_entries_per_attribute(
        series_db, ATTRIBUTES, verbose=True, title="Study Records (microarray):", tech="microarray"
    )
    study_records_rnaseq = record_entries_per_attribute(
        series_db, ATTRIBUTES, verbose=True, title="Study Records (rnaseq):", tech="rnaseq"
    )
    return (
        sample_records_microarray,
        sample_records_rnaseq,
        study_records_microarray,
        study_records_rnaseq,
    )


@app.cell
def _(
    FMT,
    sample_records_microarray: dict[str, list[str]],
    sample_records_rnaseq,
    study_records_microarray,
    study_records_rnaseq,
    upset_plot,
    warnings,
):
    # upset plots
    # Note: there is a bug in the Upsetplot package where pandas v3 raises errors. They're working on a fix: https://github.com/jnothman/UpSetPlot/issues/303, but it is not yet resolved. Use pandas <3.0.0.

    with warnings.catch_warnings():
        warnings.simplefilter(action='ignore', category=FutureWarning)
        # ========== Sample ============
        upset_plot(
            sample_records_microarray,
            title="Sample annotation coverage (microarray)",
            save=True,
            outfile=f"figures/attribute_upset_plot__level-sample__tech-microarray.{FMT}",
            dpi=500,
            ylim=100_000,
        )
        upset_plot(
            sample_records_rnaseq,
            title="Sample annotation coverage (RNA-Seq)",
            save=True,
            outfile=f"figures/attribute_upset_plot__level-sample__tech-rnaseq.{FMT}",
            dpi=500,
            ylim=100_000,
        )

        # ========== Study ============
        upset_plot(
            study_records_microarray,
            title="Study annotation coverage (microarray)",
            save=True,
            outfile=f"figures/attribute_upset_plot__level-study__tech-microarray.{FMT}",
            dpi=500,
            ylim=5_500,
        )
        # ========== Study ============
        upset_plot(
            study_records_rnaseq,
            title="Study annotation coverage (RNA-Seq)",
            save=True,
            outfile=f"figures/attribute_upset_plot__level-study__tech-rnaseq.{FMT}",
            dpi=500,
            ylim=5_500
        )
    return


@app.cell
def _(mo):
    mo.md(r"""
    # Get source counts
    """)
    return


@app.cell
def _(Literal, defaultdict, pl):
    def acceptable_platform_sample(entry, ok_platforms):
        if "platform" in entry["accession_ids"]:
            gpl = entry["accession_ids"]["platform"]
            if gpl in ok_platforms:
                return True

        return False

    def acceptable_platform_study(entry, ok_platforms):
        if "platform" in entry["accession_ids"]:
            gpls = entry["accession_ids"]["platform"].split("||")
            if len(set(gpls) & set(ok_platforms)) > 0:
                return True

        return False


    def get_source_counts(
        database: dict,
        level: Literal["sample", "study"],
        platforms: pl.DataFrame,
    ):
        platform_mapping_funcs = {"sample": acceptable_platform_sample, "study": acceptable_platform_study}
        is_acceptable_platform = platform_mapping_funcs[level]

        # collect all unique sources across all attributes
        all_sources = set()
        for id_, data in database.items():
            for attribute in ["tissue", "disease", "sex", "age"]:
                if attribute in data:
                    all_sources.update(data[attribute].keys())

        # store results for each technology
        all_results = []
        attributes = ["tissue", "disease", "sex", "age"]

        # count sources for each attribute across all GSM IDs, separated by technology
        for technology in platforms["technology"].unique():
            ok_platforms = platforms.filter(pl.col("technology") == technology)["id"].to_list()

            tissue_sources = defaultdict(int)
            disease_sources = defaultdict(int)
            sex_sources = defaultdict(int)
            age_sources = defaultdict(int)

            for id_, data in database.items():

                if not is_acceptable_platform(data, ok_platforms):
                    continue

                if "tissue" in data:
                    for source, source_data in data["tissue"].items():
                        if "id" in source_data:
                            tissue_sources[source] += 1

                if "disease" in data:
                    for source, source_data in data["disease"].items():
                        if "id" in source_data:
                            disease_sources[source] += 1

                if "sex" in data:
                    for source, source_data in data["sex"].items():
                        if "id" in source_data:
                            sex_sources[source] += 1

                if "age" in data:
                    for source, source_data in data["age"].items():
                        if "id" in source_data:
                            age_sources[source] += 1

            for attribute, source_dict in [
                ("tissue", tissue_sources),
                ("disease", disease_sources),
                ("sex", sex_sources),
                ("age", age_sources)
            ]:
                df = pl.DataFrame(
                    {
                        "technology": [technology for _ in all_sources],
                        "attribute": [attribute for _ in all_sources],
                        "source": list(all_sources),
                        "count": [source_dict.get(source, 0) for source in all_sources],
                    }
                )
                all_results.append(df)

        return pl.concat(all_results)

    return (get_source_counts,)


@app.cell
def _(PLATFORMS_FILE, get_source_counts, pl, sample_db, series_db):
    platforms = pl.read_parquet(PLATFORMS_FILE)

    sample_source_counts = get_source_counts(sample_db, "sample", platforms)
    study_source_counts = get_source_counts(series_db, "study", platforms)
    return sample_source_counts, study_source_counts


@app.cell
def _(mo):
    mo.md(r"""
    # Plot source counts
    """)
    return


@app.cell
def _(COLORS, Patch, ceil, mcolors, pl, plt, sns, ticker):
    def lighten_color(color, amount=0.5):
        try:
            c = mcolors.to_rgb(color)
        except ValueError:
            c = mcolors.to_rgb(mcolors.cnames[color])
        c = [(1 - amount) * comp + amount for comp in c]
        return c

    def plot_source_counts_by_attribute(
        df: pl.DataFrame,
        ylabel: str,
        attributes: list[str],
        figsize: tuple[int, int]=(10, 8),
        title: str = "",
        save: bool=False,
        outfile: str | None = None,
        dpi: int = 500,
        ylim_scale: int = 1,
        verbose: bool = False,
    ):

        TECHNOLOGY_COLORS = {
            "microarray": "dimgrey",
            "rnaseq": "lightgrey",
        }

        def get_tech_color(tech, base_color):
            if tech == "microarray":
                return base_color
            elif tech == "rnaseq":
                return lighten_color(base_color, amount=0.6)
            else:
                return base_color

        fig, axes = plt.subplots(2, 2, figsize=figsize)
        axes = axes.flatten()

        technologies = df["technology"].unique().sort().to_list()
        all_sources = sorted(df["source"].unique().to_list())

        for idx, attribute in enumerate(attributes):
            df_filtered = df.filter(pl.col("attribute") == attribute)

            df_plot = df_filtered.with_columns(
                pl.col("source").cast(pl.Enum(all_sources))
            ).sort("source")

            dark_color = COLORS[attribute]
            palette = {tech: get_tech_color(tech, dark_color) for tech in technologies}

            sns.barplot(
                data=df_plot.to_pandas(),
                y="count",
                x="source",
                hue="technology",
                ax=axes[idx],
                palette=palette,
            )

            if axes[idx].get_legend():
                axes[idx].get_legend().remove()

            axes[idx].tick_params(axis='x', rotation=90)
            for label in axes[idx].get_xticklabels():
                label.set_ha('center')
            axes[idx].set_title(f'{attribute.capitalize()}', fontsize=14)
            axes[idx].set_xlabel('', fontsize=12)
            axes[idx].set_ylabel(ylabel, fontsize=12)
            axes[idx].set_ylim(0, (ceil(df_plot['count'].max() / ylim_scale) * ylim_scale))
            axes[idx].grid(axis='y', alpha=0.3)

            axes[idx].yaxis.set_major_formatter(ticker.FuncFormatter(lambda x, p: f'{int(x):,}'))

            sns.despine(ax=axes[idx], top=True, right=True, left=True)

            if verbose:
                print(f"\n{attribute.capitalize()} annotations by technology:")
                for tech in df_filtered["technology"].unique():
                    tech_total = df_filtered.filter(pl.col("technology") == tech)["count"].sum()
                    print(f"  {tech}: {tech_total:,}")

        legend_elements = [
            Patch(
                facecolor=get_tech_color(tech, "dimgrey"),
                label=tech
            )
            for tech in technologies
        ]

        fig.legend(handles=legend_elements, title='Technology', loc='upper right',
                   bbox_to_anchor=(0.98, 0.86), fontsize=10)

        plt.suptitle(title, fontsize=14, fontweight='bold')
        plt.tight_layout()

        if save and isinstance(outfile, str):
            plt.savefig(outfile, dpi=dpi, bbox_inches="tight")
        plt.show()

    return lighten_color, plot_source_counts_by_attribute


@app.cell
def _(mo):
    mo.md(r"""
    ## Sample
    """)
    return


@app.cell
def _(ATTRIBUTES, FMT, plot_source_counts_by_attribute, sample_source_counts):
    plot_source_counts_by_attribute(
        sample_source_counts,
        "Samples",
        ATTRIBUTES,
        figsize=(8,8),
        title="Attribute source counts (level=sample)",
        save=True,
        outfile=f"figures/source_counts_by_attribute__level-sample__tech-all.{FMT}",
        dpi=500,
        ylim_scale=10_000,
    )
    return


@app.cell
def _(mo):
    mo.md(r"""
    ## Study
    """)
    return


@app.cell
def _(ATTRIBUTES, FMT, plot_source_counts_by_attribute, study_source_counts):
    plot_source_counts_by_attribute(
        study_source_counts,
        "Studies",
        ATTRIBUTES,
        figsize=(8,8),
        title="Attribute source counts (level=study)",
        save=True,
        outfile=f"figures/source_counts_by_attribute__level-study__tech-all.{FMT}",
        dpi=500,
        ylim_scale=1000,
    )
    return


@app.cell
def _(mo):
    mo.md(r"""
    ## Number of available tissues and diseases
    """)
    return


@app.cell
def _(mo):
    mo.md(r"""
    ### Direct annotations
    """)
    return


@app.function
def get_direct_annotations(data: dict, attributes: list[str] = ["tissue", "disease"]):
    direct_annotations = {attribute: set() for attribute in attributes}
    for anno in data.values():
        for attribute in attributes:
            if attribute not in anno:
                continue
            for source in anno[attribute]:
                for entry in anno[attribute][source]["id"].split("|"):
                    direct_annotations[attribute].add(entry)

    return direct_annotations


@app.cell
def _(sample_db, series_db):
    sample_tissue_disease_direct_annotations = get_direct_annotations(sample_db)
    study_tissue_disease_direct_annotations = get_direct_annotations(series_db)
    return (
        sample_tissue_disease_direct_annotations,
        study_tissue_disease_direct_annotations,
    )


@app.cell
def _(
    sample_tissue_disease_direct_annotations,
    study_tissue_disease_direct_annotations,
):
    unique_tissues = sample_tissue_disease_direct_annotations["tissue"] & study_tissue_disease_direct_annotations["tissue"]
    unique_diseases = (sample_tissue_disease_direct_annotations["disease"] & study_tissue_disease_direct_annotations["disease"])

    print(f"Number of unique tissues that are directly annotated: {len(unique_tissues)}")
    print(f"Number of unique diseases that are directly annotated: {len(unique_diseases)}")
    return


@app.cell
def _(mo):
    mo.md(r"""
    ### Propagated annotations
    """)
    return


@app.cell
def _(UNIQUE_PROPAGATED_TERMS: "Path", load_txt):
    unique_propagated_terms = load_txt(UNIQUE_PROPAGATED_TERMS)

    for attribute in ["tissue", "disease"]:

        terms = set()
        for term in unique_propagated_terms:
            if (attribute == "tissue") and (term.startswith("UBERON") or term.startswith("CL")):
                terms.add(term)
            if (attribute == "disease") and (term.startswith("MONDO")):
                terms.add(term)

        print(f"Number of unique {attribute}s in propagated annotations: {len(terms)}")
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    # Annotation overlap across sources
    """)
    return


@app.cell
def _(Literal, Path, pl, pmi_from_cooccurrence, re):
    def match_pattern(text: str, pattern: str) -> str:
        match = re.search(pattern, text)
        if match:
            return match.group(1)
        else:
            return ""


    def get_overlap_results(
        dir_: Path,
        overlap_type: str,
        level: Literal["sample", "series"],
        attribute_pattern: str = r"attribute-(tissue|disease|sex|age)",
        level_pattern: str = r"level-(sample|series)",
        pmi: bool = False,
        separator="\t",
        **pmi_kwargs,
    ) -> dict[str, pl.DataFrame]:
        results: dict[str, pl.DataFrame] = {}

        files = list(dir_.glob(f"{overlap_type}*"))

        for file in files:

            file_level = match_pattern(file.stem, level_pattern)
            if file_level != level:
                continue

            attribute = match_pattern(file.stem, attribute_pattern)
            df = pl.read_csv(file, separator=separator)
            if pmi:
                values = pmi_from_cooccurrence(df.to_numpy(), **pmi_kwargs)
                df = pl.DataFrame(values, schema=df.columns)

            results[attribute] = df

        if len(results) == 0:
            print("No files found that met conditions:")
            print(f"Directory: {dir}")
            print(f"Overlap type: {overlap_type}")
            print(f"Level: {level}")
            print(f"Attribute pattern: {attribute_pattern}")
            print(f"Level pattern: {level_pattern}")
            print(f"Files: {files}")
            raise RuntimeError()

        return results

    return (get_overlap_results,)


@app.cell
def _(Literal, np):
    def pmi_from_cooccurrence(x: np.typing.NDArray, method: Literal["positive", "norm"] | None = None):
        """Compute pointwise mutual information for all pairs from a 
        symmetric co-occurrence matrix.

        Arguments:
            x (NDArray):
                2D array-like, symmetric count matrix.
            positive (bool):
                If True, return PPMI.
            method (Literal['positive', 'norm'] | None):
                Compute ppmi or npmi.

        Returns:
            (NDArray): square 2D numpy array of PMI values
        """
        x = np.array(x, dtype=float)
        total = x.sum()
        col_sums = x.sum(axis=1)

        joint = x / total
        outer_marginals = np.outer(col_sums, col_sums) / (total ** 2)

        with np.errstate(divide='ignore', invalid='ignore'):
            pmi = np.where(
                joint > 0,
                np.log2(joint / outer_marginals),
                -np.inf
            )

        if method == "positive":
            pmi = np.maximum(pmi, 0)

        if method == "norm":
            with np.errstate(divide='ignore', invalid='ignore'):
                pmi = np.where(
                    joint > 0,
                    pmi / -np.log2(joint),
                    np.nan
                )

        np.fill_diagonal(pmi, np.nan)
        return pmi

    return (pmi_from_cooccurrence,)


@app.cell
def _(Path, np, pl, plt, sns):
    def plot_overlap_heatmap(
        overlap_results: dict[str, pl.DataFrame],
        subplot_shape: tuple[int, int] = (2, 2),
        figsize_per_plot: tuple[int, int] = (5, 5),
        title: str = "",
        order: list[str] | None = None,
        save: bool = False,
        dpi: int = 400,
        outfile: Path | str | None = None,
        vmax_percentile: float | int | None = None,
        **heatmap_kwargs,
    ):
        nrows, ncols = subplot_shape
        fig, axes = plt.subplots(
            nrows,
            ncols,
            figsize=(figsize_per_plot[0] * ncols, figsize_per_plot[1] * nrows),
        )

        if isinstance(axes, np.ndarray):
            axes_flat = axes.flatten()
        else:
            axes_flat = [axes]

        if isinstance(order, list):
            overlap_results = {k: overlap_results[k] for k in order}

        for ax, (group_name, df) in zip(axes_flat, overlap_results.items()):
            df = (
                df.with_columns(pl.Series("source", df.columns))
                .to_pandas()
                .set_index("source", drop=True)
            )

            if "vmax" not in heatmap_kwargs and (isinstance(vmax_percentile, (int, float))):
                vmax = np.percentile(df.to_numpy(), vmax_percentile)
                sns.heatmap(df, ax=ax, vmax=vmax, **heatmap_kwargs)
            else:
                sns.heatmap(df, ax=ax, **heatmap_kwargs)

            ax.set_title(group_name.capitalize())
            xticks = ax.get_xticklabels()
            ax.set_xticklabels(labels=xticks, rotation=45, ha='right', rotation_mode='anchor')
            ax.set_yticklabels(ax.get_yticklabels(), rotation=0, va="center")

        for ax in axes_flat[len(overlap_results):]:
            ax.axis("off")

        plt.suptitle(title, fontsize=14, fontweight="bold")
        plt.tight_layout()

        if save and isinstance(outfile, (str, Path)):
            plt.savefig(outfile, dpi=dpi, bbox_inches="tight")

        plt.show()

    return (plot_overlap_heatmap,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Sample
    """)
    return


@app.cell
def _(POST_HARMONIZATION_RESULTS, get_overlap_results):
    post_sample_overlap_count = get_overlap_results(
        POST_HARMONIZATION_RESULTS,
        overlap_type="overlap_count",
        level="sample",
    )

    post_sample_overlap_percent = get_overlap_results(
        POST_HARMONIZATION_RESULTS,
        overlap_type="overlap_percent",
        level="sample",
    )

    post_sample_overlap_pmi = get_overlap_results(
        POST_HARMONIZATION_RESULTS,
        overlap_type="overlap_count",
        level="sample",
        pmi=True,
        method="norm",
    )
    return (
        post_sample_overlap_count,
        post_sample_overlap_percent,
        post_sample_overlap_pmi,
    )


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### Absolute count overlap
    """)
    return


@app.cell
def _(
    FIGURES_DIR: "Path",
    OVERLAP_CMAP,
    OVERLAP_ORDER,
    plot_overlap_heatmap,
    post_sample_overlap_count,
):
    plot_overlap_heatmap(
        post_sample_overlap_count,
        order=OVERLAP_ORDER,
        cmap=OVERLAP_CMAP,
        vmax_percentile=95,
        title="Absolute count overlap (level=sample)",
        save=True,
        outfile=FIGURES_DIR / "post_harmonization_overlap__level-sample__metric-counts.png"
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### Percent overlap
    """)
    return


@app.cell
def _(
    FIGURES_DIR: "Path",
    OVERLAP_CMAP,
    OVERLAP_ORDER,
    plot_overlap_heatmap,
    post_sample_overlap_percent,
):
    plot_overlap_heatmap(
        post_sample_overlap_percent,
        order=OVERLAP_ORDER,
        vmax=1.0,
        vmin=0.0,
        cmap=OVERLAP_CMAP,
        title="Percent overlap (level=sample)",
        save=True,
        outfile=FIGURES_DIR / "post_harmonization_overlap__level-sample__metric-percent.png",
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### Normalized PMI
    """)
    return


@app.cell
def _(
    FIGURES_DIR: "Path",
    OVERLAP_ORDER,
    PMI_CMAP,
    plot_overlap_heatmap,
    post_sample_overlap_pmi,
):
    plot_overlap_heatmap(
        post_sample_overlap_pmi,
        order=OVERLAP_ORDER,
        cmap=PMI_CMAP,
        vmax=1,
        vmin=-1,
        title="Normalized pointwise mutual information (level=sample)",
        save=True,
        outfile=FIGURES_DIR / "post_harmonization_overlap__level-sample__metric-pmi.png",
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Series
    """)
    return


@app.cell
def _(POST_HARMONIZATION_RESULTS, get_overlap_results):
    post_series_overlap_count = get_overlap_results(
        POST_HARMONIZATION_RESULTS,
        overlap_type="overlap_count",
        level="series",
    )

    post_series_overlap_percent = get_overlap_results(
        POST_HARMONIZATION_RESULTS,
        overlap_type="overlap_percent",
        level="series",
    )

    post_series_overlap_pmi = get_overlap_results(
        POST_HARMONIZATION_RESULTS,
        overlap_type="overlap_count",
        level="series",
        pmi=True,
        method="norm",
    )
    return (
        post_series_overlap_count,
        post_series_overlap_percent,
        post_series_overlap_pmi,
    )


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### Absolute count overlap
    """)
    return


@app.cell
def _(
    FIGURES_DIR: "Path",
    OVERLAP_CMAP,
    OVERLAP_ORDER,
    plot_overlap_heatmap,
    post_series_overlap_count,
):
    plot_overlap_heatmap(
        post_series_overlap_count,
        order=OVERLAP_ORDER,
        cmap=OVERLAP_CMAP,
        vmax_percentile=95,
        title="Absolute count overlap (level=study)",
        save=True,
        outfile=FIGURES_DIR / "post_harmonization_overlap__level-study__metric-counts.png"
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### Percent overlap
    """)
    return


@app.cell
def _(
    FIGURES_DIR: "Path",
    OVERLAP_CMAP,
    OVERLAP_ORDER,
    plot_overlap_heatmap,
    post_series_overlap_percent,
):
    plot_overlap_heatmap(
        post_series_overlap_percent,
        order=OVERLAP_ORDER,
        cmap=OVERLAP_CMAP,
        vmax_percentile=95,
        title="Percent overlap (level=study)",
        save=True,
        outfile=FIGURES_DIR / "post_harmonization_overlap__level-study__metric-percent.png"
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### Normalized PMI
    """)
    return


@app.cell
def _(
    FIGURES_DIR: "Path",
    OVERLAP_ORDER,
    PMI_CMAP,
    plot_overlap_heatmap,
    post_series_overlap_pmi,
):
    plot_overlap_heatmap(
        post_series_overlap_pmi,
        order=OVERLAP_ORDER,
        cmap=PMI_CMAP,
        vmax=1,
        vmin=-1,
        title="Normalized pointwise mutual information (level=study)",
        save=True,
        outfile=FIGURES_DIR / "post_harmonization_overlap__level-study__metric-pmi.png",
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    # Annotation coverage improvement analysis
    The following plots show the annotation coverage improvement for samples and studies post-harmonization. The x-axis represents the total number of samples/studies from a particular annotation source that have an annotation for a particular attribute post-harmonization.
    """)
    return


@app.cell
def _(pl):
    def collect_db_anno(
        db: dict,
        attributes: list[str],
        entry_prefix: str | tuple[str, ...],
    ) -> pl.DataFrame:
        results = {"accession": [], "attribute": [], "source": []}
        for entry, contents in db.items():

            if not entry.startswith(entry_prefix):
                continue

            for attribute in attributes:
                if attribute not in contents:
                    continue

                for source in contents[attribute]:
                    results["accession"].append(entry)
                    results["attribute"].append(attribute)
                    results["source"].append(source)

        return pl.DataFrame(results).sort(["accession", "source", "attribute"])

    return (collect_db_anno,)


@app.cell
def _(pl):
    def extract_source_annotation_count_differences(
        raw: pl.DataFrame, new: pl.DataFrame, attributes: list[str], source: str
    ) -> pl.DataFrame:
        shared_entries = set(
            raw.filter(pl.col("source") == source)
            .join(new, on="accession", how="inner")["accession"]
            .unique()
            .to_list()
        )
        n_shared_entries = len(shared_entries)

        results = {
            "attribute": [],
            "num_old": [],
            "percent_old": [],
            "num_new": [],
            "percent_new": [],
        }
        for attribute in attributes:
            # shared entries that had attribute A annotated from source S (in raw)
            S_A_shared = set(
                raw.filter(
                    (pl.col("source") == source)
                    & (pl.col("attribute") == attribute)
                    & (pl.col("accession").is_in(shared_entries))
                )["accession"]
                .unique()
                .to_list()
            )

            # shared entries that have attribute A annotated from any source in new (O)
            O_A_shared = set(
                new.filter(
                    (pl.col("attribute") == attribute)
                    & (pl.col("accession").is_in(shared_entries))
                    & (~pl.col("accession").is_in(S_A_shared))
                )["accession"]
                .unique()
                .to_list()
            )

            num_old = len(S_A_shared)
            num_new = len(O_A_shared)
            total = num_old + num_new
            percent_old = num_old / total

            num_new = len(O_A_shared)
            percent_new = num_new / total

            results["attribute"].append(attribute)
            results["num_old"].append(num_old)
            results["percent_old"].append(percent_old)
            results["num_new"].append(num_new)
            results["percent_new"].append(percent_new)

        return pl.DataFrame(results)

    return (extract_source_annotation_count_differences,)


@app.cell
def _(extract_source_annotation_count_differences, pl):
    def add_missing_attribute_counts(df: pl.DataFrame, col_count: str, attributes: list[str]) -> pl.DataFrame:
        additional_rows = {"attribute": [], col_count: []}
        for attribute in attributes:
            if attribute not in df["attribute"]:
                additional_rows["attribute"].append(attribute)
                additional_rows[col_count].append(0)

        additional_df = pl.DataFrame(additional_rows, schema=df.schema)
        return pl.concat([df, additional_df], how="vertical").sort("attribute")


    def extract_annotation_count_differences(
        raw: pl.DataFrame, new: pl.DataFrame, attributes: list[str]
    ) -> pl.DataFrame:
        results: list[pl.DataFrame] = []
        for source in raw["source"].unique():
            result = extract_source_annotation_count_differences(raw, new, attributes, source=source)
            results.append(
                result.with_columns(pl.lit(source).alias("source"))
            )
        return pl.concat(results, how="vertical")

    return (extract_annotation_count_differences,)


@app.cell
def _(Path, lighten_color, mpatches, pl, plt, sns):
    def plot_coverage_by_attribute_stacked(
        df: pl.DataFrame,
        attribute_color_map: dict,
        attributes: list[str] = None,
        figsize: tuple[int, int] = (12, 10),
        title: str = "",
        save: bool = False,
        outfile: str | None = None,
        dpi: int = 500,
    ) -> plt.Figure:

        if attributes is None:
            attributes = df["attribute"].unique(maintain_order=True).to_list()

        sources = sorted(df["source"].unique().to_list())

        fig, axes = plt.subplots(2, 2, figsize=figsize)
        axes = axes.flatten()

        for idx, attr in enumerate(attributes):
            ax = axes[idx]

            subset_pd = (
                df.filter(pl.col("attribute") == attr)
                  .select(["source", "percent_old", "percent_new", "num_old", "num_new"])
                  .with_columns(pl.col("source").cast(pl.Enum(sources)))
                  .sort("source")
                  .to_pandas()
            )

            dark_color = attribute_color_map.get(attr, "dimgrey")
            light_color = lighten_color(dark_color, amount=0.6)

            # pre-harmonization
            sns.barplot(
                data=subset_pd,
                y="source",
                x=subset_pd["percent_old"] + subset_pd["percent_new"],
                color=light_color,
                order=sources,
                orient="h",
                saturation=1.0,
                label="Post-harmonization",
                ax=ax,
            )

            # post-harmonization
            sns.barplot(
                data=subset_pd,
                y="source",
                x="percent_old",
                color=dark_color,
                order=sources,
                orient="h",
                saturation=1.0,
                label="Raw",
                ax=ax,
            )

            # add numbers to bars
            n = len(sources)
            patches = ax.patches
            light_patches = patches[:n]
            dark_patches  = patches[n:]

            tol = 0.01  # tolerance for considering two widths the same

            for light_patch, dark_patch, num_new, num_old in zip(
                light_patches, dark_patches,
                subset_pd["num_new"], subset_pd["num_old"],
            ):
                light_width = light_patch.get_width()
                dark_width  = dark_patch.get_width()
                y_center    = light_patch.get_y() + light_patch.get_height() / 2

                zero_dark  = dark_width < tol
                same_width = abs(light_width - dark_width) < tol

                if zero_dark:
                    # only light bar visible -> show just num_new to the right
                    ax.text(
                        light_width + 0.01, y_center,
                        f"{int(num_new):,}",
                        va="center", ha="left", fontsize=8, color=dark_color,
                    )
                elif same_width:
                    # percent_new ~ 0% -> show num_old in the middle of the dark bar only
                    ax.text(
                        dark_width / 2, y_center,
                        f"{int(num_old):,}",
                        va="center", ha="center", fontsize=8, color="white",
                    )
                else:
                    # normal -> num_old in the middle of dark bar, num_new to the right
                    ax.text(
                        dark_width / 2, y_center,
                        f"{int(num_old):,}",
                        va="center", ha="center", fontsize=8, color="white",
                    )
                    ax.text(
                        light_width + 0.01, y_center,
                        f"{int(num_new):,}",
                        va="center", ha="left", fontsize=8, color=dark_color,
                    )

            ax.set_xlim(0, 1)
            ax.xaxis.set_major_formatter(plt.FuncFormatter(lambda x, _: f"{x:.0%}"))
            ax.set_xlabel("Coverage", fontsize=12)
            ax.set_ylabel("", fontsize=12)
            ax.set_title(attr.capitalize(), fontsize=14)
            ax.tick_params(axis="y", length=0)
            ax.grid(axis="x", alpha=0.3)
            sns.despine(ax=ax, left=True)

            if ax.get_legend():
                ax.get_legend().remove()

        for idx in range(len(attributes), len(axes)):
            axes[idx].set_visible(False)

        dark_color_fallback = "dimgrey"
        light_color_fallback = lighten_color(dark_color_fallback, amount=0.6)
        legend_elements = [
            mpatches.Patch(facecolor=dark_color_fallback, label="Original source"),
            mpatches.Patch(facecolor=light_color_fallback, label="Post-harmonization"),
        ]

        fig.legend(
            handles=legend_elements,
            title="Coverage",
            loc="upper right",
            bbox_to_anchor=(0.88, 0.4),
            fontsize=10,
        )

        plt.suptitle(title, fontsize=14, fontweight="bold")
        plt.tight_layout()

        if save and isinstance(outfile, (str, Path)):
            fig.savefig(outfile, dpi=dpi, bbox_inches="tight")

        plt.show()

    return (plot_coverage_by_attribute_stacked,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Get pre-harmonized annotations.

    These are annotations that have been formatted into the same schema. Some annotations from the original sources have been removed based on our inclusion criteria:

    * If an annotation can map to UBERON/CL (tissue), MONDO (disease), M/F (sex), or an age group (age) either autonomously or manually.
    * Annotations to very high level tissue or disease terms (e.g., anatomical system) since we posit that these are not informative annotations and are essentially equivalent to not having an annotation.
    * If an entry accession ID cannot be mapped to a GEO accession ID.
    * If annotations are for an entry that is not from microarray or RNA-Seq.
    """)
    return


@app.cell
def _(
    ATTRIBUTES,
    GEO_PROCESSED,
    SEMI_PROCESSED_SERIES,
    SRA_PROCESSED,
    collect_db_anno,
    load_bson,
    pl,
    sample_db,
    series_db,
):
    # geo_anno contains annotations from sources that provided annotations for GEO accession IDs.
    geo_anno = load_bson(GEO_PROCESSED)
    geo_anno = collect_db_anno(geo_anno, ATTRIBUTES, entry_prefix="GSM")

    # sra_anno contains annotations from sources that provided from annotations for SRA accession
    # IDs, but are now mapped to GEO IDs.
    sra_anno = load_bson(SRA_PROCESSED)
    sra_anno = collect_db_anno(sra_anno, ATTRIBUTES, entry_prefix="GSM")

    # convert dictionary format to DataFrame - sample
    raw_sample_df = pl.concat([geo_anno, sra_anno], how="vertical").unique() 
    new_sample_df = (
        collect_db_anno(sample_db, ATTRIBUTES, entry_prefix="GSM")
            .unique(["accession", "attribute", "source"])
    )

    # convert dictionary format to DataFrame - series
    raw_series_df = collect_db_anno(load_bson(SEMI_PROCESSED_SERIES), ATTRIBUTES, entry_prefix="GSE")
    new_series_df = (
        collect_db_anno(series_db, ATTRIBUTES, entry_prefix="GSE")
            .unique(["accession", "attribute", "source"])
    )
    return new_sample_df, new_series_df, raw_sample_df, raw_series_df


@app.cell
def _(
    ATTRIBUTES,
    extract_annotation_count_differences,
    new_sample_df,
    new_series_df,
    raw_sample_df,
    raw_series_df,
):
    # get count differences between the pre- and post-harmonized annotations
    source_harmonization_improvements_sample = extract_annotation_count_differences(
        raw_sample_df, new_sample_df, ATTRIBUTES
    )
    source_harmonization_improvements_series = extract_annotation_count_differences(
        raw_series_df, new_series_df, ATTRIBUTES
    )
    return (
        source_harmonization_improvements_sample,
        source_harmonization_improvements_series,
    )


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Plots
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### Sample
    """)
    return


@app.cell
def _(
    ATTRIBUTES,
    COLORS,
    FIGURES_DIR: "Path",
    plot_coverage_by_attribute_stacked,
    source_harmonization_improvements_sample,
):
    plot_coverage_by_attribute_stacked(
        source_harmonization_improvements_sample,
        COLORS,
        ATTRIBUTES,
        title="Post-harmonzation annotation coverage (level=sample)",
        save=True,
        outfile = FIGURES_DIR / "annotation_coverage_improvements_by_source__level-sample.png",
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### Series
    """)
    return


@app.cell
def _(
    ATTRIBUTES,
    COLORS,
    FIGURES_DIR: "Path",
    plot_coverage_by_attribute_stacked,
    source_harmonization_improvements_series,
):
    plot_coverage_by_attribute_stacked(
        source_harmonization_improvements_series,
        COLORS,
        ATTRIBUTES,
        title="Post-harmonzation annotation coverage (level=study)",
        save=True,
        outfile = FIGURES_DIR / "annotation_coverage_improvements_by_source__level-study.png",
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    # Information content of original source annotations vs MetaHQ
    """)
    return


@app.cell
def _(Path, np, plt, sns):
    def plot_ic_comparison(
        df,
        *,
        source_col="original_source",
        attribute_col="attribute",
        value_cols=("original_ic", "metahq_ic"),
        pipeline_labels=("original", "metahq"),
        provenance_col="metahq_source",
        attributes=("tissue", "disease"),
        sources=None,
        levels=8,
        thresh=0.02,
        bw_adjust=1.0,
        cmap="mako_r",
        min_kde_points=10,
        show_points=False,
        jitter=0.03,
        shared_limits=True,
        panel_size=4.2,
        tick_labelsize=13,
        axis_labelsize=15,
        title_fontsize=17,
        legend_fontsize=13,
        source_labelsize=18,
        seed=0,
        max_rows=4,
        outfile=None,
        dpi=600,
    ):
        """Grid of IC comparisons, one row per source and three columns.

        Columns are: a 2D KDE of original vs. metahq IC for the first attribute,
        the same for the second, and a grouped boxplot of both pipelines split by
        attribute. Panels with too few points, or no spread in either axis, fall
        back to a scatter because the KDE covariance would be singular.

        Sources are split across several figures of at most `max_rows` rows each,
        so a large number of sources does not produce one unreadably tall figure.

        Parameters
        ----------
        df : polars.DataFrame or pandas.DataFrame
            One row per accession x attribute. Converted to pandas if needed.
        source_col : str
            Column that determines the row partitioning.
        value_cols, pipeline_labels : sequence of str
            The two IC columns to compare, and the names they get in the boxplot
            legend. Given in the order (x-axis, y-axis).
        provenance_col : str or None
            Compared against `source_col` to flag reassigned annotations, which
            colors the point overlay. Pass None to skip the flag.
        attributes : sequence of str
            Values of `attribute_col` to give a density column each. Determines
            the number of density panels, so passing three makes a four-column grid.
        sources : sequence of str or None
            Restrict and order the rows. Defaults to every source, sorted.
        levels, thresh, bw_adjust, cmap
            Passed through to `seaborn.kdeplot`. Lower `bw_adjust` tightens the
            contours; useful when IC values are tightly clustered.
        min_kde_points : int
            Panels with fewer observations get a scatter instead of a density.
        show_points : bool
            Overlay the raw observations on top of the density.
        jitter : float
            Gaussian jitter applied to the point overlay only, never the density.
        shared_limits : bool
            Use one set of axis limits across every panel, so the identity line
            is comparable between rows and between figures. Otherwise limits are
            per row.
        panel_size : float
            Height in inches of a single row.
        tick_labelsize, axis_labelsize, title_fontsize, legend_fontsize : float
            Point sizes for the tick labels, the x/y axis labels, the panel
            titles, and the legend entries. Applied through an rc context so
            seaborn's own artists pick them up too.
        source_labelsize : float
            Point size of the rotated source name down the left edge of each row.
        max_rows : int or None
            Maximum number of source rows per figure. Sources are taken in order
            and split into consecutive chunks of this size. Pass None to put every
            source on one figure, as before.
        outfile : str or pathlib.Path or None
            If given, each figure is written next to this path. With more than one
            chunk the stem gets a `_part01`, `_part02`, ... suffix; with a single
            chunk the path is used verbatim.
        dpi : int
            Resolution for the saved files.

        Returns
        -------
        list of (matplotlib.figure.Figure, numpy.ndarray of Axes)
            One entry per figure, in source order. Note this is a list even when
            only one figure is produced.
        """
        x_col, y_col = value_cols
        x_label, y_label = pipeline_labels
 
        pdf = df.to_pandas() if hasattr(df, "to_pandas") else df.copy()
 
        if provenance_col is not None:
            pdf["_reassigned"] = np.where(
                pdf[provenance_col] == pdf[source_col], "same source", "reassigned"
            )
 
        if sources is None:
            sources = sorted(pdf[source_col].unique())
        else:
            sources = list(sources)
        attributes = list(attributes)
 
        long = pdf.melt(
            id_vars=[c for c in pdf.columns if c not in value_cols],
            value_vars=list(value_cols),
            var_name="pipeline",
            value_name="ic",
        )
        long["pipeline"] = long["pipeline"].map(dict(zip(value_cols, pipeline_labels)))
 
        rng = np.random.default_rng(seed)
 
        def _jitter(values):
            if not jitter:
                return values
            return values + rng.normal(0, jitter, size=len(values))
 
        def _limits(frame):
            lo = min(frame[x_col].min(), frame[y_col].min())
            hi = max(frame[x_col].max(), frame[y_col].max())
            pad = 0.05 * (hi - lo) or 0.5
            return (lo - pad, hi + pad)
 
        # Computed over the whole frame, so limits stay comparable across figures.
        global_lims = _limits(pdf) if shared_limits else None
 
        n_cols = len(attributes) + 1
 
        def _render(chunk):
            """Build one figure holding the rows in `chunk`."""
            fig, axes = plt.subplots(
                nrows=len(chunk),
                ncols=n_cols,
                figsize=(4.6 * n_cols, panel_size * len(chunk)),
                squeeze=False,
            )
 
            for i, source in enumerate(chunk):
                sub = pdf[pdf[source_col] == source]
                lims = global_lims if shared_limits else _limits(sub)
 
                for j, attr in enumerate(attributes):
                    ax = axes[i, j]
                    d = sub[sub[attribute_col] == attr]
                    x = d[x_col].to_numpy()
                    y = d[y_col].to_numpy()
 
                    kde_ok = len(d) >= min_kde_points and x.std() > 0 and y.std() > 0
 
                    if kde_ok:
                        shared = dict(
                            x=x,
                            y=y,
                            levels=levels,
                            thresh=thresh,
                            bw_adjust=bw_adjust,
                            clip=(lims, lims),
                            warn_singular=False,
                            ax=ax,
                        )
                        sns.kdeplot(fill=True, cmap=cmap, zorder=1, **shared)
                        sns.kdeplot(color="0.3", linewidths=0.6, zorder=2, **shared)
 
                    if show_points or not kde_ok:
                        sns.scatterplot(
                            x=_jitter(x),
                            y=_jitter(y),
                            hue=d["_reassigned"] if provenance_col else None,
                            hue_order=(
                                ["same source", "reassigned"] if provenance_col else None
                            ),
                            palette=(
                                {"same source": "0.55", "reassigned": "#d1495b"}
                                if provenance_col
                                else None
                            ),
                            color=None if provenance_col else "0.4",
                            alpha=0.35 if kde_ok else 0.6,
                            s=18,
                            edgecolor="none",
                            zorder=3,
                            # Legend on the first panel of every figure, not just
                            # the first figure, since each file stands alone.
                            legend=bool(provenance_col) and i == 0 and j == 0,
                            ax=ax,
                        )
 
                    # Identity line last, so it stays readable over the density.
                    ax.plot(lims, lims, ls="--", lw=1.2, color="0.35", zorder=4)
 
                    title = f"{attr} (n = {len(d):,})"
                    ax.set(
                        xlim=lims,
                        ylim=lims,
                        xlabel=f"{x_label} IC",
                        ylabel=f"{y_label} IC" if j == 0 else "",
                        title=title,
                    )
                    ax.set_aspect("equal", adjustable="box")
 
                ax = axes[i, -1]
                sns.boxplot(
                    data=long[long[source_col] == source],
                    x=attribute_col,
                    y="ic",
                    hue="pipeline",
                    order=attributes,
                    hue_order=list(pipeline_labels),
                    showfliers=False,
                    width=0.6,
                    ax=ax,
                )
                ax.set(xlabel="", ylabel="IC", title="IC distribution")
                ax.legend(title="", loc="lower right", fontsize=legend_fontsize)
 
                axes[i, 0].text(
                    -0.32,
                    0.5,
                    source,
                    transform=axes[i, 0].transAxes,
                    rotation=90,
                    va="center",
                    ha="center",
                    fontsize=source_labelsize,
                    fontweight="bold",
                )
 
            fig.tight_layout()
            return fig, axes
 
        if max_rows is None or max_rows < 1:
            chunks = [sources]
        else:
            chunks = [
                sources[k : k + max_rows] for k in range(0, len(sources), max_rows)
            ]
 
        def _path_for(index):
            if outfile is None:
                return None
            path = Path(outfile)
            if len(chunks) == 1:
                return path
            width = len(str(len(chunks)))
            return path.with_name(f"{path.stem}_part{index + 1:0{width}d}{path.suffix}")
 
        rc = {
            "axes.titlesize": title_fontsize,
            "axes.labelsize": axis_labelsize,
            "xtick.labelsize": tick_labelsize,
            "ytick.labelsize": tick_labelsize,
            "legend.fontsize": legend_fontsize,
            "legend.title_fontsize": legend_fontsize,
        }
 
        results = []
        for index, chunk in enumerate(chunks):
            with plt.rc_context(rc):
                fig, axes = _render(chunk)
            path = _path_for(index)
            if path is not None:
                path.parent.mkdir(parents=True, exist_ok=True)
                fig.savefig(path, dpi=dpi, bbox_inches="tight")
            results.append((fig, axes))
 
        return results

    return (plot_ic_comparison,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Sample
    """)
    return


@app.cell
def _(INFORMATION_CONTENT_SAMPLE_RESULTS, pl):
    ic_analysis_sample = pl.read_parquet(INFORMATION_CONTENT_SAMPLE_RESULTS)
    return (ic_analysis_sample,)


@app.cell
def _(FIGURES_DIR: "Path", ic_analysis_sample, plot_ic_comparison):
    plot_ic_comparison(ic_analysis_sample, outfile=FIGURES_DIR / "ic_original_source_vs_metahq__level-sample.png")
    return


@app.cell
def _(Path, np, plt, sns):
    def plot_ic_boxplots(
        df,
        *,
        source_col="original_source",
        attribute_col="attribute",
        value_cols=("original_ic", "metahq_ic"),
        pipeline_labels=("original", "metahq"),
        attributes=("tissue", "disease"),
        sources=None,
        palette=None,
        showfliers=False,
        box_width=0.55,
        box_gap=0.15,
        box_linewidth=1.0,
        shared_limits=True,
        annotate_counts=True,
        xtick_rotation=35,
        row_spacing=0.12,
        panel_height=3.0,
        width_per_source=1.1,
        min_width=6.5,
        tick_labelsize=13,
        axis_labelsize=15,
        title_fontsize=17,
        legend_fontsize=13,
        legend_row=-1,
        legend_loc="lower left",
        legend_bbox=None,
        legend_ncol=2,
        count_fontsize=10,
        outfile=None,
        dpi=600,
    ):
        """Stacked IC boxplots, one row per attribute.

        Each row is a single axes holding every source side by side, with the two
        pipelines as the hue within each source. Rows share the x grouping, so the
        same source occupies the same horizontal position in every row.

        Parameters
        ----------
        df : polars.DataFrame or pandas.DataFrame
            One row per accession x attribute. Converted to pandas if needed.
        source_col : str
            Column giving the outer x grouping.
        value_cols, pipeline_labels : sequence of str
            The two IC columns to compare, and the names they get in the legend.
        attributes : sequence of str
            Values of `attribute_col` to give a row each, in order.
        sources : sequence of str or None
            Restrict and order the x groups. Defaults to every source, sorted.
        palette : dict or sequence or None
            Passed to `seaborn.boxplot`; keys are `pipeline_labels` if a dict.
        showfliers : bool
            Draw outlier points beyond the whiskers.
        box_width : float
            Fraction of the per-source slot taken by the pair of boxes. Lower
            values make the boxes skinnier and open up white space between
            sources.
        box_gap : float
            Fraction of each box's width shaved off to separate the two pipelines
            within a source. Requires seaborn >= 0.13; pass 0 on older versions.
        box_linewidth : float
            Line width of the box, whisker, and median strokes. Worth dropping
            toward 0.8 when the boxes are very narrow.
        shared_limits : bool
            Use one set of y limits across rows, so tissue and disease are directly
            comparable. Otherwise each row is scaled to its own data.
        annotate_counts : bool
            Print the per-source observation count above each group.
        xtick_rotation : float
            Rotation for the source labels; raise it when names are long. Labels
            are anchored at their right edge so they slant up into the axes.
        row_spacing : float
            Vertical gap between rows as a fraction of the mean row height. Applied
            after `tight_layout`, so it overrides the padding that would otherwise
            be reserved between panels.
        panel_height : float
            Height in inches of a single row.
        width_per_source : float
            Figure width in inches contributed by each source, floored at
            `min_width`.
        tick_labelsize, axis_labelsize, title_fontsize, legend_fontsize : float
            Point sizes for the tick labels, the y axis label, the row titles, and
            the legend entries. Applied through an rc context so seaborn's own
            artists pick them up too.
        count_fontsize : float
            Point size of the `n = ...` annotations.
        legend_row : int
            Index of the row that carries the legend. Defaults to -1, the bottom
            panel. Every other row's legend is removed.
        legend_loc : str
            Corner of that axes the legend anchors to, e.g. "lower left".
        legend_bbox : tuple or None
            Passed to `bbox_to_anchor` for manual placement, in axes coordinates
            where (0, 0) is the bottom left of the panel and (1, 1) the top right.
            `legend_loc` then names which corner of the legend box sits at that
            point, so ("lower left", (0.02, 0.02)) insets it slightly. Values
            outside 0-1 push the legend beyond the axes.
        legend_ncol : int
            Columns in the legend; 2 keeps the two pipelines on one line.
        outfile : str or pathlib.Path or None
            If given, the figure is written here at `dpi`.

        Returns
        -------
        (matplotlib.figure.Figure, numpy.ndarray of Axes)
        """
        pdf = df.to_pandas() if hasattr(df, "to_pandas") else df.copy()
 
        attributes = list(attributes)
        if sources is None:
            sources = sorted(pdf[source_col].unique())
        else:
            sources = list(sources)
 
        pdf = pdf[pdf[source_col].isin(sources) & pdf[attribute_col].isin(attributes)]
 
        long = pdf.melt(
            id_vars=[c for c in pdf.columns if c not in value_cols],
            value_vars=list(value_cols),
            var_name="pipeline",
            value_name="ic",
        )
        long["pipeline"] = long["pipeline"].map(dict(zip(value_cols, pipeline_labels)))
 
        def _limits(values):
            values = np.asarray(values, dtype=float)
            values = values[np.isfinite(values)]
            if not values.size:
                return (0.0, 1.0)
            lo, hi = float(values.min()), float(values.max())
            pad = 0.15 * (hi - lo) or 0.5
            return (lo - pad, hi + pad)
 
        global_lims = _limits(long["ic"]) if shared_limits else None
 
        # `gap` landed in seaborn 0.13; only pass it when asked for, so the
        # function still runs against older versions with box_gap=0.
        box_kwargs = dict(
            showfliers=showfliers,
            width=box_width,
            linewidth=box_linewidth,
        )
        if box_gap:
            box_kwargs["gap"] = box_gap
 
        rc = {
            "axes.titlesize": title_fontsize,
            "axes.labelsize": axis_labelsize,
            "xtick.labelsize": tick_labelsize,
            "ytick.labelsize": tick_labelsize,
            "legend.fontsize": legend_fontsize,
            "legend.title_fontsize": legend_fontsize,
        }
 
        fig_width = max(min_width, width_per_source * max(len(sources), 1) + 2.0)
 
        with plt.rc_context(rc):
            fig, axes = plt.subplots(
                nrows=len(attributes),
                ncols=1,
                figsize=(fig_width, panel_height * len(attributes)),
                sharex=True,
                sharey=shared_limits,
                squeeze=False,
            )
            axes = axes[:, 0]
 
            for i, attr in enumerate(attributes):
                ax = axes[i]
                d = long[long[attribute_col] == attr]
 
                sns.boxplot(
                    data=d,
                    x=source_col,
                    y="ic",
                    hue="pipeline",
                    order=sources,
                    hue_order=list(pipeline_labels),
                    palette=palette,
                    ax=ax,
                    **box_kwargs,
                )
 
                lims = global_lims if shared_limits else _limits(d["ic"])
                ax.set(xlabel="", ylabel="IC", ylim=lims, title=attr)
                ax.grid(axis="y", ls=":", lw=0.6, alpha=0.6)
                ax.set_axisbelow(True)
 
                if annotate_counts:
                    # One count per source: rows are accessions, not melted records.
                    counts = (
                        pdf[pdf[attribute_col] == attr]
                        .groupby(source_col)
                        .size()
                        .reindex(sources, fill_value=0)
                    )
                    top = lims[1] - 0.02 * (lims[1] - lims[0])
                    for k, source in enumerate(sources):
                        ax.text(
                            k,
                            top,
                            f"n={counts[source]:,}",
                            ha="center",
                            va="top",
                            fontsize=count_fontsize,
                            color="0.35",
                        )
 
                # Seaborn adds a legend to every axes; drop them all here and
                # rebuild a single one after the loop.
                legend = ax.get_legend()
                if legend is not None:
                    legend.remove()
 
            handles, labels = axes[0].get_legend_handles_labels()
            if handles:
                axes[legend_row].legend(
                    handles,
                    labels,
                    title="",
                    loc=legend_loc,
                    bbox_to_anchor=legend_bbox,
                    ncol=legend_ncol,
                    fontsize=legend_fontsize,
                    framealpha=0.85,
                )
 
            axes[-1].tick_params(axis="x", labelbottom=True)
            if xtick_rotation:
                plt.setp(
                    axes[-1].get_xticklabels(),
                    rotation=xtick_rotation,
                    ha="right",
                    rotation_mode="anchor",
                )
 
            fig.tight_layout()
            # After tight_layout, so this wins over the padding it reserved.
            fig.subplots_adjust(hspace=row_spacing)
 
        if outfile is not None:
            path = Path(outfile)
            path.parent.mkdir(parents=True, exist_ok=True)
            fig.savefig(path, dpi=dpi, bbox_inches="tight")
        return fig, axes

    return (plot_ic_boxplots,)


@app.cell
def _(FIGURES_DIR: "Path", ic_analysis_sample, plot_ic_boxplots):
    plot_ic_boxplots(
        ic_analysis_sample,
        outfile=FIGURES_DIR / "ic_original_source_vs_metahq__boxes__level-sample.png",
        row_spacing=0.2,
        width_per_source=0.8,
    )
    return


if __name__ == "__main__":
    app.run()
