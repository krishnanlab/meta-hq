import marimo

__generated_with = "0.23.3"
app = marimo.App(width="medium")


@app.cell
def _():
    import re
    from pathlib import Path
    from typing import Literal

    import marimo as mo
    import matplotlib.pyplot as plt
    import numpy as np
    import polars as pl
    import seaborn as sns

    return Literal, Path, mo, np, pl, plt, re, sns


@app.cell
def _(Path):
    RESULTS = Path("results")
    overlap_results = list(RESULTS.glob("overlap*"))
    return (RESULTS,)


@app.cell
def _(re):
    def match_pattern(text: str, pattern: str) -> str:
        match = re.search(pattern, text)
        if match:
            return match.group(1)
        else:
            return ""

    return (match_pattern,)


@app.cell
def _(Literal, Path, RESULTS, match_pattern, pl):
    def get_overlap_results(
        dir: Path,
        overlap_type: str,
        level: Literal["sample", "series"],
        attribute_pattern: str = r"attribute-(tissue|disease|sex|age)",
        level_pattern: str = r"level-(sample|series)",
        separator="\t",
    ) -> dict[str, pl.DataFrame]:
        results: dict[str, pl.DataFrame] = {}

        files = list(RESULTS.glob(f"{overlap_type}*"))

        for file in files:

            file_level = match_pattern(file.stem, level_pattern)
            if file_level != level:
                continue

            attribute = match_pattern(file.stem, attribute_pattern)
            results[attribute] = pl.read_csv(file, separator=separator)

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
def _(np, pl, plt, sns):
    def plot_heatmap(
        overlap_results: dict[str, pl.DataFrame],
        subplot_shape: tuple[int, int] = (2, 2),
        figsize_per_plot: tuple[int, int] = (5, 5),
        order: list[str] | None = None,
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
            axes_flat = [axes]  # single subplot case

        if isinstance(order, list):
            overlap_results = {k: overlap_results[k] for k in order}

        for ax, (group_name, df) in zip(axes_flat, overlap_results.items()):
            df = (
                df.with_columns(pl.Series("source", df.columns))
                .to_pandas()
                .set_index("source", drop=True)
            )

            sns.heatmap(df, ax=ax, **heatmap_kwargs)
            ax.set_title(group_name.capitalize())

            xticks = ax.get_xticklabels()
            ax.set_xticklabels(labels=xticks, rotation=45, ha='right', rotation_mode='anchor')

        for ax in axes_flat[len(overlap_results):]:
            ax.axis("off")

        plt.tight_layout()
        plt.show()

    return (plot_heatmap,)


@app.cell
def _():
    ATTRIBUTE_ORDER = ["tissue", "disease", "sex", "age"]
    CMAP = "Blues"
    return ATTRIBUTE_ORDER, CMAP


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    # Sample overlap across sources
    """)
    return


@app.cell
def _(ATTRIBUTE_ORDER, CMAP, RESULTS, get_overlap_results, plot_heatmap):
    sample_overlap_count = get_overlap_results(RESULTS, overlap_type="overlap_count", level="sample")
    plot_heatmap(sample_overlap_count, order=ATTRIBUTE_ORDER, cmap=CMAP)
    return


@app.cell
def _(ATTRIBUTE_ORDER, CMAP, RESULTS, get_overlap_results, plot_heatmap):
    sample_overlap_percent = get_overlap_results(RESULTS, overlap_type="overlap_percent", level="sample")
    plot_heatmap(sample_overlap_percent, order=ATTRIBUTE_ORDER, cmap=CMAP)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    # Series overlap across sources
    """)
    return


@app.cell
def _(ATTRIBUTE_ORDER, CMAP, RESULTS, get_overlap_results, plot_heatmap):
    series_overlap_count = get_overlap_results(RESULTS, overlap_type="overlap_count", level="series")
    plot_heatmap(series_overlap_count, order=ATTRIBUTE_ORDER, cmap=CMAP)
    return


@app.cell
def _(ATTRIBUTE_ORDER, CMAP, RESULTS, get_overlap_results, plot_heatmap):
    series_overlap_percent = get_overlap_results(RESULTS, overlap_type="overlap_percent", level="series")
    plot_heatmap(series_overlap_percent, order=ATTRIBUTE_ORDER, cmap=CMAP)
    return


if __name__ == "__main__":
    app.run()
