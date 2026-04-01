"""
Create a file including citations for each source in a MetaHQ query result.
A new Reference must be created for every new annotation set added to MetaHQ.

Author: Parker Hicks
Date: 2026-03-31

Last updated: 2026-04-01 by Parker Hicks
"""

import textwrap
from dataclasses import dataclass
from pathlib import Path
from string import Template

import polars as pl

from metahq_core.sources import REFERENCE_MAP, Reference
from metahq_core.util.io import save_plain_text
from metahq_core.util.supported import CITATION_TEMPLATE


@dataclass
class CitationConfig:
    """Storage for attributes required to property format a reference.

    Attributes:
        version (str):
            Version of the MetaHQ database.
        attribute (str):
            Attribute of the queried database entries.
        level (str):
            Curation level.
        species (str):
            Species of the quereied database entries.
        ecode (str):
            Evidence code of the quereied database entries.
        tech (str):
            Technology of the quereied database entries.
        mode (str):
            Query mode (e.g, annotate, label).
        date (str):
            Date formatted as 'YYYY-MM-DD HR:MIN:SEC'.
        outfile (str | Path):
            Outfile to save the reference to.

    """

    version: str
    attribute: str
    level: str
    species: str
    ecode: str
    tech: str
    mode: str
    date: str
    outfile: str | Path = "CITATION.txt"


def build_citation_file(
    references: str,
    config: CitationConfig,
    indent: str = "",
):
    """Build the final citation file substituting placeholder variables in the citation template."""
    metahq_reference = textwrap.fill(
        format_citation(REFERENCE_MAP["KrishnanLab"](0)),
        width=80,
        subsequent_indent=indent,
    )

    with open(CITATION_TEMPLATE, "r", encoding="utf-8") as f:
        template = f.read()

    return Template(template).safe_substitute(
        references=references,
        version=config.version,
        attribute=config.attribute,
        level=config.level,
        species=config.species,
        ecode=config.ecode,
        tech=config.tech,
        mode=config.mode,
        date=config.date,
        metahq_reference=metahq_reference,
    )


def build_reference_list(value_counts: pl.DataFrame) -> list[Reference]:
    """Build a list of initialized references from a polars.DataFrame of counts for each source."""
    refs = []
    for row in value_counts.iter_rows():
        source = row[0]
        count = row[1]

        refs.append(REFERENCE_MAP[source](count))

    return refs


def format_citation(reference: Reference) -> str:
    """Within this script, citations are written with newline characters to maintain
    readibility. This function removes the newline characters and reformats as a single
    stipped line.

    Arguments:
        reference (Reference):
            A populated Reference object.
    """
    return " ".join(reference.citation.split())


def format_reference(reference: Reference, index: int, indent: str = "    ") -> str:
    """Format a reference for export to CITATION.txt.

    Arguments:
        reference (Reference):
            A populated Reference object.
        index (int):
            Index of the reference in a list. Used for ordered display of references.
        indent (str):
            Indentation for formatting.

    Returns:
        (str): A formatted reference.

    Examples:

    >>> from metahq_core.sources import KrishnanLab
    >>> from metahq_core.export.references import format_reference
    >>> format_reference(KrishnanLab(5), 1)
    [1] KrishnanLab
        Hicks, P. et al. MetaHQ: Harmonized, high-quality metadata annotations of public
        omics samples and studies. arXiv, (2026).
        url: https://doi.org/10.5281/zenodo.17663086
        Annotations: 5
        License: CC BY-NC 4.0
    """
    citation_text = format_citation(reference)
    formatted = (
        f"[{index}] {reference.source}\n"
        f"{indent}{textwrap.fill(citation_text, width=80, subsequent_indent=indent)}\n"
        f"{indent}url: {reference.url}\n"
        f"{indent}Annotations: {reference.n}\n"
        f"{indent}License: {reference.rights}"
    )

    if isinstance(reference.notes, str):
        formatted += f"\n{indent}Notes: {reference.notes}"

    return formatted


def format_references(references: list[Reference]) -> str:
    """
    Format a list of (reference, annotation_count) tuples.

    Arguments:
        references (list[tuple[Reference, int]]): List of tuples
            containing (Reference, annotation_count).

    Returns:
        (str): Formatted string with all references numbered sequentially.
    """
    return "\n\n".join(
        format_reference(ref, index=i + 1) for i, ref in enumerate(references)
    )


def save_citations(source_counts: pl.DataFrame, config: CitationConfig):
    """Build and save citations from a polars.DataFrame of source counts."""
    refs = build_reference_list(source_counts)
    refs = format_references(refs)

    text = build_citation_file(refs, config)

    save_plain_text(text, config.outfile)
