"""
Create a file including citations for each source in a MetaHQ query result.
A new Reference must be created for every new annotation set added to MetaHQ.

Author: Parker Hicks
Date: 2026-03-31

Last updated: 2026-03-31 by Parker Hicks
"""

import textwrap
from abc import ABC
from dataclasses import dataclass


@dataclass
class Reference(ABC):
    """Abstract Reference scaffold"""

    source: str
    citation: str
    doi: str
    url: str
    rights: str
    notes: str | None


@dataclass
class Ale(Reference):
    """Reference information for ALE."""

    source = "ALE"
    citation = """Giles, C. B. et al. ALE: automated label extraction from GEO metadata. BMC
    bioinformatics 18, 509 (2017)."""
    doi = "10.1186/s12859-017-1888-1"
    url = "https://github.com/wrenlab/label-extraction/blob/master/data/manual/geo_manual_labels_jdw.tsv"
    rights = "CC0 1.0"
    notes = None


@dataclass
class Bgee(Reference):
    """Reference information for Bgee."""

    source = "BGEE"
    citation = """Bastian, F. B. et al. The Bgee suite: integrated curated expression atlas
    and comparative transcriptomics in animals. Nucleic acids research 49, D831–D847 (2021)."""
    doi = "10.1093/nar/gkaa793"
    url = "https://www.bgee.org"
    rights = "CC0 1.0"
    notes = None


@dataclass
class CellO(Reference):
    """Reference information for CellO."""

    source = "CellO"
    citation = """Bernstein, M. N., Ma, Z., Gleicher, M. & Dewey, C. N. CellO: Comprehensive and
    hierarchical cell type classification of human cells with the Cell Ontology. Iscience 24
    (2021)."""
    doi = "10.1016/j.isci.2020.101913"
    url = "https://zenodo.org/records/4609473"
    rights = "CC BY 4.0"
    notes = None


@dataclass
class Creeds(Reference):
    """Reference information for CREEDS."""

    source = "CREEDS"
    citation = """Wang, Z. et al. Extraction and analysis of signatures from the Gene Expression
    Omnibus by the crowd. Nature communications 7, 12846 (2016)."""
    doi = "10.1038/ncomms12846"
    url = "https://maayanlab.cloud/CREEDS/"
    rights = "CC BY 4.0"
    notes = None


@dataclass
class DiSignAtlas(Reference):
    """Reference information for DiSignAtlas."""

    source = "DiSignAtlas"
    citation = """Zhai, Z. et al. DiSignAtlas: an atlas of human and mouse disease signatures
    based on bulk and single-cell transcriptomics. Nucleic Acids Research 52, D1236–D1245 (2024)."""
    doi = "10.1093/nar/gkad961"
    url = "http://www.inbirg.com/disignatlas/"
    rights = """DiSignAtlas is free only for academic usage. For commercial usage, please contact
    Prof. Jianbo Pan at panjianbo@cqmu.edu.cn"""
    notes = None


@dataclass
class Gemma(Reference):
    """Reference information for Gemma."""

    source = "Gemma"
    citation = """Lim, N. et al. Curation of over 10 000 transcriptomic studies to
    enable data reuse. Database 2021, baab006 (2021)."""
    doi = "10.1093/database/baab006"
    url = "https://gemma.msl.ubc.ca/home.html"
    rights = "CC BY-NC 4.0"
    notes = None


@dataclass
class Golightly(Reference):
    """Reference information for Golightly_2018."""

    source = "Golightly_2018"
    citation = """Golightly, N. P., Bell, A., Bischoff, A. I., Hollingsworth, P. D. & Piccolo, S. R.
    Curated compendium of human transcriptional biomarker data. Scientific data 5, 1–8 (2018)."""
    doi = "10.1038/sdata.2018.66"
    url = "https://osf.io/ssk3t/overview"
    rights = "CC0 1.0"
    notes = None


@dataclass
class Gu(Reference):
    """Reference information for Gu_2023."""

    source = "Gu_2023"
    citation = """Gu, J., Dai, J., Lu, H. & Zhao, H. Comprehensive analysis of ubiquitously
    expressed genes in humans from a data-driven perspective. Genomics, Proteomics & Bioinformatics
    21, 164–176 (2023)."""
    doi = "10.1016/j.gpb.2021.08.017"
    url = "https://academic.oup.com/gpb/article/21/1/164/7274179"
    rights = ""
    notes = "Table S3"


@dataclass
class Johnson(Reference):
    """Reference information for Johnson_2023."""

    source = "Johnson_2023"
    citation = """ Johnson, K. A. & Krishnan, A. Human pan-body age-and sex-specific molecular
    phenomena inferred from public transcriptome data using machine learning. bioRxiv,
    2023–01 (2023)."""
    doi = "10.1101/2023.01.12.523796"
    url = "https://github.com/krishnanlab/Age-sex_signatures_in_humans_code/tree/master/data/labels/full"
    rights = "CC0 1.0"
    notes = None


# TODO: Add url
@dataclass
class Sirota(Reference):
    """Reference information for Sirota_2011."""

    source = "Sirota_2011"
    citation = """Sirota, M. et al. Discovery and preclinical validation of drug indications using
    compendia of public gene expression data. Science translational medicine 3, 96ra77–96ra77
    (2011)."""
    doi = "10.1126/scitranslmed.3001318"
    url = ""
    rights = "CC BY-NC"
    notes = None


# TODO: Check url
@dataclass
class Ursa(Reference):
    """Reference information for URSA."""

    source = "URSA"
    citation = """Lee, Y., Krishnan, A., Zhu, Q. & Troyanskaya, O. G. Ontology-aware classification
    of tissue and cell-type signals in gene expression profiles across platforms and technologies.
    Bioinformatics 29, 3036–3044 (2013)."""
    doi = "10.1093/bioinformatics/btt529"
    url = "ursa.princeton.edu"
    rights = "CC BY-NC 3.0"
    notes = None


# TODO: Check url
@dataclass
class UrsaHD(Reference):
    """Reference information for URSA_HD."""

    source = "URSA_HD"
    citation = """Lee, Y. et al. A computational framework for genome-wide characterization of the
    human disease landscape. Cell systems 8, 152–162 (2019)."""
    doi = "10.1016/j.cels.2018.12.010"
    url = "ursahd.princeton.edu"
    rights = "CC BY-NC 3.0"
    notes = None


def format_reference(
    reference: Reference, index: int, annotation_count: int, indent: str = "    "
) -> str:
    """Format a reference for export to CITATION.txt."""
    citation_text = " ".join(reference.citation.split())
    formatted = (
        f"[{index}] {reference.source}\n"
        f"{indent}{textwrap.fill(citation_text, width=80, subsequent_indent=indent)}\n"
        f"{indent}License: {reference.rights}\n"
        f"{indent}Annotations: {annotation_count}"
    )

    if isinstance(reference.notes, str):
        formatted += f"\n{reference.notes}"

    return formatted
