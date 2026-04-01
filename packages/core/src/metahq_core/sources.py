"""
Reference information for annotation sources in MetaHQ.

To add a new reference, build a new class with a Reference structure
and add it to the REFERENCE_MAP at the bottom of this script.

Author: Parker Hicks
Date: 2026-04-01

Last updated: 2026-04-01 by Parker Hicks
"""

from abc import ABC, abstractmethod


class Reference(ABC):
    """Abstract Reference scaffold"""

    source: str
    citation: str
    doi: str
    url: str
    rights: str
    notes: str | None

    @abstractmethod
    def __init__(self, n: int):
        self.n = n


class Ale(Reference):
    """Reference information for ALE."""

    source: str = "ALE"
    citation: str = """Giles, C. B. et al. ALE: automated label extraction from GEO metadata. BMC
    bioinformatics 18, 509 (2017)."""
    doi: str = "10.1186/s12859-017-1888-1"
    url: str = (
        "https://github.com/wrenlab/label-extraction/blob/master/data/manual/geo_manual_labels_jdw.tsv"
    )
    rights: str = "CC0 1.0"
    notes: str | None = None

    def __init__(self, n: int):
        self.n = n


class Bgee(Reference):
    """Reference information for Bgee."""

    source: str = "BGee"
    citation: str = """Bastian, F. B. et al. The Bgee suite: integrated curated expression atlas
    and comparative transcriptomics in animals. Nucleic acids research 49, D831–D847 (2021)."""
    doi: str = "10.1093/nar/gkaa793"
    url: str = "https://www.bgee.org"
    rights: str = "CC0 1.0"
    notes: str | None = None

    def __init__(self, n: int):
        self.n = n


class CellO(Reference):
    """Reference information for CellO."""

    source: str = "CellO"
    citation: str = """Bernstein, M. N., Ma, Z., Gleicher, M. & Dewey, C. N. CellO: Comprehensive and
    hierarchical cell type classification of human cells with the Cell Ontology. Iscience 24
    (2021)."""
    doi: str = "10.1016/j.isci.2020.101913"
    url: str = "https://zenodo.org/records/4609473"
    rights: str = "CC BY 4.0"
    notes: str | None = None

    def __init__(self, n: int):
        self.n = n


class Creeds(Reference):
    """Reference information for CREEDS."""

    source: str = "CREEDS"
    citation: str = """Wang, Z. et al. Extraction and analysis of signatures from the Gene Expression
    Omnibus by the crowd. Nature communications 7, 12846 (2016)."""
    doi: str = "10.1038/ncomms12846"
    url: str = "https://maayanlab.cloud/CREEDS/"
    rights: str = "CC BY 4.0"
    notes: str | None = None

    def __init__(self, n: int):
        self.n = n


class DiSignAtlas(Reference):
    """Reference information for DiSignAtlas."""

    source: str = "DiSignAtlas"
    citation: str = """Zhai, Z. et al. DiSignAtlas: an atlas of human and mouse disease signatures
    based on bulk and single-cell transcriptomics. Nucleic Acids Research 52, D1236–D1245 (2024)."""
    doi: str = "10.1093/nar/gkad961"
    url: str = "http://www.inbirg.com/disignatlas/"
    rights: str = """DiSignAtlas is free only for academic usage. For commercial usage,
    please contact Prof. Jianbo Pan at panjianbo@cqmu.edu.cn"""
    notes: str | None = "CC BY-NC implied"

    def __init__(self, n: int):
        self.n = n


class Gemma(Reference):
    """Reference information for Gemma."""

    source: str = "Gemma"
    citation: str = """Lim, N. et al. Curation of over 10 000 transcriptomic studies to
    enable data reuse. Database 2021, baab006 (2021)."""
    doi: str = "10.1093/database/baab006"
    url: str = "https://gemma.msl.ubc.ca/home.html"
    rights: str = "CC BY-NC 4.0"
    notes: str | None = None

    def __init__(self, n: int):
        self.n = n


class Golightly(Reference):
    """Reference information for Golightly_2018."""

    source: str = "Golightly_2018"
    citation: str = """Golightly, N. P., Bell, A., Bischoff, A. I., Hollingsworth, P. D. & Piccolo, S. R.
    Curated compendium of human transcriptional biomarker data. Scientific data 5, 1–8 (2018)."""
    doi: str = "10.1038/sdata.2018.66"
    url: str = "https://osf.io/ssk3t/overview"
    rights: str = "CC0 1.0"
    notes: str | None = None

    def __init__(self, n: int):
        self.n = n


class Gu(Reference):
    """Reference information for Gu_2023."""

    source: str = "Gu_2023"
    citation: str = """Gu, J., Dai, J., Lu, H. & Zhao, H. Comprehensive analysis of ubiquitously
    expressed genes in humans from a data-driven perspective. Genomics, Proteomics & Bioinformatics
    21, 164–176 (2023)."""
    doi: str = "10.1016/j.gpb.2021.08.017"
    url: str = "https://academic.oup.com/gpb/article/21/1/164/7274179"
    rights: str = "CC0 1.0"
    notes: str | None = "Table S3"

    def __init__(self, n: int):
        self.n = n


class Johnson(Reference):
    """Reference information for Johnson_2023."""

    source: str = "Johnson_2023"
    citation: str = """ Johnson, K. A. & Krishnan, A. Human pan-body age-and sex-specific molecular
    phenomena inferred from public transcriptome data using machine learning. bioRxiv,
    2023–01 (2023)."""
    doi: str = "10.1101/2023.01.12.523796"
    url: str = (
        "https://github.com/krishnanlab/Age-sex_signatures_in_humans_code/tree/master/data/labels/full"
    )
    rights: str = "CC0 1.0"
    notes: str | None = None

    def __init__(self, n: int):
        self.n = n


# TODO: Add url
class Sirota(Reference):
    """Reference information for Sirota_2011."""

    source: str = "Sirota_2011"
    citation: str = """Sirota, M. et al. Discovery and preclinical validation of drug indications using
    compendia of public gene expression data. Science translational medicine 3, 96ra77–96ra77
    (2011)."""
    doi: str = "10.1126/scitranslmed.3001318"
    url: str = ""
    rights: str = "CC BY-NC"
    notes: str | None = None

    def __init__(self, n: int):
        self.n = n


# TODO: Check url
class Ursa(Reference):
    """Reference information for URSA."""

    source: str = "URSA"
    citation: str = """Lee, Y., Krishnan, A., Zhu, Q. & Troyanskaya, O. G. Ontology-aware classification
    of tissue and cell-type signals in gene expression profiles across platforms and technologies.
    Bioinformatics 29, 3036–3044 (2013)."""
    doi: str = "10.1093/bioinformatics/btt529"
    url: str = "ursa.princeton.edu"
    rights: str = "CC BY-NC 3.0"
    notes: str | None = None

    def __init__(self, n: int):
        self.n = n


# TODO: Check url
class UrsaHD(Reference):
    """Reference information for URSA_HD."""

    source: str = "URSA_HD"
    citation: str = """Lee, Y. et al. A computational framework for genome-wide characterization of the
    human disease landscape. Cell systems 8, 152–162 (2019)."""
    doi: str = "10.1016/j.cels.2018.12.010"
    url: str = "ursahd.princeton.edu"
    rights: str = "CC BY-NC 3.0"
    notes: str | None = None

    def __init__(self, n: int):
        self.n = n


class KrishnanLab(Reference):
    """Reference information for KrishnanLab."""

    source: str = "KrishnanLab"
    citation: str = """Hicks, P. et al. MetaHQ: Harmonized, high-quality metadata annotations of
    public omics samples and studies. arXiv, (2026)."""
    doi: str = "10.48550/arXiv.2602.07805"
    url: str = "https://doi.org/10.5281/zenodo.17663086"
    rights: str = "CC BY-NC 4.0"
    notes: str | None = None

    def __init__(self, n: int):
        self.n = n


REFERENCE_MAP = {
    "ALE": Ale,
    "BGee": Bgee,
    "CellO": CellO,
    "CREEDS": Creeds,
    "DiSignAtlas": DiSignAtlas,
    "Gemma": Gemma,
    "Golightly_2018": Golightly,
    "Gu_2023": Gu,
    "KrishnanLab": KrishnanLab,
    "Johnson_2023": Johnson,
    "Sirota_2011": Sirota,
    "URSA": Ursa,
    "URSA_HD": UrsaHD,
}
