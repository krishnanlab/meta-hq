from collections import defaultdict
from copy import deepcopy
from pathlib import Path
from typing import Literal, TypeAlias

from tqdm import tqdm

from metahq_setup.config import (
    CONTROL_ID,
    DELIMITER,
    ECODE_KEY,
    ID_KEY,
    MONDO_RELATIONS,
    MONDO_SYSTEMS,
    NON_ONTOLOGY_BASED_KEYS,
    ONTOLOGY_BASED_KEYS,
    UBERON_RELATIONS,
    UBERON_SYSTEMS,
)
from metahq_setup.ontology import RelationsLazyFrame
from metahq_setup.util.logging import setup_logger

OntologyName: TypeAlias = Literal["uberon", "mondo"]


def merge_set_values(dict_: dict[str, set[str]]):
    """Combine all lists that are values of a dictionary to a single list."""
    merged = set()
    for value in dict_.values():
        merged.update(value)
    return merged


class OntologySystems:
    """Load, store, and manipulate sets of ontology system terms.

    Attributes:
        mondo (set[str]):
            A set of MONDO system terms.
        uberon (set[str]):
            A set of UBERON-CL system terms.
    """

    def __init__(self):
        self.mondo: set[str] = set()
        self.uberon: set[str] = set()

    def add_from_txt(
        self, file: Path, ontology: OntologyName, encoding="utf-8"
    ) -> None:
        """Populate the system set for a given ontology using terms in a plain text file."""
        with open(file, "r", encoding=encoding) as f:
            systems: set[str] = {line.strip() for line in f.readlines()}

        if ontology == "uberon":
            self.uberon.update(systems)

        if ontology == "mondo":
            self.mondo.update(systems)

    @property
    def all_systems(self) -> set[str]:
        """Return the union of all systems."""
        return self.uberon | self.mondo


class TermFilterer:
    """Filter annotations for specific terms in an ontology.

    Attributes:
        anno (dict):
            Dictionary of MetaHQ annotations in the proper format.

        uberon: OntologyRelations
            Stores of term: descendant relationships

        mondo: OntologyRelations
            Stores of term: descendant relationships
    """

    def __init__(
        self,
        db,
        uberon_relations: Path = UBERON_RELATIONS,
        mondo_relations: Path = MONDO_RELATIONS,
        uberon_systems: Path = UBERON_SYSTEMS,
        mondo_systems: Path = MONDO_SYSTEMS,
    ):
        self.db: dict[str, dict] = db
        self.uberon = RelationsLazyFrame.from_parquet(uberon_relations)
        self.mondo = RelationsLazyFrame.from_parquet(mondo_relations)

        self.systems: OntologySystems = self._load_systems(
            {"uberon": uberon_systems, "mondo": mondo_systems}
        )
        self.general_annotations = set()
        self.logger = setup_logger("metahq_setup.combiners._term_filterer.TermFilterer")

    def get_specific_annotations(self) -> dict[str, dict]:
        """Remove high-level, unspecific annotations.

        These negatively affect downstream applications
        such as generating labels.
        """
        descendants = {
            "tissue": {
                "all": self.uberon.get_descendants(),
                "systems": merge_set_values(
                    self.uberon.get_descendants(subset=list(self.systems.uberon))
                ),
            },
            "disease": {
                "all": self.mondo.get_descendants(),
                "systems": merge_set_values(
                    self.mondo.get_descendants(subset=list(self.systems.mondo))
                ),
            },
        }

        specific = defaultdict(dict)
        for id_, anno in tqdm(
            self.db.items(),
            total=len(self.db),
            desc="Collecting specific annotations...",
        ):
            for attribute, entry in anno.items():

                # skip these they are not from an ontology
                if attribute in NON_ONTOLOGY_BASED_KEYS:
                    specific[id_][attribute] = entry
                    continue

                if attribute in ONTOLOGY_BASED_KEYS:
                    att_anno = self.collect_entry_onto_annotations(
                        entry,
                        descendants[attribute]["all"],
                        descendants[attribute]["systems"],
                    )
                    if len(att_anno) > 0:
                        specific[id_][attribute] = att_anno

        return dict(specific)

    def collect_entry_onto_annotations(
        self,
        entry_anno: dict[str, dict],
        descendants: dict[str, set[str]],
        system_descendants: set[str],
    ) -> dict[str, dict]:
        """Remove any annotations above the system level for a particular entry in the database."""
        all_annotations = set()
        for source in entry_anno.values():
            if ID_KEY in source:
                source_annos = set(source[ID_KEY].split(DELIMITER))

                # subset for system descendants or controls
                subset = set()
                for term in source_annos:
                    if term in self.systems.all_systems:
                        self.general_annotations.add(term)
                    elif (term in system_descendants) or (term == CONTROL_ID):
                        subset.add(term)
                    else:
                        self.general_annotations.add(term)

                all_annotations.update(subset)

        specific = self._get_specific_entry_annotations(all_annotations, descendants)

        new = defaultdict(dict)
        for source_name, entry in entry_anno.items():
            if ID_KEY not in entry:
                continue
            source_ids = [
                term for term in entry[ID_KEY].split(DELIMITER) if term in specific
            ]

            if len(source_ids) < 1:
                continue

            new[source_name][ID_KEY] = DELIMITER.join(source_ids)
            new[source_name][ECODE_KEY] = entry[ECODE_KEY]

        return dict(new)

    def _get_specific_entry_annotations(
        self, anno: set[str], descendants: dict[str, set[str]], verbose: bool = False
    ) -> str:
        """Get most specific annotations for an entry in MetaHQ.

        Arguments:
            anno (set[str]):
                Unique set of term annotations for a single entry.

            descendants (dict[str, set[str]]):
                Term: descendants relationships for all terms.

        Returns:
            (str): The most specific term annotations for the entry.
        """
        specific = deepcopy(anno)

        # if a descendant of a term is also annotated to gene
        # discard term annotation
        for term in anno:
            if term == CONTROL_ID:
                specific.add(term)
                continue
            desc = descendants.get(term)

            if not desc:
                if verbose:
                    self.logger.info("%s is not under system descendants.", term)
                continue

            if desc & anno:
                specific.discard(term)

        return DELIMITER.join(specific)

    @staticmethod
    def _load_systems(
        files: dict[OntologyName, Path], encoding: str = "utf-8"
    ) -> OntologySystems:
        systems = OntologySystems()
        for ontology, file in files.items():
            systems.add_from_txt(file, ontology=ontology, encoding=encoding)

        return systems
