"""
Structured objects for ontology entries from the OBO format.
"""

import re
import warnings
from dataclasses import dataclass, field


@dataclass(slots=True)
class Synonym:
    """Ontology term synonyms."""

    name: str
    scope: str
    sources: list[str]


@dataclass(slots=True)
class XRef:
    """Ontology term cross-references"""

    ref_id: str
    sources: list[str]


@dataclass(slots=True)
class GraphConnections:
    """Ontology is_a / part_of relationships."""

    is_a: list[str]
    part_of: list[str]


@dataclass(slots=True)
class OboEntry:
    """OBO ontology term entry."""

    id: str
    name: str
    definition: str | None = None
    def_sources: list[str] = field(default_factory=list)
    synonyms: list[Synonym] = field(default_factory=list)
    xrefs: list[XRef] = field(default_factory=list)
    is_a: list[str] = field(default_factory=list)
    part_of: list[str] = field(default_factory=list)
    property_values: dict[str, list[str]] = field(default_factory=dict)

    @classmethod
    def from_text(cls, text: str) -> "OboEntry":
        id_ = name = definition = None
        def_sources = []
        synonyms = []
        xrefs = []
        is_a = []
        part_of = []
        property_values = {}

        for line in text.strip().splitlines():
            line = line.strip()

            # Skip header and blank lines
            if not line or line == "[Term]":
                continue

            key, _, value = line.partition(": ")

            if key == "id":
                id_ = value

            elif key == "name":
                name = value

            elif key == "def":
                # def: "Some text." [SRC1, SRC2]
                m = re.match(r'"(.+?)"\s*\[([^\]]*)\]', value)
                if m:
                    definition = m.group(1)
                    def_sources = _parse_source_list(m.group(2))

            elif key == "synonym":
                # synonym: "name" SCOPE [SRC1, SRC2]
                m = re.match(r'"(.+?)"\s+(\w+)\s*\[([^\]]*)\]', value)
                if m:
                    synonyms.append(
                        Synonym(
                            name=m.group(1),
                            scope=m.group(2),
                            sources=_parse_source_list(m.group(3)),
                        )
                    )

            elif key == "xref":
                # xref: REF_ID {source="X", source="Y"}  (annotations optional)
                m = re.match(r"(\S+)(?:\s*\{([^}]*)\})?", value)
                if m:
                    ref_id = m.group(1)
                    annotations: list[str] = []
                    if m.group(2):
                        for pair in m.group(2).split(","):
                            _, _, v = pair.strip().partition("=")
                            annotations.append(v.strip().strip('"'))
                    xrefs.append(XRef(ref_id=ref_id, sources=annotations))

            elif key == "property_value":
                # property_value: key value
                pkey, _, pvalue = value.partition(" ")
                property_values.setdefault(pkey, []).append(pvalue)

            elif key == "is_a":
                m = re.match(r"(\S+)(?:\s*\{([^}]*)\})?", value)
                if m and not _is_gci_axiom(m.group(2)):
                    is_a.append(m.group(1))

            elif key == "relationship":
                # relationship: part_of ID ! comment  (other relation types, e.g.
                # develops_from or mutually_spatially_disjoint_with, are not
                # hierarchical and are recorded on both terms they relate, so
                # capturing them here would introduce cycles into the graph)
                reltype, _, target = value.partition(" ")
                if reltype == "part_of":
                    m = re.match(r"(\S+)(?:\s*\{([^}]*)\})?", target)
                    if m and not _is_gci_axiom(m.group(2)):
                        part_of.append(m.group(1))

        if not id_ or not name:
            warnings.warn("Initializing OboEntry without a name or id.", RuntimeWarning)
            return cls(
                id="",
                name="",
                definition=definition,
                def_sources=def_sources,
                synonyms=synonyms,
                xrefs=xrefs,
                is_a=is_a,
                part_of=part_of,
                property_values=property_values,
            )

        return cls(
            id=id_,
            name=name,
            definition=definition,
            def_sources=def_sources,
            synonyms=synonyms,
            xrefs=xrefs,
            is_a=is_a,
            part_of=part_of,
            property_values=property_values,
        )

    @property
    def id_prefix(self) -> str:
        """Return the ontology prefix of a full ontology ID."""
        return self.id.split(":")[0]


def _is_gci_axiom(annotation: str | None) -> bool:
    """Check whether an is_a/relationship trailing annotation marks a GCI axiom.

    A `{gci_relation=..., gci_filler=...}` annotation expresses a conditional class
    axiom (e.g. "X, when part_of Y, is_a Z"), not an unconditional hierarchy edge,
    so it must be excluded from is_a/part_of or Graph.construct() forms a cycle.
    """
    return annotation is not None and "gci_relation" in annotation


def _parse_source_list(raw: str) -> list[str]:
    """Parse a comma-separated source list, returning [] for empty strings."""
    return [s.strip() for s in raw.split(",") if s.strip()]
