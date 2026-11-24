"""
Generate the SVG figures used in the README and documentation.

Standard library only, so it runs anywhere:

    python scripts/docs_figures.py            # writes docs/assets/*.svg

Figures
-------
graphic_abstract.svg   sources -> harmonization -> data package -> CLI -> outputs
data_model.svg         the files in a MetaHQ data package and how they join
label_semantics.svg    how annotate and label modes treat ontology relatives

Edit the content here and re-run; do not edit the SVGs by hand.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from html import escape
from pathlib import Path

OUT = Path(__file__).resolve().parents[1] / "docs" / "assets"

FONT = "Helvetica, Arial, sans-serif"
MONO = "Menlo, Consolas, monospace"

# palette: one color per role, used consistently across figures
INK = "#1f2933"
MUTED = "#52606d"
LINE = "#9aa5b1"
SOURCE = ("#e3f0fb", "#2f6fa8")  # (fill, stroke) community sources
PROCESS = ("#fdf0dc", "#b7791f")  # harmonization / build steps
STORE = ("#eceff3", "#52606d")  # files in the data package
USER = ("#e3f5ea", "#2f855a")  # what users run and get back
POS = "#2f855a"
UNK = "#b7791f"
NEG = "#c53030"
CTRL = "#6b46c1"


@dataclass
class Svg:
    width: int
    height: int
    title: str
    parts: list[str] = field(default_factory=list)

    def add(self, s: str) -> None:
        self.parts.append(s)

    def rect(self, x, y, w, h, fill, stroke, rx=8, sw=1.5, dash=None):
        d = f' stroke-dasharray="{dash}"' if dash else ""
        self.add(
            f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="{rx}" '
            f'fill="{fill}" stroke="{stroke}" stroke-width="{sw}"{d}/>'
        )

    def text(
        self,
        x,
        y,
        s,
        size=13,
        weight="normal",
        color=INK,
        anchor="start",
        family=FONT,
        italic=False,
    ):
        style = ' font-style="italic"' if italic else ""
        self.add(
            f'<text x="{x}" y="{y}" font-family="{family}" font-size="{size}" '
            f'font-weight="{weight}" fill="{color}" text-anchor="{anchor}"{style} '
            'xml:space="preserve">'
            f"{escape(s)}</text>"
        )

    def lines(self, x, y, rows, size=12, gap=None, **kw):
        gap = gap or size + 5
        for i, row in enumerate(rows):
            self.text(x, y + i * gap, row, size=size, **kw)

    def arrow(self, x1, y1, x2, y2, color=LINE, sw=2, dash=None):
        d = f' stroke-dasharray="{dash}"' if dash else ""
        self.add(
            f'<line x1="{x1}" y1="{y1}" x2="{x2}" y2="{y2}" stroke="{color}" '
            f'stroke-width="{sw}" marker-end="url(#arrow)"{d}/>'
        )

    def path(self, d, color=LINE, sw=2, dash=None, arrow=True):
        da = f' stroke-dasharray="{dash}"' if dash else ""
        m = ' marker-end="url(#arrow)"' if arrow else ""
        self.add(
            f'<path d="{d}" fill="none" stroke="{color}" stroke-width="{sw}"{da}{m}/>'
        )

    def circle(self, cx, cy, r, fill, stroke, sw=1.5):
        self.add(
            f'<circle cx="{cx}" cy="{cy}" r="{r}" fill="{fill}" stroke="{stroke}" '
            f'stroke-width="{sw}"/>'
        )

    def legend(self, x, y, items, title="Legend"):
        """items: list of (fill, stroke, label). Draws a boxed legend."""
        w = 24 + max(len(label) for _, _, label in items) * 6.8 + 40
        h = 34 + len(items) * 22
        self.rect(x, y, w, h, "#ffffff", LINE, rx=6, sw=1)
        self.text(x + 12, y + 20, title, size=12, weight="bold")
        for i, (fill, stroke, label) in enumerate(items):
            yy = y + 36 + i * 22
            self.rect(x + 12, yy - 11, 22, 14, fill, stroke, rx=3, sw=1.5)
            self.text(x + 42, yy, label, size=12, color=MUTED)

    def render(self) -> str:
        head = (
            f'<svg xmlns="http://www.w3.org/2000/svg" width="{self.width}" '
            f'height="{self.height}" viewBox="0 0 {self.width} {self.height}" '
            f'role="img" aria-label="{escape(self.title)}">\n'
            f"<title>{escape(self.title)}</title>\n"
            '<defs><marker id="arrow" viewBox="0 0 10 10" refX="9" refY="5" '
            'markerWidth="7" markerHeight="7" orient="auto-start-reverse">'
            f'<path d="M 0 0 L 10 5 L 0 10 z" fill="{LINE}"/></marker></defs>\n'
            f'<rect width="100%" height="100%" fill="#ffffff"/>\n'
        )
        return head + "\n".join(self.parts) + "\n</svg>\n"


def card(svg: Svg, x, y, w, h, colors, title, rows, size=12, mono=False):
    fill, stroke = colors
    svg.rect(x, y, w, h, fill, stroke)
    svg.text(x + 14, y + 24, title, size=14, weight="bold", color=stroke)
    svg.lines(
        x + 14,
        y + 46,
        rows,
        size=size,
        color=INK,
        family=MONO if mono else FONT,
    )


# ---------------------------------------------------------------------------
# 1. graphic abstract
# ---------------------------------------------------------------------------
def graphic_abstract() -> Svg:
    svg = Svg(1180, 590, "MetaHQ graphic abstract")
    svg.text(
        30, 40, "MetaHQ: community-curated metadata for public omics data", 22, "bold"
    )
    svg.text(
        30,
        64,
        "Harmonize expert annotations of GEO/SRA samples and studies once; "
        "query them as tables or ML-ready labels.",
        14,
        color=MUTED,
    )

    # column 1: sources
    card(
        svg,
        30,
        95,
        250,
        330,
        SOURCE,
        "13 curation sources",
        [
            "Keyed by GEO (GSM / GSE):",
            "  ALE, CREEDS, DiSignAtlas,",
            "  Gemma, Golightly_2018,",
            "  Johnson_2023, KrishnanLab,",
            "  Sirota_2011, URSA, URSA_HD",
            "",
            "Keyed by SRA (SRR / SRX):",
            "  BGee, Cello, Gu_2023,",
            "  Johnson_2023",
            "",
            "Each with its own vocabulary,",
            "IDs and license",
        ],
    )

    # column 2: harmonize
    card(
        svg,
        330,
        95,
        250,
        330,
        PROCESS,
        "Harmonize (metahq-build)",
        [
            "Map terms to ontologies",
            "  tissue  -> UBERON / CL",
            "  disease -> MONDO",
            "  sex     -> M / F",
            "  age     -> 7 age groups",
            "Map SRA IDs to GSM (OmicIDX)",
            "Keep the most specific term",
            "Tag each annotation with",
            "  source + evidence code",
            "  (expert / crowd)",
            "Merge to sample and series",
            "  level records",
        ],
    )

    # column 3: data package
    x, y = 630, 95
    svg.rect(x, y, 230, 330, *STORE)
    svg.text(x + 14, y + 24, "Data package (Zenodo)", 14, "bold", STORE[1])
    blocks = [
        ("annotations/", "sample + series BSON"),
        ("ontology/", "relations matrices,"),
        ("", "term search index"),
        ("metadata/", "GEO text fields,"),
        ("", "platforms, links,"),
        ("", "refine.bio IDs"),
    ]
    yy = y + 50
    for name, desc in blocks:
        if name:
            yy += 10
            svg.text(x + 14, yy, name, 13, "bold", family=MONO)
            yy += 18
        svg.text(x + 24, yy, desc, 12, color=MUTED)
        yy += 18
    svg.text(x + 14, y + 300, "metahq setup downloads it", 12, color=MUTED, italic=True)

    # column 4: CLI
    card(
        svg,
        910,
        95,
        240,
        150,
        USER,
        "metahq CLI",
        [
            "search    text -> term IDs",
            "retrieve  tissues | diseases",
            "          | sex | age",
            "filters   species, tech,",
            "          ecode, license",
        ],
        mono=True,
        size=11,
    )
    card(
        svg,
        910,
        270,
        240,
        155,
        USER,
        "You get",
        [
            "result.parquet / .tsv / .json",
            "  annotate: 0 / 1",
            "  label: +1 / 0 / -1 / 2",
            "  + sources, external links",
            "CITATION.txt per query",
            "optional refine.bio dataset",
        ],
        size=12,
    )

    for x1, x2 in ((280, 330), (580, 630), (860, 910)):
        svg.arrow(x1 + 4, 260, x2 - 6, 260)
    svg.arrow(1030, 245, 1030, 266)

    # bottom strip: what the label modes mean, in one line
    svg.rect(30, 450, 860, 80, "#ffffff", LINE, rx=6, sw=1)
    svg.text(46, 475, "Ontology-aware labels", 14, "bold")
    svg.lines(
        46,
        497,
        [
            "Query 'heart': a myocardium sample is +1 (descendant), an 'organ' sample "
            "is 0 (ancestor: unknown),",
            "a liver sample is -1. Disease controls from a study with cases get 2.",
        ],
        size=12,
        color=MUTED,
    )
    svg.legend(
        910,
        440,
        [
            (*SOURCE, "community sources"),
            (*PROCESS, "build pipeline"),
            (*STORE, "distributed files"),
            (*USER, "what you run / get"),
        ],
    )
    return svg


# ---------------------------------------------------------------------------
# 2. data model
# ---------------------------------------------------------------------------
def data_model() -> Svg:
    svg = Svg(1180, 730, "MetaHQ data package data model")
    svg.text(30, 40, "Inside a MetaHQ data package", 22, "bold")
    svg.text(
        30,
        64,
        "One record per sample (GSM) or series (GSE); everything else is a "
        "lookup table joined on an ID.",
        14,
        color=MUTED,
    )

    # central record
    x, y, w, h = 420, 100, 340, 400
    svg.rect(x, y, w, h, *STORE)
    svg.text(x + 14, y + 24, "annotations/combined__level-sample.bson", 13, "bold",
             STORE[1], family=MONO)  # fmt: skip
    record = [
        '"GSM1656436": {',
        '  "organism": "homo sapiens",',
        '  "accession_ids": {',
        '    "sample": "GSM1656436",',
        '    "series": "GSE67812",',
        '    "platform": "GPL11154",',
        '    "srx": "SRX993404", "srs": ..,',
        '    "srp": "SRP057118" },',
        '  "tissue": {',
        '    "DiSignAtlas": {',
        '      "id": "UBERON:0002084",',
        '      "ecode": "expert-curated" },',
        '    "Gu_2023": { ... } },',
        '  "disease": { "<source>": {..} },',
        '  "sex":     { "<source>": {"id": "M"} },',
        '  "age":     { "<source>": {"id": "adult"} }',
        "}",
    ]
    svg.lines(x + 16, y + 50, record, size=12, gap=19, family=MONO)
    svg.text(x + 14, y + h - 14, "combined__level-series.bson: same shape, keyed by GSE",
             11, color=MUTED, italic=True)  # fmt: skip

    def table(tx, ty, tw, name, cols, colors=STORE, note=None):
        th = 34 + 18 * len(cols) + (18 if note else 0)
        svg.rect(tx, ty, tw, th, colors[0], colors[1])
        svg.text(tx + 12, ty + 22, name, 12, "bold", colors[1], family=MONO)
        svg.lines(tx + 18, ty + 42, cols, size=11, gap=18, family=MONO, color=INK)
        if note:
            svg.text(tx + 12, ty + th - 10, note, 11, color=MUTED, italic=True)
        return th

    # left column: metadata tables, each joined to the record on one ID
    k = "join on "
    table(30, 100, 330, "metadata/technologies.parquet",
          ["id (GPL)", "name", "technology  rnaseq | microarray"],
          note=k + "accession_ids.platform")  # fmt: skip
    table(30, 226, 330, "metadata/metadata__level-sample.parquet",
          ["sample", "title, description", "source_name_ch1/2",
           "characteristics_ch1/2"], note=k + "accession_ids.sample")  # fmt: skip
    table(30, 370, 330, "metadata/metadata__level-series.parquet",
          ["series", "title, summary, overall_design", "sample_id, description"],
          note=k + "accession_ids.series")  # fmt: skip
    table(30, 496, 330, "metadata/external_links.parquet",
          ["series", "Gemma, BGee, DiSignAtlas", "  {browse_url, records[id,url]}"],
          note=k + "accession_ids.series")  # fmt: skip
    table(30, 622, 330, "metadata/refinebio_map.parquet",
          ["gsm, gse", "refinebio_sample, refinebio_experiment"],
          note=k + "sample / series")  # fmt: skip

    # right column: ontology tables and source registry
    th = table(820, 100, 330, "ontology/<onto>/relations.parquet",
               ["terms x terms Int8 matrix", "row r, col c = 1 if r is an",
                "ancestor of c (or r == c)", "columns sorted by term ID",
                "<onto> = uberon_ext | mondo"],
               note=k + "tissue / disease id")  # fmt: skip
    table(820, 100 + th + 16, 330, "ontology/<onto>/id_map.parquet",
          ["id, name"], note=k + "tissue / disease id")  # fmt: skip
    table(820, 100 + th + 114, 330, "ontology/ontology_search.duckdb",
          ["ontology_search_docs", "  term_id, name, ontology, type",
           "ontology_synonyms", "  term_id, synonym, scope"],
          note="used by metahq search")  # fmt: skip
    table(820, 600 - 96, 330, "metahq_core.sources (code)",
          ["source -> citation, license", "  permissive: CC0 / CC BY",
           "  nc: CC BY-NC / academic"], colors=PROCESS,
          note=k + "source name")  # fmt: skip

    svg.arrow(418, 300, 366, 300)
    svg.arrow(762, 300, 814, 300)

    svg.legend(
        420,
        560,
        [
            (*STORE, "file in the data package"),
            (*PROCESS, "defined in metahq-core code"),
        ],
    )
    svg.lines(420, 680, ["Pipe-delimited values ('GSE1|GSE2', 'UBERON:1|UBERON:2')",
                         "hold several IDs."], size=12, color=MUTED)  # fmt: skip
    return svg


# ---------------------------------------------------------------------------
# 3. label semantics
# ---------------------------------------------------------------------------
def label_semantics() -> Svg:
    svg = Svg(1060, 520, "How annotate and label modes use the ontology")
    svg.text(30, 40, "Query: heart (UBERON:0000948)", 22, "bold")
    svg.text(
        30,
        64,
        "Each sample's most specific annotation is compared with the query term "
        "through the ontology.",
        14,
        color=MUTED,
    )

    nodes = {
        "organ": (330, 120),
        "heart": (110, 230),
        "liver": (260, 230),
        "brain": (400, 230),
        "kidney": (550, 230),
        "myocardium": (110, 340),
        "cerebral cortex": (400, 340),
    }
    edges = [
        ("organ", "heart"),
        ("organ", "liver"),
        ("organ", "brain"),
        ("organ", "kidney"),
        ("heart", "myocardium"),
        ("brain", "cerebral cortex"),
    ]
    label = {
        "organ": ("0", UNK),
        "heart": ("+1", POS),
        "myocardium": ("+1", POS),
        "liver": ("-1", NEG),
        "brain": ("-1", NEG),
        "cerebral cortex": ("-1", NEG),
        "kidney": ("-1", NEG),
    }
    for a, b in edges:
        (x1, y1), (x2, y2) = nodes[a], nodes[b]
        svg.path(f"M {x1} {y1 + 22} L {x2} {y2 - 22}", sw=1.5, arrow=False)
    for name, (cx, cy) in nodes.items():
        value, color = label[name]
        w = max(90, len(name) * 8 + 24)
        stroke_w = 3 if name == "heart" else 1.5
        svg.rect(cx - w / 2, cy - 20, w, 40, "#ffffff", color, rx=20, sw=stroke_w)
        svg.text(cx, cy + 5, name, 13, "bold" if name == "heart" else "normal",
                 anchor="middle")  # fmt: skip
        svg.circle(cx + w / 2 - 2, cy - 18, 13, color, color)
        svg.text(cx + w / 2 - 2, cy - 14, value, 11, "bold", "#ffffff", "middle")

    svg.lines(30, 410, ["Lines join a parent term (above) to its child (below).",
                        "Badges: label of a sample annotated to that term."],
              size=12, color=MUTED, italic=True)  # fmt: skip

    # table on the right
    x, y = 640, 100
    svg.rect(x, y, 390, 250, "#ffffff", LINE, rx=6, sw=1)
    svg.text(x + 14, y + 24, "A sample annotated to ...", 13, "bold")
    svg.text(x + 236, y + 24, "annotate", 13, "bold", anchor="middle")
    svg.text(x + 330, y + 24, "label", 13, "bold", anchor="middle")
    rows = [
        ("heart itself", "1", "+1", POS),
        ("a descendant (myocardium)", "1", "+1", POS),
        ("an ancestor (organ)", "dropped", "0", UNK),
        ("anything else (liver)", "dropped", "-1", NEG),
        ("a disease control*", "-", "2", CTRL),
    ]
    svg.lines(x + 14, y + 216, ["* MONDO:0000000, in a study where other samples",
                                "  are +1 for the term; otherwise 0. Diseases only."],
              size=11, color=MUTED)  # fmt: skip
    for i, (what, ann, lab, color) in enumerate(rows):
        yy = y + 56 + i * 30
        svg.text(x + 14, yy, what, 12)
        svg.text(x + 236, yy, ann, 12, color=MUTED, anchor="middle", family=MONO)
        svg.text(x + 330, yy, lab, 13, "bold", color, "middle", family=MONO)

    svg.legend(
        640,
        370,
        [
            ("#ffffff", POS, "+1  is the term"),
            ("#ffffff", UNK, " 0  unknown (too general)"),
            ("#ffffff", NEG, "-1  is not the term"),
            ("#ffffff", CTRL, " 2  control in a study of the term"),
        ],
        title="Labels (label mode)",
    )
    return svg


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    for name, build in (
        ("graphic_abstract", graphic_abstract),
        ("data_model", data_model),
        ("label_semantics", label_semantics),
    ):
        path = OUT / f"{name}.svg"
        path.write_text(build().render(), encoding="utf-8")
        print(f"wrote {path}")


if __name__ == "__main__":
    main()
