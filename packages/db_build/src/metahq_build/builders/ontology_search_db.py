"""
Build the DuckDB database for metahq ontology search functions.

Tables created:
  - ontology_terms(id, name, ontology, type)
  - ontology_synonyms(term_id, synonym, scope)
  - ontology_search_docs(term_id, name, ontology, type, syn_exact, syn_narrow, syn_broad, syn_related)
"""

import json
from pathlib import Path

import duckdb
import polars as pl

from metahq_build.config import ONTOLOGY_DIR, ONTOLOGY_SEARCH_DB
from metahq_build.util.logging import setup_logger

_TABLE_TERMS = "ontology_terms"
_TABLE_SYNS = "ontology_synonyms"
_TABLE_DOCS = "ontology_search_docs"

_DEFAULT_SCOPE = "RELATED"

_TYPE_BY_ONTOLOGY = {
    "MONDO": "disease",
    "UBERON": "tissue",
    "CL": "celltype",
}

_SEARCH_MACRO_SQL = Path(__file__).parent / "search_macro.sql"


def _quote_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _syns_by_scope(scope: str) -> pl.Expr:
    return (
        pl.when(pl.col("syn_list").is_null() | (pl.col("syn_list").list.len() == 0))
        .then(pl.lit([], dtype=pl.List(pl.Utf8)))
        .otherwise(
            pl.col("syn_list")
            .list.eval(
                pl.when(pl.element().struct.field("scope") == scope)
                .then(pl.element().struct.field("synonym"))
                .otherwise(None)
            )
            .list.drop_nulls()
        )
        .alias(f"syn_{scope.lower()}")
    )


class OntologySearchDbBuilder:
    """Builds the DuckDB ontology search database from MONDO and UBERON/CL name lists."""

    def __init__(
        self,
        mondo: Path | None = None,
        uberon_cl: Path | None = None,
        out_db: Path | None = None,
    ):
        self.mondo = mondo
        self.uberon_cl = uberon_cl
        self.out_db = out_db
        self.logger = setup_logger(
            f"metahq_build.builders.ontology_search_db.{self.__class__.__name__}"
        )

    def build(self) -> None:
        """Build and write the ontology search DuckDB database."""
        mondo, uberon_cl, out_db = self._resolve_paths()

        self.logger.info("Loading name/synonym lists...")
        namelists = self._load_namelists(mondo, uberon_cl)

        self.logger.info("Collecting terms + synonyms...")
        df_terms, df_syns, df_docs = self._build_dataframes(namelists)

        self.logger.info("Writing DuckDB → %s ...", out_db)
        self._write_db(df_terms, df_syns, df_docs, out_db)

        self.logger.info(
            "Done. Terms: %s | Synonyms: %s | Docs: %s",
            f"{df_terms.height:,}",
            f"{df_syns.height:,}",
            f"{df_docs.height:,}",
        )

    def _resolve_paths(self) -> tuple[Path, Path, Path]:

        mondo = self.mondo or ONTOLOGY_DIR / "mondo" / "names_synonyms.json"
        uberon_cl = self.uberon_cl or ONTOLOGY_DIR / "uberon" / "names_synonyms.json"
        out_db = self.out_db or ONTOLOGY_SEARCH_DB
        return mondo, uberon_cl, out_db

    def _load_namelists(self, mondo: Path, uberon_cl: Path) -> list[dict]:
        with open(mondo) as f:
            mondo_list = json.load(f)
        with open(uberon_cl) as f:
            uberon_cl_list = json.load(f)
        return [mondo_list, uberon_cl_list]

    @staticmethod
    def _build_dataframes(
        namelists: list[dict],
    ) -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]:
        term_rows = []
        syn_rows = []

        for namelist in namelists:
            for term_id, entry in namelist.items():
                name = entry.get("name")
                synonyms = entry.get("synonyms", [])

                if not name:
                    continue

                ont = term_id.split(":")[0]
                term_rows.append(
                    {
                        "id": term_id,
                        "name": name,
                        "ontology": ont,
                        "type": _TYPE_BY_ONTOLOGY.get(ont, "none"),
                    }
                )

                for syn in synonyms:
                    syn_rows.append(
                        {
                            "term_id": term_id,
                            "synonym": syn["text"],
                            "scope": syn.get("scope", _DEFAULT_SCOPE),
                        }
                    )

        df_terms = pl.DataFrame(term_rows)

        df_syns = (
            pl.DataFrame(syn_rows)
            if syn_rows
            else pl.DataFrame(
                {
                    "term_id": pl.Series([], pl.Utf8),
                    "synonym": pl.Series([], pl.Utf8),
                    "scope": pl.Series([], pl.Utf8),
                }
            )
        )

        syn_grouped = (
            (
                df_syns.group_by("term_id").agg(
                    pl.struct(["synonym", "scope"]).alias("syn_list")
                )
            )
            if df_syns.height > 0
            else pl.DataFrame({"term_id": [], "syn_list": []})
        )

        df_terms_plus = df_terms.join(
            syn_grouped, left_on="id", right_on="term_id", how="left"
        )
        df_terms_plus = df_terms_plus.with_columns(pl.col("syn_list").fill_null([]))
        df_terms_plus = df_terms_plus.with_columns(
            _syns_by_scope("EXACT"),
            _syns_by_scope("NARROW"),
            _syns_by_scope("BROAD"),
            _syns_by_scope("RELATED"),
        )

        df_docs = pl.DataFrame(
            {
                "term_id": df_terms_plus["id"],
                "ontology": df_terms_plus["ontology"],
                "type": df_terms_plus["type"],
                "name": df_terms_plus["name"],
                "syn_exact": df_terms_plus["syn_exact"],
                "syn_narrow": df_terms_plus["syn_narrow"],
                "syn_broad": df_terms_plus["syn_broad"],
                "syn_related": df_terms_plus["syn_related"],
            }
        )

        return df_terms, df_syns, df_docs

    def _write_db(
        self,
        df_terms: pl.DataFrame,
        df_syns: pl.DataFrame,
        df_docs: pl.DataFrame,
        out_db: Path,
    ) -> None:
        with duckdb.connect(str(out_db)) as conn:
            conn.register("df_terms", df_terms.to_arrow())
            conn.register("df_syns", df_syns.to_arrow())
            conn.register("df_docs", df_docs.to_arrow())

            conn.execute(
                f"CREATE OR REPLACE TABLE {_quote_ident(_TABLE_TERMS)} AS SELECT * FROM df_terms"
            )
            conn.execute(
                f"CREATE OR REPLACE TABLE {_quote_ident(_TABLE_SYNS)} AS SELECT * FROM df_syns"
            )
            conn.execute(
                f"CREATE OR REPLACE TABLE {_quote_ident(_TABLE_DOCS)} AS SELECT * FROM df_docs"
            )

            conn.execute(f"""
            PRAGMA create_fts_index('{_quote_ident(_TABLE_DOCS)}', 'term_id',
                'name',
                'syn_exact',
                'syn_narrow',
                'syn_broad',
                'syn_related',
                stemmer = 'none', stopwords = 'none', ignore = '([^0-9a-z+/-])+',
                strip_accents = 1, lower = 1, overwrite = 1)
            """)

            conn.execute(_SEARCH_MACRO_SQL.read_text())
