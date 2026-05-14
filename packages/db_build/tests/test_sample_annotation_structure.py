"""Test the schema and entries of the sample-level MetaHQ BSON database.

Requires a complete sample-level MetaHQ database.
Run `metahq-build package --config data/build_config.yaml` if needed.
"""

import bson
import pytest
from pydantic import ValidationError

from metahq_build.config.schema import SampleEntry


@pytest.fixture(scope="session")
def db(pytestconfig) -> dict:
    """Load compressed BSON."""
    file = pytestconfig.getoption("sample_db")
    with open(file, "rb") as f:
        return bson.decode(f.read())


@pytest.fixture(scope="session")
def ids(db):
    return list(db.keys())


class TestEntrySchema:

    @pytest.fixture(autouse=True)
    def setup(self, db, ids):
        self.db = db
        self.ids = ids

    def _validate_all(self) -> list[str]:
        errors = []
        for id_ in self.ids:
            try:
                SampleEntry.model_validate(self.db[id_])
            except ValidationError as e:
                errors.append(f"\n[{id_}\n{e}]")
        return errors

    def test_all_entries_pass_schema(self):
        errors = self._validate_all()
        assert not errors, "Schema validataions found:" + "".join(errors)
