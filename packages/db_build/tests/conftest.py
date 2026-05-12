from metahq_build.config import SAMPLE_COMBINED_BSON, SERIES_COMBINED_BSON


def pytest_addoption(parser):
    """Pytest CLI argument parser."""
    parser.addoption("--sample-db", action="store", default=SAMPLE_COMBINED_BSON)
    parser.addoption("--series-db", action="store", default=SERIES_COMBINED_BSON)
