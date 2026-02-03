# Publishing Checklist

Before a MetaHQ package is published to PyPI, make sure all statements below are satisfied.

1. Updated package version in:
    - `/packages/core/src/metahq_core/__init__.py` for `metahq_core`.
    - `/packages/cli/src/metahq_cli/__init__.py` for `metahq_cli`.
2. All tests passing.
3. Updated/checked hard-coded variables in:
    - `/packages/core/src/metahq_core/util/supported.py` for `metahq_core`.
    - `/packages/cli/src/metahq_cli/util/supported.py` for `metahq_cli`.
4. Made note of any new hard-coded variables.
5. Updated `pyproject.toml` in:
    - `/packages/core/pyproject.toml` for `metahq_core`.
    - `/packages/cli/pyproject.toml` for `metahq_cli`.
6. Updated `pip install` from tar example in `docs/getting-started/installation.md` with new package number.
7. Updated/checked `metahq setup` example specifying a DOI in `docs/user-guide/setup.md` with newest MetaHQ data package DOI.
8. Updated/checked manual documentation in `docs/user-guide`.
