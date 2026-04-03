"""
MetaHQ Setup Package.

A data pipeline package for preprocessing raw biomedical annotations
and building the MetaHQ database.

This package provides:
- Data source processors for various biomedical databases
- Pipeline orchestration with checkpointing
- Ontology processing and relationship extraction
- ID mapping between GEO and SRA
- Annotation propagation through ontology hierarchies
- Database building utilities

Example:
    >>> from metahq_setup import Pipeline
    >>> pipeline = Pipeline.from_config("config.yaml")
    >>> pipeline.run()
"""

from metahq_setup.__version__ import __version__

__all__ = ["__version__"]
