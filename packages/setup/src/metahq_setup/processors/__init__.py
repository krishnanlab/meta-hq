"""
Data source processors for metahq-setup.

Provides a plugin architecture for processing various biomedical data sources
into standardized annotation format.

To create a new processor:
    1. Inherit from BaseProcessor
    2. Implement download(), process(), and validate() methods
    3. Decorate with @ProcessorRegistry.register

Example:
    >>> from metahq_setup.processors import BaseProcessor, ProcessorRegistry
    >>> @ProcessorRegistry.register
    ... class MyProcessor(BaseProcessor):
    ...     source_name = "my_source"
    ...     version = "1.0.0"
    ...     # Implement required methods...

    >>> # Use the processor
    >>> processor = ProcessorRegistry.get("my_source")
    >>> data = processor.run(output_dir=Path("./output"))
"""

from metahq_setup.processors.base import (
    BaseProcessor,
    ProcessorError,
    ValidationError,
)
from metahq_setup.processors.registry import ProcessorRegistry

# Import all processors to trigger registration
from metahq_setup.processors.ale import ALEProcessor
from metahq_setup.processors.cello import CellOProcessor
from metahq_setup.processors.creeds import CREEDSProcessor
from metahq_setup.processors.disign_atlas import DiSignAtlasProcessor
from metahq_setup.processors.gemma import GemmaProcessor
from metahq_setup.processors.golightly import GolightlyProcessor
from metahq_setup.processors.gu import GuProcessor
from metahq_setup.processors.sampleclass_zoo import SampleClassZooProcessor
from metahq_setup.processors.ursa import URSAProcessor
from metahq_setup.processors.ursahd import URSAHDProcessor

__all__ = [
    # Base classes
    "BaseProcessor",
    "ProcessorRegistry",
    "ProcessorError",
    "ValidationError",
    # Processor implementations
    "ALEProcessor",
    "CellOProcessor",
    "CREEDSProcessor",
    "DiSignAtlasProcessor",
    "GemmaProcessor",
    "GolightlyProcessor",
    "GuProcessor",
    "SampleClassZooProcessor",
    "URSAProcessor",
    "URSAHDProcessor",
]
