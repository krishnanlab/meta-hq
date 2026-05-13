"""
Registry for data source processors.

Provides a centralized registry for discovering and managing processor
implementations using a decorator pattern.
"""

from typing import Type

from metahq_build.processors.base import BaseProcessor


class ProcessorRegistry:
    """
    Central registry for all data source processors.

    Uses a class decorator pattern to automatically register processors
    when they are defined.

    Attributes:
        _processors (dict[str, Type[BaseProcessor]]):
            Mapping of source names to processor classes
    """

    _processors: dict[str, Type[BaseProcessor]] = {}

    @classmethod
    def register(cls, processor_class: Type[BaseProcessor]) -> Type[BaseProcessor]:
        """
        Register a processor class.

        Used as a class decorator::

            @ProcessorRegistry.register
            class MyProcessor(BaseProcessor):
                ...

        Arguments:
            processor_class (Type[BaseProcessor]):
                Processor class to register

        Returns:
            (Type[BaseProcessor]): The processor class (unchanged)

        Raises:
            (ValueError): If processor with same source_name already registered
        """
        if not hasattr(processor_class, "source_name"):
            raise ValueError(
                f"Processor {processor_class.__name__} must define source_name"
            )

        source_name = processor_class.source_name

        if source_name in cls._processors:
            existing = cls._processors[source_name]
            raise ValueError(
                f"Processor with source_name '{source_name}' already registered: "
                f"{existing.__name__}"
            )

        cls._processors[source_name] = processor_class
        return processor_class

    @classmethod
    def get(cls, source_name: str) -> BaseProcessor:
        """
        Get a processor instance by source name.

        Arguments:
            source_name (str):
                Name of the data source

        Returns:
            (BaseProcessor): Instance of the processor

        Raises:
            KeyError: If processor not found
        """
        if source_name not in cls._processors:
            available = ", ".join(cls._processors.keys())
            raise KeyError(
                f"Processor '{source_name}' not found. "
                f"Available processors: {available}"
            )

        processor_class = cls._processors[source_name]
        return processor_class()

    @classmethod
    def get_class(cls, source_name: str) -> Type[BaseProcessor]:
        """
        Get a processor class (not instance) by source name.

        Arguments:
            source_name (str):
                Name of the data source

        Returns:
            (Type[BaseProcessor]): Processor class

        Raises:
            KeyError: If processor not found
        """
        if source_name not in cls._processors:
            available = ", ".join(cls._processors.keys())
            raise KeyError(
                f"Processor '{source_name}' not found. "
                f"Available processors: {available}"
            )

        return cls._processors[source_name]

    @classmethod
    def list_processors(cls) -> list[str]:
        """
        Get list of all registered processor source names.

        Returns:
            (list[str]): List of registered source names
        """
        return sorted(cls._processors.keys())

    @classmethod
    def get_all(cls) -> dict[str, Type[BaseProcessor]]:
        """
        Get all registered processor classes.

        Returns:
            (dict[str, Type[BaseProcessor]]): Mapping of source names to classes
        """
        return cls._processors.copy()

    @classmethod
    def is_registered(cls, source_name: str) -> bool:
        """
        Check if a processor is registered.

        Arguments:
            source_name (str):
                Name of the data source

        Returns:
            (bool): True if processor is registered
        """
        return source_name in cls._processors

    @classmethod
    def unregister(cls, source_name: str) -> None:
        """
        Unregister a processor (mainly for testing).

        Arguments:
            source_name (str):
                Name of the data source to unregister

        Raises:
            KeyError: If processor not found
        """
        if source_name not in cls._processors:
            raise KeyError(f"Processor '{source_name}' not registered")

        del cls._processors[source_name]

    @classmethod
    def clear(cls) -> None:
        """Clear all registered processors (mainly for testing)."""
        cls._processors.clear()

    @classmethod
    def get_info(cls, source_name: str) -> dict[str, str]:
        """
        Get information about a registered processor.

        Arguments:
            source_name (str):
                Name of the data source

        Returns:
            (dict[str, str]): Dictionary with processor information

        Raises:
            KeyError: If processor not found
        """
        processor_class = cls.get_class(source_name)
        return {
            "source_name": processor_class.source_name,
            "version": processor_class.version,
            "description": getattr(processor_class, "description", ""),
            "class_name": processor_class.__name__,
        }

    @classmethod
    def get_all_info(cls) -> dict[str, dict[str, str]]:
        """
        Get information about all registered processors.

        Returns:
            (dict[str, dict[str, str]]): Mapping of source names to info dicts
        """
        return {source: cls.get_info(source) for source in cls.list_processors()}
