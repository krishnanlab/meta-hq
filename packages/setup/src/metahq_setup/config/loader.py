"""
Configuration loading and merging utilities.

Handles loading configuration from YAML files, environment variables,
and command-line overrides with proper priority handling.
"""

import os
from pathlib import Path
from typing import Any

import yaml
from pydantic import ValidationError

from metahq_setup.config.schema import PipelineConfig


def load_yaml(file_path: Path) -> dict[str, Any]:
    """
    Load configuration from a YAML file.

    Arguments:
        file_path (Path):
            Path to YAML configuration file

    Returns:
        (dict[str, Any]): Configuration dictionary

    Raises:
        FileNotFoundError: If file doesn't exist
        yaml.YAMLError: If file is not valid YAML
    """
    if not file_path.exists():
        raise FileNotFoundError(f"Configuration file not found: {file_path}")

    with open(file_path, "r") as f:
        try:
            config_dict = yaml.safe_load(f)
            return config_dict if config_dict is not None else {}
        except yaml.YAMLError as e:
            raise yaml.YAMLError(f"Invalid YAML in {file_path}: {e}")


def merge_configs(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    """
    Recursively merge two configuration dictionaries.

    Override values take precedence over base values. Nested dictionaries
    are merged recursively.

    Arguments:
        base (dict[str, Any]):
            Base configuration dictionary
        override (dict[str, Any]):
            Override configuration dictionary

    Returns:
        (dict[str, Any]): Merged configuration
    """
    merged = base.copy()

    for key, value in override.items():
        if key in merged and isinstance(merged[key], dict) and isinstance(value, dict):
            # Recursively merge nested dictionaries
            merged[key] = merge_configs(merged[key], value)
        else:
            # Override value
            merged[key] = value

    return merged


def load_env_overrides() -> dict[str, Any]:
    """
    Load configuration overrides from environment variables.

    Environment variables follow the pattern: METAHQ_SETUP_<KEY>=<VALUE>
    Nested keys use double underscores: METAHQ_SETUP_PARALLEL__NUM_WORKERS=8

    Returns:
        (dict[str, Any]): Configuration overrides from environment
    """
    prefix = "METAHQ_SETUP_"
    overrides: dict[str, Any] = {}

    for env_key, env_value in os.environ.items():
        if not env_key.startswith(prefix):
            continue

        # Remove prefix and convert to lowercase
        config_key = env_key[len(prefix) :].lower()

        # Handle nested keys (double underscore)
        if "__" in config_key:
            parts = config_key.split("__")
            current = overrides
            for part in parts[:-1]:
                if part not in current:
                    current[part] = {}
                current = current[part]
            current[parts[-1]] = _parse_env_value(env_value)
        else:
            overrides[config_key] = _parse_env_value(env_value)

    return overrides


def _parse_env_value(value: str) -> Any:
    """
    Parse environment variable value to appropriate type.

    Arguments:
        value (str):
            String value from environment variable

    Returns:
        (Any): Parsed value (bool, int, str)
    """
    # Try boolean
    if value.lower() in ("true", "yes", "1"):
        return True
    if value.lower() in ("false", "no", "0"):
        return False

    # Try integer
    try:
        return int(value)
    except ValueError:
        pass

    # Return as string
    return value


def load_config(
    config_file: Path | None = None,
    overrides: dict[str, Any] | None = None,
    use_env: bool = True,
) -> PipelineConfig:
    """
    Load pipeline configuration with priority handling.

    Configuration is loaded and merged in this order (highest priority last):
    1. Default configuration (from Pydantic defaults)
    2. User configuration file (if provided)
    3. Environment variables (if use_env=True)
    4. Direct overrides (if provided)

    Arguments:
        config_file (Path | None):
            Path to user configuration file
        overrides (dict[str, Any] | None):
            Direct configuration overrides
        use_env (bool):
            Whether to load overrides from environment variables

    Returns:
        (PipelineConfig): Validated pipeline configuration

    Raises:
        ValidationError: If configuration is invalid

    Examples:
        >>> # Load with defaults only
        >>> config = load_config()

        >>> # Load from file
        >>> config = load_config(config_file=Path("my_config.yaml"))

        >>> # Load with overrides
        >>> config = load_config(
        ...     config_file=Path("my_config.yaml"),
        ...     overrides={"parallel": {"num_workers": 16}}
        ... )
    """
    # Seed with sensible defaults so no-config invocations work
    config_dict: dict[str, Any] = {
        "data_dir": Path.cwd() / "data",
        "output_dir": Path.cwd() / "output",
    }

    # Load from file if provided
    if config_file is not None:
        file_config = load_yaml(config_file)
        config_dict = merge_configs(config_dict, file_config)

    # Load environment variable overrides
    if use_env:
        env_config = load_env_overrides()
        config_dict = merge_configs(config_dict, env_config)

    # Apply direct overrides
    if overrides is not None:
        config_dict = merge_configs(config_dict, overrides)

    # Validate and create PipelineConfig
    try:
        return PipelineConfig(**config_dict)
    except ValidationError as e:
        raise ValueError(f"Invalid configuration file: {config_file}") from e


def save_config(config: PipelineConfig, output_path: Path) -> None:
    """
    Save configuration to a YAML file.

    Arguments:
        config (PipelineConfig):
            Configuration to save
        output_path (Path):
            Path to save YAML file
    """
    output_path.parent.mkdir(parents=True, exist_ok=True)

    # Convert to dict and save
    config_dict = config.model_dump(mode="python")

    # Convert Path objects to strings for YAML serialization
    def path_to_str(obj: Any) -> Any:
        if isinstance(obj, Path):
            return str(obj)
        elif isinstance(obj, dict):
            return {k: path_to_str(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [path_to_str(item) for item in obj]
        return obj

    config_dict = path_to_str(config_dict)

    with open(output_path, "w") as f:
        yaml.safe_dump(config_dict, f, default_flow_style=False, sort_keys=False)


def get_default_config() -> PipelineConfig:
    """
    Get default pipeline configuration.

    Returns:
        (PipelineConfig): Default configuration with all defaults applied
    """
    return PipelineConfig(
        data_dir=Path.cwd() / "data",
        output_dir=Path.cwd() / "output",
    )
