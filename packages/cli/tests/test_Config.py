"""
Unit tests for Config class.

Authors: Claude Code, Parker Hicks
Date: 2025-11-21

Last updated: 2025-11-21 by Parker Hicks
"""

from unittest.mock import Mock, patch

import pytest
import yaml

from metahq_cli.setup.config import Config


class TestConfig:
    """Tests for the Config class."""

    @pytest.fixture
    def mock_logger(self):
        """Fixture for mock logger."""
        return Mock()

    @pytest.fixture
    def config(self, mock_logger, tmp_path):
        """Fixture for Config instance."""
        return Config(
            version="v1.0.0",
            zenodo_doi="17663087",
            data_dir=str(tmp_path / "data"),
            logs=str(tmp_path / "logs"),
            logger=mock_logger,
            verbose=False,
        )

    @pytest.fixture
    def verbose_config(self, mock_logger, tmp_path):
        """Fixture for verbose Config instance."""
        return Config(
            version="v1.0.0",
            zenodo_doi="17663087",
            data_dir=str(tmp_path / "data"),
            logs=str(tmp_path / "logs"),
            logger=mock_logger,
            verbose=True,
        )

    @pytest.fixture
    def mock_config_file(self, tmp_path):
        """Fixture that mocks CONFIG_FILE to use a temporary path."""
        config_file = tmp_path / "config.yaml"
        with patch("metahq_cli.setup.config.CONFIG_FILE", config_file):
            yield config_file

    # ========================================
    # ======  initialization tests
    # ========================================

    def test_config_initialization(self, mock_logger, tmp_path):
        """Test Config stores all attributes correctly."""
        config = Config(
            version="v1.0.0",
            zenodo_doi="17663087",
            data_dir=str(tmp_path / "data"),
            logs=str(tmp_path / "logs"),
            logger=mock_logger,
            loglevel=20,
            verbose=True,
        )

        assert config.version == "v1.0.0"
        assert config.zenodo_doi == "17663087"
        assert config.data_dir == str(tmp_path / "data")
        assert config.logs == str(tmp_path / "logs")
        assert config.logger == mock_logger
        assert config.verbose is True
        assert config.ok_keys == ["version", "zenodo_doi", "data_dir", "logs"]

    def test_config_creates_logger_when_none_provided(self, tmp_path):
        """Test Config creates a logger when none is provided."""
        with patch("metahq_cli.setup.config.setup_logger") as mock_setup_logger:
            mock_setup_logger.return_value = Mock()

            config = Config(
                version="v1.0.0",
                zenodo_doi="17663087",
                data_dir=str(tmp_path / "data"),
                logs=str(tmp_path / "logs"),
            )

            mock_setup_logger.assert_called_once()
            assert config.logger is not None

    # ========================================
    # ======  check tests
    # ========================================

    def test_check_creates_config_file_when_not_exists(
        self, verbose_config, mock_config_file
    ):
        """Test check creates config file when it doesn't exist."""
        assert not mock_config_file.exists()

        verbose_config.check()

        assert mock_config_file.exists()
        verbose_config.logger.debug.assert_called()

    def test_check_validates_existing_config(self, verbose_config, mock_config_file):
        """Test check validates existing config file."""
        # Create valid config file
        valid_config = {
            "version": "v1.0.0",
            "zenodo_doi": "17663087",
            "data_dir": "/some/path",
            "logs": "/some/logs",
        }
        with open(mock_config_file, "w", encoding="utf-8") as f:
            yaml.safe_dump(valid_config, f)

        with patch.object(verbose_config, "is_acceptable_config", return_value=True):
            verbose_config.check()

        verbose_config.logger.debug.assert_called()

    def test_check_resets_invalid_config(self, verbose_config, mock_config_file):
        """Test check resets invalid config with defaults."""
        # Create invalid config file
        invalid_config = {"invalid_key": "value"}
        with open(mock_config_file, "w", encoding="utf-8") as f:
            yaml.safe_dump(invalid_config, f)

        with patch.object(verbose_config, "set_default") as mock_set_default:
            with patch.object(
                verbose_config, "is_acceptable_config", return_value=False
            ):
                verbose_config.check()

            mock_set_default.assert_called_once()
            verbose_config.logger.warning.assert_called()

    def test_check_silent_when_not_verbose(self, config, mock_config_file):
        """Test check doesn't log debug messages when not verbose."""
        assert not mock_config_file.exists()

        config.check()

        assert mock_config_file.exists()
        config.logger.debug.assert_not_called()

    # ========================================
    # ======  is_acceptable_config tests
    # ========================================

    def test_is_acceptable_config_returns_true_for_valid(
        self, config, mock_config_file
    ):
        """Test is_acceptable_config returns True for valid config."""
        valid_config = {
            "version": "v1.0.0",
            "zenodo_doi": "17663087",
            "data_dir": "/some/path",
            "logs": "/some/logs",
        }
        with open(mock_config_file, "w", encoding="utf-8") as f:
            yaml.safe_dump(valid_config, f)

        result = config.is_acceptable_config()

        assert result is True

    def test_is_acceptable_config_returns_false_for_missing_keys(
        self, config, mock_config_file
    ):
        """Test is_acceptable_config returns False when keys are missing."""
        invalid_config = {
            "version": "v1.0.0",
            "zenodo_doi": "17663087",
            # missing data_dir and logs
        }
        with open(mock_config_file, "w", encoding="utf-8") as f:
            yaml.safe_dump(invalid_config, f)

        result = config.is_acceptable_config()

        assert result is False

    def test_is_acceptable_config_returns_false_for_extra_keys(
        self, config, mock_config_file
    ):
        """Test is_acceptable_config returns False when extra keys present."""
        invalid_config = {
            "version": "v1.0.0",
            "zenodo_doi": "17663087",
            "data_dir": "/some/path",
            "logs": "/some/logs",
            "extra_key": "extra_value",
        }
        with open(mock_config_file, "w", encoding="utf-8") as f:
            yaml.safe_dump(invalid_config, f)

        result = config.is_acceptable_config()

        assert result is False

    # ========================================
    # ======  load_config tests
    # ========================================

    def test_load_config_returns_dict(self, config, mock_config_file):
        """Test load_config returns config as dictionary."""
        expected_config = {
            "version": "v1.0.0",
            "zenodo_doi": "17663087",
            "data_dir": "/some/path",
            "logs": "/some/logs",
        }
        with open(mock_config_file, "w", encoding="utf-8") as f:
            yaml.safe_dump(expected_config, f)

        result = config.load_config()

        assert result == expected_config

    def test_load_config_exits_on_yaml_error(self, config, mock_config_file):
        """Test load_config exits on YAML parse error."""
        # Write invalid YAML
        with open(mock_config_file, "w", encoding="utf-8") as f:
            f.write("invalid: yaml: content: [")

        with pytest.raises(SystemExit):
            config.load_config()

    # ========================================
    # ======  load_config_str tests
    # ========================================

    def test_load_config_str_returns_string(self, config, mock_config_file):
        """Test load_config_str returns config as string."""
        content = "version: v1.0.0\nzenodo_doi: '17663087'\n"
        with open(mock_config_file, "w", encoding="utf-8") as f:
            f.write(content)

        result = config.load_config_str()

        assert result == content

    # ========================================
    # ======  make_config tests
    # ========================================

    def test_make_config_returns_correct_dict(self, config):
        """Test make_config returns config dictionary with correct values."""
        result = config.make_config()

        assert result["version"] == config.version
        assert result["zenodo_doi"] == config.zenodo_doi
        assert result["data_dir"] == config.data_dir
        assert result["logs"] == config.logs
        assert len(result) == 4

    # ========================================
    # ======  save_config tests
    # ========================================

    def test_save_config_writes_yaml(self, config, mock_config_file):
        """Test save_config writes config to YAML file."""
        test_config = {
            "version": "v1.0.0",
            "zenodo_doi": "17663087",
            "data_dir": "/test/path",
            "logs": "/test/logs",
        }

        config.save_config(test_config)

        # Verify file was written
        with open(mock_config_file, "r", encoding="utf-8") as f:
            saved_config = yaml.safe_load(f)

        assert saved_config == test_config
        config.logger.info.assert_called()

    def test_save_config_exits_on_yaml_error(self, config):
        """Test save_config exits on YAML dump error."""
        # Create an object that can't be serialized to YAML
        unserializable = {"func": lambda x: x}

        with pytest.raises(SystemExit):
            config.save_config(unserializable)

    # ========================================
    # ======  setup tests
    # ========================================

    def test_setup_calls_check_initialize_save(self, config):
        """Test setup calls check, initialize_config, and save_config."""
        with (
            patch.object(config, "check") as mock_check,
            patch.object(config, "initialize_config") as mock_initialize,
            patch.object(config, "save_config") as mock_save,
        ):
            mock_initialize.return_value = {"test": "config"}

            config.setup()

            mock_check.assert_called_once()
            mock_initialize.assert_called_once()
            mock_save.assert_called_once_with({"test": "config"})

    # ========================================
    # ======  initialize_config tests
    # ========================================

    def test_initialize_config_returns_resolved_paths(self, verbose_config, tmp_path):
        """Test initialize_config resolves data_dir path."""
        result = verbose_config.initialize_config()

        assert result["version"] == verbose_config.version
        assert result["zenodo_doi"] == verbose_config.zenodo_doi
        assert result["logs"] == verbose_config.logs
        # data_dir should be resolved to absolute path
        assert result["data_dir"] == str((tmp_path / "data").resolve())

    def test_initialize_config_logs_when_verbose(self, verbose_config):
        """Test initialize_config logs in verbose mode."""
        verbose_config.initialize_config()

        verbose_config.logger.debug.assert_called()

    def test_initialize_config_silent_when_not_verbose(self, config):
        """Test initialize_config doesn't log when not verbose."""
        config.initialize_config()

        config.logger.debug.assert_not_called()

    # ========================================
    # ======  path property tests
    # ========================================

    def test_path_returns_config_file_path(self, config, mock_config_file):
        """Test path property returns CONFIG_FILE path as string."""
        result = config.path

        assert result == str(mock_config_file)
