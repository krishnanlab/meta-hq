"""
Unit tests for FileConfig and Downloader classes.

Authors: Claude Code, Parker Hicks
Date: 2025-11-21

Last updated: 2025-11-21 by Parker Hicks
"""

import tarfile
from unittest.mock import MagicMock, Mock, patch

import pytest
import requests

from metahq_cli.setup.downloader import Downloader, FileConfig

# set some global variables that will be updated for each version of database
url_cur = "https://zenodo.org/api/records/17663087/files/metahq.tar.gz/content"
doi_cur = "17663087"
filename_cur = "metahq.tar.gz"
version_cur = "v1.0.0-alpha"

class TestFileConfig:
    """Tests for the FileConfig dataclass."""

    @pytest.fixture
    def mock_checkdir(self):
        """Fixture that mocks checkdir to return the path unchanged."""
        with patch("metahq_cli.setup.downloader.checkdir") as mock:
            mock.side_effect = lambda x: x
            yield mock

    @pytest.fixture
    def file_config(self, tmp_path):
        """Fixture for a FileConfig instance."""
        return FileConfig(
            doi=doi_cur,
            filename=filename_cur,
            url=url_cur,
            version=version_cur,
            outdir=tmp_path,
            filesize=1048576,  # 1 MB
        )

    def test_fileconfig_initialization(self, tmp_path):
        """Test FileConfig stores all attributes correctly."""
        config = FileConfig(
            doi=doi_cur,
            filename=filename_cur,
            url=url_cur,
            version=version_cur,
            outdir=tmp_path,
        )

        assert config.doi == doi_cur
        assert config.filename == filename_cur
        assert config.url == url_cur
        assert config.version == version_cur
        assert config.outdir == tmp_path
        assert config.filesize is None

    def test_make_outdir(self, file_config, mock_checkdir):
        """Test mak_outdir calls checkdir."""
        mock_checkdir.reset_mock()

        file_config.make_outdir()

        mock_checkdir.assert_called_once_with(file_config.outdir)

    def test_filename_stemmed(self, file_config):
        """Test filename_stemmed returns filename without extensions."""
        assert file_config.filename_stemmed == "metahq"

    def test_filename_stemmed_multiple_extensions(self, tmp_path):
        """Test filename_stemmed handles multiple extensions."""
        config = FileConfig(
            doi=doi_cur,
            filename="metahq_data.tar.gz",
            url=url_cur,
            version=version_cur,
            outdir=tmp_path,
        )

        assert config.filename_stemmed == "metahq_data"

    def test_outfile(self, file_config, tmp_path):
        """Test outfile returns correct path."""
        expected = tmp_path / filename_cur
        assert file_config.outfile == expected

    def test_size_mb(self, file_config):
        """Test size_mb returns filesize in MB."""
        # 1048576 bytes = 1 MB
        assert file_config.size_mb == 1.0

    def test_size_mb_rounds_correctly(self, tmp_path):
        """Test size_mb rounds to 2 decimal places."""
        config = FileConfig(
            doi=doi_cur,
            filename=filename_cur,
            url=url_cur,
            version=version_cur,
            outdir=tmp_path,
            filesize=1500000,  # ~1.43 MB
        )

        assert config.size_mb == 1.43

    def test_size_mb_raises_when_filesize_not_set(self, tmp_path):
        """Test size_mb raises AttributeError when filesize is None."""
        config = FileConfig(
            doi=doi_cur,
            filename=filename_cur,
            url=url_cur,
            version=version_cur,
            outdir=tmp_path,
        )

        with pytest.raises(AttributeError, match="filesize not set"):
            _ = config.size_mb


class TestDownloader:
    """Tests for the Downloader class."""

    @pytest.fixture
    def mock_logger(self):
        """Fixture for mock logger."""
        return Mock()

    @pytest.fixture
    def mock_dependencies(self):
        """Fixture that mocks all external dependencies for Downloader."""
        with (
            patch("metahq_cli.setup.downloader.metahq_dois") as mock_dois,
            patch("metahq_cli.setup.downloader.zenodo_records_url") as mock_records_url,
            patch("metahq_cli.setup.downloader.zenodo_files_dir") as mock_files_dir,
            patch("metahq_cli.setup.downloader.checkdir") as mock_checkdir,
            patch("metahq_cli.setup.downloader.setup_logger") as mock_setup_logger,
            patch("metahq_cli.setup.downloader.get_console") as mock_get_console,
        ):
            mock_dois.return_value = {
                "version": version_cur,
                "filename": filename_cur,
            }
            mock_records_url.return_value = "https://zenodo.org/api/records"
            mock_files_dir.return_value = "files"
            mock_checkdir.side_effect = lambda x: x
            mock_setup_logger.return_value = Mock()

            yield {
                "dois": mock_dois,
                "records_url": mock_records_url,
                "files_dir": mock_files_dir,
                "checkdir": mock_checkdir,
                "setup_logger": mock_setup_logger,
                "get_console": mock_get_console,
            }

    @pytest.fixture
    def downloader(self, mock_logger, tmp_path):
        """Fixture for Downloader instance."""
        return Downloader(
            doi=doi_cur,
            outdir=tmp_path,
            logger=mock_logger,
            verbose=False,
        )

    @pytest.fixture
    def verbose_downloader(self, mock_logger, tmp_path):
        """Fixture for verbose Downloader instance."""
        return Downloader(
            doi=doi_cur,
            outdir=tmp_path,
            logger=mock_logger,
            verbose=True,
        )

    def test_downloader_initialization(self, mock_logger, tmp_path):
        """Test Downloader initialization creates config correctly."""
        downloader = Downloader(
            doi=doi_cur,
            outdir=tmp_path,
            logger=mock_logger,
            verbose=True,
        )

        assert downloader.config.doi == doi_cur
        assert downloader.config.filename == filename_cur
        assert downloader.config.version == version_cur
        assert downloader.logger == mock_logger
        assert downloader.verbose is True
        assert downloader._use_progress is True

    def test_downloader_initialization_default_logger(
        self, mock_dependencies, tmp_path
    ):
        """Test Downloader creates logger when none provided."""
        downloader = Downloader(
            doi=doi_cur,
            outdir=tmp_path,
        )

        mock_dependencies["setup_logger"].assert_called_once()
        assert downloader.logger is not None

    def test_make_url(self, downloader):
        """Test _make_url generates correct URL."""
        url = downloader._make_url(doi_cur, filename_cur)

        assert url == url_cur

    def test_make_config(self, mock_logger, tmp_path):
        """Test _make_config creates FileConfig with correct values."""
        downloader = Downloader(
            doi=doi_cur,
            outdir=tmp_path,
            logger=mock_logger,
        )

        assert downloader.config.doi == doi_cur
        assert downloader.config.filename == filename_cur
        assert (
            downloader.config.url
            == url_cur
        )
        assert downloader.config.version == version_cur
        assert downloader.config.outdir == tmp_path

    # ========================================
    # ======  check_outdir_exists tests
    # ========================================

    def test_check_outdir_exists_prompts_when_exists(
        self,
        downloader,
    ):
        """Test check_outdir_exists prompts user when directory exists."""
        with patch("metahq_cli.setup.downloader.click.confirm") as mock_confirm:
            mock_confirm.return_value = False

            with pytest.raises(SystemExit):
                downloader.check_outdir_exists()

            mock_confirm.assert_called_once()

    def test_check_outdir_exists_removes_dir_on_confirm(
        self, verbose_downloader, tmp_path
    ):
        """Test check_outdir_exists removes directory when user confirms."""
        with (
            patch("metahq_cli.setup.downloader.click.confirm") as mock_confirm,
            patch("metahq_cli.setup.downloader.shutil.rmtree") as mock_rmtree,
        ):
            mock_confirm.return_value = True

            verbose_downloader.check_outdir_exists()

            mock_rmtree.assert_called_once_with(tmp_path)
            verbose_downloader.logger.info.assert_called()

    def test_check_outdir_exists_exits_on_decline(
        self,
        verbose_downloader,
    ):
        """Test check_outdir_exists exits when user declines."""
        with patch("metahq_cli.setup.downloader.click.confirm") as mock_confirm:
            mock_confirm.return_value = False

            with pytest.raises(SystemExit, match="Terminating"):
                verbose_downloader.check_outdir_exists()

            verbose_downloader.logger.info.assert_called_with(
                "Keeping existing data directory."
            )

    # ========================================
    # ======  get_stats tests
    # ========================================

    def test_get_stats_sets_filesize(self, downloader):
        """Test get_stats retrieves and sets filesize."""
        mock_response = Mock()
        mock_response.headers = {"content-length": "1048576"}
        mock_response.status_code = 200

        with patch("metahq_cli.setup.downloader.requests.head") as mock_head:
            mock_head.return_value = mock_response

            downloader.get_stats()

            assert downloader.config.filesize == 1048576
            assert downloader._use_progress is True

    def test_get_stats_disables_progress_when_filesize_zero(self, verbose_downloader):
        """Test get_stats disables progress bar when filesize is 0."""
        mock_response = Mock()
        mock_response.headers = {"content-length": "0"}
        mock_response.status_code = 200

        with patch("metahq_cli.setup.downloader.requests.head") as mock_head:
            mock_head.return_value = mock_response

            verbose_downloader.get_stats()

            assert verbose_downloader.config.filesize == 0
            assert verbose_downloader._use_progress is False
            verbose_downloader.logger.warning.assert_called()

    def test_get_stats_logs_info_when_verbose(self, verbose_downloader):
        """Test get_stats logs information in verbose mode."""
        mock_response = Mock()
        mock_response.headers = {"content-length": "1048576"}
        mock_response.status_code = 200
        mock_response.url = "https://zenodo.org/test"

        with patch("metahq_cli.setup.downloader.requests.head") as mock_head:
            mock_head.return_value = mock_response

            verbose_downloader.get_stats()

            verbose_downloader.logger.info.assert_called()
            verbose_downloader.logger.debug.assert_called()

    # ========================================
    # ======  download tests
    # ========================================

    def test_download_with_progress_called_when_use_progress_true(self, downloader):
        """Test _download calls _download_with_progress when _use_progress is True."""
        downloader._use_progress = True

        with (
            patch.object(downloader, "_download_with_progress") as mock_with_progress,
            patch.object(downloader, "_download_no_progress") as mock_no_progress,
        ):
            downloader._download()

            mock_with_progress.assert_called_once()
            mock_no_progress.assert_not_called()

    def test_download_no_progress_called_when_use_progress_false(self, downloader):
        """Test _download calls _download_no_progress when _use_progress is False."""
        downloader._use_progress = False

        with (
            patch.object(downloader, "_download_with_progress") as mock_with_progress,
            patch.object(downloader, "_download_no_progress") as mock_no_progress,
        ):
            downloader._download()

            mock_no_progress.assert_called_once()
            mock_with_progress.assert_not_called()

    def test_download_no_progress_writes_chunks(self, downloader):
        """Test _download_no_progress writes data to file."""
        mock_response = Mock()
        mock_response.iter_content.return_value = [b"chunk1", b"chunk2"]

        with patch("metahq_cli.setup.downloader.requests.get") as mock_get:
            mock_get.return_value = mock_response

            downloader._download_no_progress()

            assert downloader.config.outfile.exists()
            content = downloader.config.outfile.read_bytes()
            assert content == b"chunk1chunk2"

    def test_download_with_progress_uses_progress_bar(self, downloader):
        """Test _download_with_progress uses progress bar."""
        mock_response = Mock()
        mock_response.iter_content.return_value = [b"chunk1", b"chunk2"]
        downloader.config.filesize = 12

        with (
            patch("metahq_cli.setup.downloader.requests.get") as mock_get,
            patch("metahq_cli.setup.downloader.progress_bar") as mock_progress_bar,
        ):
            mock_get.return_value = mock_response
            mock_progress = MagicMock()
            mock_progress_bar.return_value.__enter__ = Mock(return_value=mock_progress)
            mock_progress_bar.return_value.__exit__ = Mock(return_value=False)
            mock_progress.add_task.return_value = 1

            downloader._download_with_progress()

            mock_progress_bar.assert_called_once_with(padding="    ")
            mock_progress.add_task.assert_called_once()

    # ========================================
    # ======  get method error handling tests
    # ========================================

    def test_get_handles_connection_error(self, downloader):
        """Test get handles ConnectionError correctly."""
        with (
            patch.object(downloader, "check_outdir_exists"),
            patch.object(
                downloader,
                "get_stats",
                side_effect=requests.exceptions.ConnectionError(),
            ),
            patch.object(downloader, "_raise_connection_error") as mock_raise,
        ):
            mock_raise.side_effect = SystemExit(1)

            with pytest.raises(SystemExit):
                downloader.get()

            mock_raise.assert_called_once()

    def test_get_handles_timeout_error(self, downloader):
        """Test get handles Timeout error correctly."""
        with (
            patch.object(downloader, "check_outdir_exists"),
            patch.object(
                downloader, "get_stats", side_effect=requests.exceptions.Timeout()
            ),
            patch.object(downloader, "_raise_timeout_error") as mock_raise,
        ):
            mock_raise.side_effect = SystemExit(1)

            with pytest.raises(SystemExit):
                downloader.get()

            mock_raise.assert_called_once()

    def test_get_handles_404_error(self, downloader):
        """Test get handles 404 HTTPError correctly."""
        mock_response = Mock()
        mock_response.status_code = 404
        http_error = requests.exceptions.HTTPError(response=mock_response)

        with (
            patch.object(downloader, "check_outdir_exists"),
            patch.object(downloader, "get_stats", side_effect=http_error),
            patch.object(downloader, "_raise_404_error") as mock_raise,
        ):
            mock_raise.side_effect = SystemExit(1)

            with pytest.raises(SystemExit):
                downloader.get()

            mock_raise.assert_called_once()

    def test_get_handles_403_error(self, downloader):
        """Test get handles 403 HTTPError correctly."""
        mock_response = Mock()
        mock_response.status_code = 403
        http_error = requests.exceptions.HTTPError(response=mock_response)

        with (
            patch.object(downloader, "check_outdir_exists"),
            patch.object(downloader, "get_stats", side_effect=http_error),
            patch.object(downloader, "_raise_403_error") as mock_raise,
        ):
            mock_raise.side_effect = SystemExit(1)

            with pytest.raises(SystemExit):
                downloader.get()

            mock_raise.assert_called_once()

    def test_get_handles_other_http_error(self, downloader):
        """Test get handles other HTTPError correctly."""
        mock_response = Mock()
        mock_response.status_code = 500
        http_error = requests.exceptions.HTTPError(response=mock_response)

        with (
            patch.object(downloader, "check_outdir_exists"),
            patch.object(downloader, "get_stats", side_effect=http_error),
            patch.object(downloader, "_raise_http_error") as mock_raise,
        ):
            mock_raise.side_effect = SystemExit(1)

            with pytest.raises(SystemExit):
                downloader.get()

            mock_raise.assert_called_once_with(http_error)

    def test_get_handles_permission_error(self, downloader):
        """Test get handles PermissionError correctly."""
        with (
            patch.object(downloader, "check_outdir_exists"),
            patch.object(downloader, "get_stats", side_effect=PermissionError()),
            patch.object(downloader, "_raise_permissions_error") as mock_raise,
        ):
            mock_raise.side_effect = SystemExit(1)

            with pytest.raises(SystemExit):
                downloader.get()

            mock_raise.assert_called_once()

    def test_get_handles_keyboard_interrupt(self, downloader):
        """Test get handles KeyboardInterrupt correctly."""
        with (
            patch.object(downloader, "check_outdir_exists"),
            patch.object(downloader, "get_stats", side_effect=KeyboardInterrupt()),
            patch.object(downloader, "_raise_keyboard_interrupt") as mock_raise,
        ):
            mock_raise.side_effect = SystemExit(130)

            with pytest.raises(SystemExit):
                downloader.get()

            mock_raise.assert_called_once()

    def test_get_handles_general_exception(self, downloader):
        """Test get handles general Exception correctly."""
        general_exception = Exception("Something went wrong")

        with (
            patch.object(downloader, "check_outdir_exists"),
            patch.object(downloader, "get_stats", side_effect=general_exception),
            patch.object(downloader, "_raise_general_exception") as mock_raise,
        ):
            mock_raise.side_effect = SystemExit(1)

            with pytest.raises(SystemExit):
                downloader.get()

            mock_raise.assert_called_once_with(general_exception)

    # ========================================
    # ======  error message tests
    # ========================================

    def test_raise_connection_error(self, downloader):
        """Test _raise_connection_error logs and exits."""
        with pytest.raises(SystemExit) as exc_info:
            downloader._raise_connection_error()

        assert exc_info.value.code == 1
        downloader.logger.error.assert_called_once()

    def test_raise_timeout_error(self, downloader):
        """Test _raise_timeout_error logs and exits."""
        with pytest.raises(SystemExit) as exc_info:
            downloader._raise_timeout_error()

        assert exc_info.value.code == 1
        downloader.logger.error.assert_called_once()

    def test_raise_403_error(self, downloader):
        """Test _raise_403_error logs and exits."""
        with pytest.raises(SystemExit) as exc_info:
            downloader._raise_403_error()

        assert exc_info.value.code == 1
        downloader.logger.error.assert_called_once()

    def test_raise_404_error(self, downloader):
        """Test _raise_404_error logs and exits."""
        with pytest.raises(SystemExit) as exc_info:
            downloader._raise_404_error()

        assert exc_info.value.code == 1
        downloader.logger.error.assert_called_once()

    def test_raise_http_error(self, downloader):
        """Test _raise_http_error logs and exits."""
        mock_response = Mock()
        mock_response.status_code = 500
        http_error = requests.exceptions.HTTPError(response=mock_response)

        with pytest.raises(SystemExit) as exc_info:
            downloader._raise_http_error(http_error)

        assert exc_info.value.code == 1
        downloader.logger.error.assert_called_once()

    def test_raise_permissions_error(self, downloader):
        """Test _raise_permissions_error logs and exits."""
        with pytest.raises(SystemExit) as exc_info:
            downloader._raise_permissions_error()

        assert exc_info.value.code == 1
        downloader.logger.error.assert_called_once()

    def test_raise_general_exception(self, downloader):
        """Test _raise_general_exception logs and exits."""
        exception = Exception("test error")

        with pytest.raises(SystemExit) as exc_info:
            downloader._raise_general_exception(exception)

        assert exc_info.value.code == 1
        downloader.logger.error.assert_called_once()

    def test_raise_keyboard_interrupt_removes_partial_download(self, downloader):
        """Test _raise_keyboard_interrupt removes partial download if exists."""
        # Create a partial download file
        partial_file = downloader.config.outfile
        partial_file.write_text("partial content")

        with pytest.raises(SystemExit) as exc_info:
            downloader._raise_keyboard_interrupt()

        assert exc_info.value.code == 130
        assert not partial_file.exists()
        downloader.logger.error.assert_called()
        downloader.logger.info.assert_called()

    def test_raise_keyboard_interrupt_no_partial_file(self, downloader):
        """Test _raise_keyboard_interrupt handles case when no partial file exists."""
        with pytest.raises(SystemExit) as exc_info:
            downloader._raise_keyboard_interrupt()

        assert exc_info.value.code == 130
        downloader.logger.error.assert_called_once()

    # ========================================
    # ======  extract tests
    # ========================================

    def test_extract_calls_internal_extract(self, verbose_downloader):
        """Test extract calls _extract and _move_tar_contents."""
        with (
            patch.object(verbose_downloader, "_extract") as mock_extract,
            patch.object(
                verbose_downloader, "_move_tar_contents"
            ) as mock_move_tar_contents,
        ):
            verbose_downloader.extract()

            mock_extract.assert_called_once()
            mock_move_tar_contents.assert_called_once()
            verbose_downloader.logger.info.assert_called()

    def test_extract_tar_file(self, downloader, tmp_path):
        """Test _extract extracts tar archive correctly."""
        # Create a test tar file
        tar_content_dir = tmp_path / "tar_content"
        tar_content_dir.mkdir()
        test_file = tar_content_dir / "test.txt"
        test_file.write_text("test content")

        tar_path = tmp_path / filename_cur
        with tarfile.open(tar_path, "w:gz") as tar:
            tar.add(test_file, arcname="test.txt")

        downloader._extract()

        extracted_file = tmp_path / "test.txt"
        assert extracted_file.exists()
        assert extracted_file.read_text() == "test content"

    def test_move_tar_contents(self, verbose_downloader, tmp_path):
        """Test _move_tar_contents moves contents and cleans up."""
        # Create tar directory structure
        tar_dir = tmp_path / "metahq"
        tar_dir.mkdir()
        (tar_dir / "file1.txt").write_text("content1")
        (tar_dir / "file2.txt").write_text("content2")

        # Create fake tar file
        tar_file = tmp_path / filename_cur
        tar_file.write_text("fake tar")

        verbose_downloader._move_tar_contents(base_dir=tmp_path, tar_dir=tar_dir)

        # Check files were moved
        assert (tmp_path / "file1.txt").exists()
        assert (tmp_path / "file2.txt").exists()

        # Check tar dir was removed
        assert not tar_dir.exists()

        # Check tar file was removed
        assert not tar_file.exists()

    def test_extract_logs_when_verbose(self, verbose_downloader):
        """Test extract logs information in verbose mode."""
        with (
            patch.object(verbose_downloader, "_extract"),
            patch.object(verbose_downloader, "_move_tar_contents"),
        ):
            verbose_downloader.extract()

            verbose_downloader.logger.info.assert_called()
