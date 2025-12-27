"""
Unit tests for AzureBlobPath - pathlib-style interface for Azure Blob Storage.

Tests the Path-like interface for navigating and manipulating blobs, including:
- Path construction and navigation
- File operations (read, write, delete)
- Directory operations (iterdir, glob, mkdir)
- Blob operations (copy, move, rename)
- Metadata and properties
"""

import pytest
from unittest.mock import Mock, MagicMock, patch, call
from flowlet.storage.azure_path import AzureBlobPath
from flowlet.storage.azure import AzureBlobFile


@pytest.fixture
def mock_blob_service_client():
    """Create a mock BlobServiceClient with container and blob clients."""
    mock_service = Mock()
    mock_container = Mock()
    mock_blob = Mock()

    mock_service.get_container_client.return_value = mock_container
    mock_container.get_blob_client.return_value = mock_blob

    return mock_service, mock_container, mock_blob


class TestAzureBlobPathInit:
    """Test path initialization and creation."""

    def test_init_with_service_client(self, mock_blob_service_client):
        """Test initialization with BlobServiceClient."""
        mock_service, _, _ = mock_blob_service_client

        path = AzureBlobPath(
            container_name='container',
            blob_path='data/file.txt',
            blob_service_client=mock_service
        )

        assert path.container_name == 'container'
        assert path._blob_path == 'data/file.txt'
        assert path._blob_service_client is mock_service

    @patch('flowlet.persistence.azure_path.BlobServiceClient')
    def test_init_with_connection_string(self, mock_service_class):
        """Test initialization with connection string."""
        mock_service = Mock()
        mock_service_class.from_connection_string.return_value = mock_service

        path = AzureBlobPath(
            container_name='container',
            blob_path='data/file.txt',
            connection_string='DefaultEndpointsProtocol=https;...'
        )

        assert path.container_name == 'container'
        assert path._connection_string is not None
        mock_service_class.from_connection_string.assert_called_once()

    def test_from_connection_string(self):
        """Test from_connection_string class method."""
        with patch('flowlet.persistence.azure_path.BlobServiceClient'):
            path = AzureBlobPath.from_connection_string(
                connection_string='test',
                container='container',
                path='data/file.txt'
            )

            assert path.container_name == 'container'
            assert path._blob_path == 'data/file.txt'

    def test_from_service_client(self, mock_blob_service_client):
        """Test from_service_client class method."""
        mock_service, _, _ = mock_blob_service_client

        path = AzureBlobPath.from_service_client(
            blob_service_client=mock_service,
            container='container',
            path='data/file.txt'
        )

        assert path.container_name == 'container'
        assert path._blob_path == 'data/file.txt'
        assert path._blob_service_client is mock_service

    def test_path_normalization(self, mock_blob_service_client):
        """Test that paths are normalized (leading/trailing slashes removed)."""
        mock_service, _, _ = mock_blob_service_client

        path = AzureBlobPath(
            container_name='container',
            blob_path='/data/file.txt/',
            blob_service_client=mock_service
        )

        assert path._blob_path == 'data/file.txt'


class TestAzureBlobPathNavigation:
    """Test path navigation and construction."""

    def test_truediv_operator(self, mock_blob_service_client):
        """Test / operator for path joining."""
        mock_service, _, _ = mock_blob_service_client

        root = AzureBlobPath.from_service_client(mock_service, 'container')
        data = root / 'data'
        file = data / 'file.txt'

        assert file._blob_path == 'data/file.txt'
        assert file.container_name == 'container'

    def test_truediv_multiple_levels(self, mock_blob_service_client):
        """Test joining multiple path levels."""
        mock_service, _, _ = mock_blob_service_client

        root = AzureBlobPath.from_service_client(mock_service, 'container')
        path = root / 'reports' / '2024' / '12' / 'summary.pdf'

        assert path._blob_path == 'reports/2024/12/summary.pdf'

    def test_parent_property(self, mock_blob_service_client):
        """Test parent property."""
        mock_service, _, _ = mock_blob_service_client

        path = AzureBlobPath.from_service_client(
            mock_service,
            'container',
            'data/2024/file.txt'
        )

        parent = path.parent
        assert parent._blob_path == 'data/2024'

        grandparent = parent.parent
        assert grandparent._blob_path == 'data'

    def test_parent_of_root(self, mock_blob_service_client):
        """Test parent of container root."""
        mock_service, _, _ = mock_blob_service_client

        path = AzureBlobPath.from_service_client(mock_service, 'container', 'file.txt')
        parent = path.parent

        assert parent._blob_path == ''

    def test_name_property(self, mock_blob_service_client):
        """Test name property (final component)."""
        mock_service, _, _ = mock_blob_service_client

        path = AzureBlobPath.from_service_client(
            mock_service,
            'container',
            'data/file.txt'
        )

        assert path.name == 'file.txt'

    def test_parts_property(self, mock_blob_service_client):
        """Test parts property."""
        mock_service, _, _ = mock_blob_service_client

        path = AzureBlobPath.from_service_client(
            mock_service,
            'container',
            'data/2024/file.txt'
        )

        assert path.parts == ('container', 'data', '2024', 'file.txt')

    def test_suffix_property(self, mock_blob_service_client):
        """Test suffix property (file extension)."""
        mock_service, _, _ = mock_blob_service_client

        path = AzureBlobPath.from_service_client(
            mock_service,
            'container',
            'data/file.txt'
        )

        assert path.suffix == '.txt'

        path_no_ext = AzureBlobPath.from_service_client(
            mock_service,
            'container',
            'data/file'
        )
        assert path_no_ext.suffix == ''

    def test_stem_property(self, mock_blob_service_client):
        """Test stem property (filename without extension)."""
        mock_service, _, _ = mock_blob_service_client

        path = AzureBlobPath.from_service_client(
            mock_service,
            'container',
            'data/report.pdf'
        )

        assert path.stem == 'report'


class TestAzureBlobPathStringRepresentation:
    """Test string representations of paths."""

    def test_str(self, mock_blob_service_client):
        """Test __str__ representation."""
        mock_service, _, _ = mock_blob_service_client

        path = AzureBlobPath.from_service_client(
            mock_service,
            'container',
            'data/file.txt'
        )

        assert str(path) == 'az://container/data/file.txt'

    def test_repr(self, mock_blob_service_client):
        """Test __repr__ representation."""
        mock_service, _, _ = mock_blob_service_client

        path = AzureBlobPath.from_service_client(
            mock_service,
            'container',
            'data/file.txt'
        )

        assert 'AzureBlobPath' in repr(path)
        assert 'container' in repr(path)
        assert 'data/file.txt' in repr(path)

    def test_as_posix(self, mock_blob_service_client):
        """Test as_posix() method."""
        mock_service, _, _ = mock_blob_service_client

        path = AzureBlobPath.from_service_client(
            mock_service,
            'container',
            'data/file.txt'
        )

        assert path.as_posix() == 'container/data/file.txt'


class TestAzureBlobPathEquality:
    """Test path equality and hashing."""

    def test_equality(self, mock_blob_service_client):
        """Test path equality."""
        mock_service, _, _ = mock_blob_service_client

        path1 = AzureBlobPath.from_service_client(
            mock_service,
            'container',
            'data/file.txt'
        )
        path2 = AzureBlobPath.from_service_client(
            mock_service,
            'container',
            'data/file.txt'
        )

        assert path1 == path2

    def test_inequality_different_path(self, mock_blob_service_client):
        """Test inequality for different paths."""
        mock_service, _, _ = mock_blob_service_client

        path1 = AzureBlobPath.from_service_client(
            mock_service,
            'container',
            'data/file1.txt'
        )
        path2 = AzureBlobPath.from_service_client(
            mock_service,
            'container',
            'data/file2.txt'
        )

        assert path1 != path2

    def test_hashable(self, mock_blob_service_client):
        """Test that paths are hashable."""
        mock_service, _, _ = mock_blob_service_client

        path = AzureBlobPath.from_service_client(
            mock_service,
            'container',
            'data/file.txt'
        )

        # Should be usable in sets and dicts
        path_set = {path}
        assert path in path_set


class TestAzureBlobPathExistence:
    """Test existence checks."""

    def test_exists_blob(self, mock_blob_service_client):
        """Test exists() for a blob."""
        mock_service, _, mock_blob = mock_blob_service_client

        mock_blob.get_blob_properties.return_value = Mock()

        path = AzureBlobPath.from_service_client(
            mock_service,
            'container',
            'file.txt'
        )

        assert path.exists() is True
        mock_blob.get_blob_properties.assert_called_once()

    def test_exists_blob_not_found(self, mock_blob_service_client):
        """Test exists() for non-existent blob."""
        mock_service, _, mock_blob = mock_blob_service_client

        mock_blob.get_blob_properties.side_effect = Exception('BlobNotFound')

        path = AzureBlobPath.from_service_client(
            mock_service,
            'container',
            'file.txt'
        )

        assert path.exists() is False

    def test_is_file(self, mock_blob_service_client):
        """Test is_file() returns True for existing blob."""
        mock_service, _, mock_blob = mock_blob_service_client

        mock_blob.get_blob_properties.return_value = Mock()

        path = AzureBlobPath.from_service_client(
            mock_service,
            'container',
            'file.txt'
        )

        assert path.is_file() is True

    def test_is_dir_with_blobs(self, mock_blob_service_client):
        """Test is_dir() returns True if blobs exist under prefix."""
        mock_service, mock_container, _ = mock_blob_service_client

        # Mock list_blobs to return at least one blob
        mock_blob_item = Mock()
        mock_container.list_blobs.return_value = iter([mock_blob_item])

        path = AzureBlobPath.from_service_client(
            mock_service,
            'container',
            'data'
        )

        assert path.is_dir() is True

    def test_is_dir_no_blobs(self, mock_blob_service_client):
        """Test is_dir() returns False if no blobs exist under prefix."""
        mock_service, mock_container, _ = mock_blob_service_client

        mock_container.list_blobs.return_value = iter([])

        path = AzureBlobPath.from_service_client(
            mock_service,
            'container',
            'data'
        )

        assert path.is_dir() is False


class TestAzureBlobPathFileOperations:
    """Test file I/O operations."""

    def test_read_text(self, mock_blob_service_client):
        """Test read_text() method."""
        mock_service, _, mock_blob = mock_blob_service_client

        # Mock blob properties and download
        mock_properties = Mock()
        mock_properties.size = 13
        mock_blob.get_blob_properties.return_value = mock_properties

        mock_download = Mock()
        mock_download.readall.return_value = b'Hello, Azure!'
        mock_blob.download_blob.return_value = mock_download

        path = AzureBlobPath.from_service_client(
            mock_service,
            'container',
            'file.txt'
        )

        content = path.read_text()
        assert content == 'Hello, Azure!'

    def test_read_bytes(self, mock_blob_service_client):
        """Test read_bytes() method."""
        mock_service, _, mock_blob = mock_blob_service_client

        # Mock blob properties and download
        mock_properties = Mock()
        mock_properties.size = 4
        mock_blob.get_blob_properties.return_value = mock_properties

        mock_download = Mock()
        mock_download.readall.return_value = b'\x00\x01\x02\x03'
        mock_blob.download_blob.return_value = mock_download

        path = AzureBlobPath.from_service_client(
            mock_service,
            'container',
            'file.bin'
        )

        content = path.read_bytes()
        assert content == b'\x00\x01\x02\x03'

    def test_write_text(self, mock_blob_service_client):
        """Test write_text() method."""
        mock_service, mock_container, mock_blob = mock_blob_service_client

        path = AzureBlobPath.from_service_client(
            mock_service,
            'container',
            'file.txt'
        )

        bytes_written = path.write_text('Hello, Azure!')

        assert bytes_written == 13
        mock_blob.upload_blob.assert_called_once()

    def test_write_bytes(self, mock_blob_service_client):
        """Test write_bytes() method."""
        mock_service, _, mock_blob = mock_blob_service_client

        path = AzureBlobPath.from_service_client(
            mock_service,
            'container',
            'file.bin'
        )

        bytes_written = path.write_bytes(b'\x00\x01\x02\x03')

        assert bytes_written == 4
        mock_blob.upload_blob.assert_called_once()

    def test_open_returns_azure_blob_file(self, mock_blob_service_client):
        """Test that open() returns AzureBlobFile."""
        mock_service, _, _ = mock_blob_service_client

        path = AzureBlobPath.from_service_client(
            mock_service,
            'container',
            'file.txt'
        )

        with patch('flowlet.persistence.azure_path.AzureBlobFile') as mock_file_class:
            mock_file = Mock()
            mock_file_class.return_value = mock_file

            file = path.open('w')

            mock_file_class.assert_called_once()
            assert file is mock_file


class TestAzureBlobPathBlobOperations:
    """Test blob-specific operations."""

    def test_delete(self, mock_blob_service_client):
        """Test delete() method."""
        mock_service, _, mock_blob = mock_blob_service_client

        path = AzureBlobPath.from_service_client(
            mock_service,
            'container',
            'file.txt'
        )

        path.delete()
        mock_blob.delete_blob.assert_called_once()

    def test_delete_missing_raises(self, mock_blob_service_client):
        """Test delete() raises FileNotFoundError for missing blob."""
        mock_service, _, mock_blob = mock_blob_service_client

        mock_blob.delete_blob.side_effect = Exception('BlobNotFound')

        path = AzureBlobPath.from_service_client(
            mock_service,
            'container',
            'file.txt'
        )

        with pytest.raises(FileNotFoundError):
            path.delete(missing_ok=False)

    def test_delete_missing_ok(self, mock_blob_service_client):
        """Test delete() with missing_ok=True doesn't raise."""
        mock_service, _, mock_blob = mock_blob_service_client

        mock_blob.delete_blob.side_effect = Exception('BlobNotFound')

        path = AzureBlobPath.from_service_client(
            mock_service,
            'container',
            'file.txt'
        )

        # Should not raise
        path.delete(missing_ok=True)

    def test_unlink_alias(self, mock_blob_service_client):
        """Test unlink() is an alias for delete()."""
        mock_service, _, mock_blob = mock_blob_service_client

        path = AzureBlobPath.from_service_client(
            mock_service,
            'container',
            'file.txt'
        )

        path.unlink()
        mock_blob.delete_blob.assert_called_once()

    def test_copy_to(self, mock_blob_service_client):
        """Test copy_to() method."""
        mock_service, _, mock_blob = mock_blob_service_client

        source = AzureBlobPath.from_service_client(
            mock_service,
            'container',
            'source.txt'
        )
        dest = AzureBlobPath.from_service_client(
            mock_service,
            'container',
            'dest.txt'
        )

        mock_blob.url = 'https://account.blob.core.windows.net/container/source.txt'

        result = source.copy_to(dest)

        assert result == dest
        # Destination blob client should call start_copy_from_url
        mock_service.get_container_client.return_value.get_blob_client.return_value.start_copy_from_url.assert_called()

    def test_move_to(self, mock_blob_service_client):
        """Test move_to() method (copy + delete)."""
        mock_service, _, mock_blob = mock_blob_service_client

        source = AzureBlobPath.from_service_client(
            mock_service,
            'container',
            'source.txt'
        )
        dest = AzureBlobPath.from_service_client(
            mock_service,
            'container',
            'dest.txt'
        )

        mock_blob.url = 'https://account.blob.core.windows.net/container/source.txt'

        result = source.move_to(dest)

        assert result == dest
        # Should delete source after copy
        mock_blob.delete_blob.assert_called_once()

    def test_rename(self, mock_blob_service_client):
        """Test rename() method."""
        mock_service, _, mock_blob = mock_blob_service_client

        path = AzureBlobPath.from_service_client(
            mock_service,
            'container',
            'data/old_name.txt'
        )

        mock_blob.url = 'https://account.blob.core.windows.net/container/data/old_name.txt'

        new_path = path.rename('new_name.txt')

        assert new_path._blob_path == 'data/new_name.txt'


class TestAzureBlobPathListing:
    """Test directory listing operations."""

    def test_iterdir_non_recursive(self, mock_blob_service_client):
        """Test iterdir() non-recursive listing."""
        mock_service, mock_container, _ = mock_blob_service_client

        # Mock blobs
        mock_blob1 = Mock()
        mock_blob1.name = 'data/file1.txt'
        mock_blob2 = Mock()
        mock_blob2.name = 'data/file2.txt'
        mock_blob3 = Mock()
        mock_blob3.name = 'data/subdir/file3.txt'

        mock_container.list_blobs.return_value = [mock_blob1, mock_blob2, mock_blob3]

        path = AzureBlobPath.from_service_client(
            mock_service,
            'container',
            'data'
        )

        children = list(path.iterdir(recursive=False))

        # Should return file1, file2, and subdir (not file3 directly)
        assert len(children) >= 2

    def test_iterdir_recursive(self, mock_blob_service_client):
        """Test iterdir() recursive listing."""
        mock_service, mock_container, _ = mock_blob_service_client

        # Mock blobs
        mock_blob1 = Mock()
        mock_blob1.name = 'data/file1.txt'
        mock_blob2 = Mock()
        mock_blob2.name = 'data/subdir/file2.txt'

        mock_container.list_blobs.return_value = [mock_blob1, mock_blob2]

        path = AzureBlobPath.from_service_client(
            mock_service,
            'container',
            'data'
        )

        children = list(path.iterdir(recursive=True))

        assert len(children) == 2
        assert any(c._blob_path == 'data/file1.txt' for c in children)
        assert any(c._blob_path == 'data/subdir/file2.txt' for c in children)

    def test_glob(self, mock_blob_service_client):
        """Test glob() pattern matching."""
        mock_service, mock_container, _ = mock_blob_service_client

        # Mock blobs
        mock_blob1 = Mock()
        mock_blob1.name = 'data/file1.txt'
        mock_blob2 = Mock()
        mock_blob2.name = 'data/file2.json'
        mock_blob3 = Mock()
        mock_blob3.name = 'data/subdir/file3.txt'

        mock_container.list_blobs.return_value = [mock_blob1, mock_blob2, mock_blob3]

        path = AzureBlobPath.from_service_client(
            mock_service,
            'container',
            'data'
        )

        # Glob for .txt files
        txt_files = list(path.glob('*.txt'))

        assert len(txt_files) == 1
        assert txt_files[0]._blob_path == 'data/file1.txt'


class TestAzureBlobPathMetadata:
    """Test metadata and properties operations."""

    def test_stat(self, mock_blob_service_client):
        """Test stat() returns blob properties."""
        mock_service, _, mock_blob = mock_blob_service_client

        mock_properties = Mock()
        mock_properties.size = 1024
        mock_blob.get_blob_properties.return_value = mock_properties

        path = AzureBlobPath.from_service_client(
            mock_service,
            'container',
            'file.txt'
        )

        props = path.stat()
        assert props == mock_properties
        assert props.size == 1024

    def test_get_size(self, mock_blob_service_client):
        """Test get_size() method."""
        mock_service, _, mock_blob = mock_blob_service_client

        mock_properties = Mock()
        mock_properties.size = 2048
        mock_blob.get_blob_properties.return_value = mock_properties

        path = AzureBlobPath.from_service_client(
            mock_service,
            'container',
            'file.txt'
        )

        assert path.get_size() == 2048

    def test_get_set_metadata(self, mock_blob_service_client):
        """Test get_metadata() and set_metadata() methods."""
        mock_service, _, mock_blob = mock_blob_service_client

        mock_properties = Mock()
        mock_properties.metadata = {'key': 'value'}
        mock_blob.get_blob_properties.return_value = mock_properties

        path = AzureBlobPath.from_service_client(
            mock_service,
            'container',
            'file.txt'
        )

        # Get metadata
        metadata = path.get_metadata()
        assert metadata == {'key': 'value'}

        # Set metadata
        new_metadata = {'author': 'test', 'version': '1.0'}
        path.set_metadata(new_metadata)

        mock_blob.set_blob_metadata.assert_called_once_with(new_metadata)

    def test_get_set_content_type(self, mock_blob_service_client):
        """Test get_content_type() and set_content_type() methods."""
        mock_service, _, mock_blob = mock_blob_service_client

        mock_properties = Mock()
        mock_content_settings = Mock()
        mock_content_settings.content_type = 'application/pdf'
        mock_properties.content_settings = mock_content_settings
        mock_blob.get_blob_properties.return_value = mock_properties

        path = AzureBlobPath.from_service_client(
            mock_service,
            'container',
            'document.pdf'
        )

        # Get content type
        assert path.get_content_type() == 'application/pdf'

        # Set content type
        with patch('flowlet.persistence.azure_path.ContentSettings'):
            path.set_content_type('application/json')
            mock_blob.set_http_headers.assert_called_once()


class TestAzureBlobPathDirectoryOperations:
    """Test directory-like operations."""

    def test_mkdir_creates_marker(self, mock_blob_service_client):
        """Test mkdir() creates a marker blob."""
        mock_service, mock_container, _ = mock_blob_service_client

        path = AzureBlobPath.from_service_client(
            mock_service,
            'container',
            'data/logs'
        )

        path.mkdir()

        # Should get blob client for marker
        mock_container.get_blob_client.assert_called()

    def test_rmdir_recursive(self, mock_blob_service_client):
        """Test rmdir() with recursive=True."""
        mock_service, mock_container, _ = mock_blob_service_client

        # Mock blobs under directory
        mock_blob1 = Mock()
        mock_blob1.name = 'data/logs/file1.txt'
        mock_blob2 = Mock()
        mock_blob2.name = 'data/logs/file2.txt'

        mock_container.list_blobs.return_value = [mock_blob1, mock_blob2]

        path = AzureBlobPath.from_service_client(
            mock_service,
            'container',
            'data/logs'
        )

        path.rmdir(recursive=True)

        # Should delete all blobs under prefix
        assert mock_container.delete_blob.call_count >= 2
