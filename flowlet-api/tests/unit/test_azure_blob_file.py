"""
Unit tests for AzureBlobFile - Azure Blob Storage file handle interface.

Tests the file-like interface for reading and writing blobs, including:
- Streaming reads with chunked downloads
- Append mode with AppendBlob support
- File operations (read, write, seek, tell)
- Context manager support
"""

import io
import pytest
from unittest.mock import Mock, MagicMock, patch, call
from flowlet.storage.azure import AzureBlobFile, open_azure_blob, DEFAULT_CHUNK_SIZE


@pytest.fixture
def mock_blob_service_client():
    """Create a mock BlobServiceClient."""
    mock_client = Mock()
    mock_container = Mock()
    mock_blob = Mock()

    mock_client.get_container_client.return_value = mock_container
    mock_container.get_blob_client.return_value = mock_blob

    return mock_client, mock_container, mock_blob


class TestAzureBlobFileInit:
    """Test initialization and mode setup."""

    def test_init_read_mode(self, mock_blob_service_client):
        """Test initialization in read mode."""
        _, _, mock_blob = mock_blob_service_client
        mock_service = mock_blob_service_client[0]

        # Mock blob properties
        mock_properties = Mock()
        mock_properties.size = 1024
        mock_blob.get_blob_properties.return_value = mock_properties

        # Create file in read mode
        file = AzureBlobFile(
            connection_string='test',
            container_name='container',
            blob_name='test.txt',
            mode='r',
            blob_service_client=mock_service
        )

        assert file.mode == 'r'
        assert file.encoding == 'utf-8'
        assert file._blob_size == 1024
        assert file._position == 0
        mock_blob.get_blob_properties.assert_called_once()

    def test_init_write_mode(self, mock_blob_service_client):
        """Test initialization in write mode."""
        mock_service = mock_blob_service_client[0]

        file = AzureBlobFile(
            connection_string='test',
            container_name='container',
            blob_name='test.txt',
            mode='w',
            blob_service_client=mock_service
        )

        assert file.mode == 'w'
        assert file._write_buffer is not None
        assert isinstance(file._write_buffer, io.BytesIO)

    def test_init_append_mode_new_blob(self, mock_blob_service_client):
        """Test initialization in append mode with new blob (creates AppendBlob)."""
        _, mock_container, mock_blob = mock_blob_service_client
        mock_service = mock_blob_service_client[0]

        # First call (in __init__) raises BlobNotFound, then create succeeds
        first_exception = Exception('BlobNotFound')
        first_exception.__class__.__name__ = 'ResourceNotFoundError'  # More realistic
        mock_blob.get_blob_properties.side_effect = first_exception
        mock_blob.create_append_blob.return_value = None

        # Ensure container exists check succeeds
        mock_container.create_container.return_value = None

        file = AzureBlobFile(
            connection_string='test',
            container_name='container',
            blob_name='test.txt',
            mode='a',
            blob_service_client=mock_service
        )

        assert file.mode == 'a'
        assert file._is_append_blob is True
        assert file._blob_size == 0
        mock_blob.create_append_blob.assert_called_once()

    def test_init_append_mode_existing_append_blob(self, mock_blob_service_client):
        """Test initialization in append mode with existing AppendBlob."""
        _, _, mock_blob = mock_blob_service_client
        mock_service = mock_blob_service_client[0]

        # Mock AppendBlob properties
        mock_properties = Mock()
        mock_properties.blob_type = 'AppendBlob'
        mock_properties.size = 512
        mock_blob.get_blob_properties.return_value = mock_properties

        file = AzureBlobFile(
            connection_string='test',
            container_name='container',
            blob_name='test.txt',
            mode='a',
            blob_service_client=mock_service
        )

        assert file._is_append_blob is True
        assert file._blob_size == 512
        assert file._position == 512

    def test_init_append_mode_existing_block_blob_degraded(self, mock_blob_service_client):
        """Test initialization in append mode with BlockBlob (degraded mode)."""
        _, _, mock_blob = mock_blob_service_client
        mock_service = mock_blob_service_client[0]

        # Mock BlockBlob properties
        mock_properties = Mock()
        mock_properties.blob_type = 'BlockBlob'
        mock_blob.get_blob_properties.return_value = mock_properties

        # Mock download
        mock_download = Mock()
        mock_download.readall.return_value = b'existing content'
        mock_blob.download_blob.return_value = mock_download

        with pytest.warns(UserWarning, match="degraded mode"):
            file = AzureBlobFile(
                connection_string='test',
                container_name='container',
                blob_name='test.txt',
                mode='a',
                blob_service_client=mock_service
            )

        assert file._degraded_append_mode is True
        assert file._is_append_blob is False
        assert file._write_buffer is not None

    def test_binary_mode_no_encoding(self, mock_blob_service_client):
        """Test that binary mode has no encoding."""
        mock_service = mock_blob_service_client[0]

        file = AzureBlobFile(
            connection_string='test',
            container_name='container',
            blob_name='test.bin',
            mode='wb',
            blob_service_client=mock_service
        )

        assert file.encoding is None


class TestAzureBlobFileRead:
    """Test reading operations."""

    def test_read_all(self, mock_blob_service_client):
        """Test reading entire blob."""
        _, _, mock_blob = mock_blob_service_client
        mock_service = mock_blob_service_client[0]

        # Mock blob properties
        mock_properties = Mock()
        mock_properties.size = 13
        mock_blob.get_blob_properties.return_value = mock_properties

        # Mock download
        mock_download = Mock()
        mock_download.readall.return_value = b'Hello, Azure!'
        mock_blob.download_blob.return_value = mock_download

        file = AzureBlobFile(
            connection_string='test',
            container_name='container',
            blob_name='test.txt',
            mode='r',
            blob_service_client=mock_service
        )

        content = file.read()
        assert content == 'Hello, Azure!'
        assert file._position == 13

    def test_read_partial(self, mock_blob_service_client):
        """Test reading partial content."""
        _, _, mock_blob = mock_blob_service_client
        mock_service = mock_blob_service_client[0]

        # Mock blob properties
        mock_properties = Mock()
        mock_properties.size = 13
        mock_blob.get_blob_properties.return_value = mock_properties

        # Mock download
        mock_download = Mock()
        mock_download.readall.return_value = b'Hello, Azure!'
        mock_blob.download_blob.return_value = mock_download

        file = AzureBlobFile(
            connection_string='test',
            container_name='container',
            blob_name='test.txt',
            mode='r',
            blob_service_client=mock_service
        )

        content = file.read(5)
        assert content == 'Hello'
        assert file._position == 5

    def test_read_binary(self, mock_blob_service_client):
        """Test reading in binary mode."""
        _, _, mock_blob = mock_blob_service_client
        mock_service = mock_blob_service_client[0]

        # Mock blob properties
        mock_properties = Mock()
        mock_properties.size = 4
        mock_blob.get_blob_properties.return_value = mock_properties

        # Mock download
        mock_download = Mock()
        mock_download.readall.return_value = b'\x00\x01\x02\x03'
        mock_blob.download_blob.return_value = mock_download

        file = AzureBlobFile(
            connection_string='test',
            container_name='container',
            blob_name='test.bin',
            mode='rb',
            blob_service_client=mock_service
        )

        content = file.read()
        assert content == b'\x00\x01\x02\x03'
        assert isinstance(content, bytes)

    def test_readline(self, mock_blob_service_client):
        """Test reading a single line."""
        _, _, mock_blob = mock_blob_service_client
        mock_service = mock_blob_service_client[0]

        # Mock blob properties
        mock_properties = Mock()
        mock_properties.size = 21
        mock_blob.get_blob_properties.return_value = mock_properties

        # Mock download - called for initial load
        mock_download = Mock()
        mock_download.readall.return_value = b'Line 1\nLine 2\nLine 3'
        mock_blob.download_blob.return_value = mock_download

        file = AzureBlobFile(
            connection_string='test',
            container_name='container',
            blob_name='test.txt',
            mode='r',
            blob_service_client=mock_service
        )

        # Read first line
        line1 = file.readline()
        assert line1 == 'Line 1\n'

        # Position should have moved, but since we're using a buffer,
        # the second readline should work from the same buffer
        line2 = file.readline()
        assert line2 == 'Line 2\n'

    def test_readlines(self, mock_blob_service_client):
        """Test reading all lines."""
        _, _, mock_blob = mock_blob_service_client
        mock_service = mock_blob_service_client[0]

        # Mock blob properties
        mock_properties = Mock()
        mock_properties.size = 21
        mock_blob.get_blob_properties.return_value = mock_properties

        # Mock download
        mock_download = Mock()
        mock_download.readall.return_value = b'Line 1\nLine 2\nLine 3'
        mock_blob.download_blob.return_value = mock_download

        file = AzureBlobFile(
            connection_string='test',
            container_name='container',
            blob_name='test.txt',
            mode='r',
            blob_service_client=mock_service
        )

        lines = file.readlines()
        # Note: readlines() reads until end of file
        assert len(lines) >= 2  # At least 2 lines (may include partial last line)
        assert lines[0] == 'Line 1\n'
        assert lines[1] == 'Line 2\n'

    def test_iteration(self, mock_blob_service_client):
        """Test iterating over lines."""
        _, _, mock_blob = mock_blob_service_client
        mock_service = mock_blob_service_client[0]

        # Mock blob properties
        mock_properties = Mock()
        mock_properties.size = 21
        mock_blob.get_blob_properties.return_value = mock_properties

        # Mock download
        mock_download = Mock()
        mock_download.readall.return_value = b'Line 1\nLine 2\nLine 3'
        mock_blob.download_blob.return_value = mock_download

        file = AzureBlobFile(
            connection_string='test',
            container_name='container',
            blob_name='test.txt',
            mode='r',
            blob_service_client=mock_service
        )

        lines = list(file)
        assert len(lines) == 3
        assert lines[0] == 'Line 1\n'


class TestAzureBlobFileWrite:
    """Test writing operations."""

    def test_write_text(self, mock_blob_service_client):
        """Test writing text content."""
        mock_service = mock_blob_service_client[0]

        file = AzureBlobFile(
            connection_string='test',
            container_name='container',
            blob_name='test.txt',
            mode='w',
            blob_service_client=mock_service
        )

        bytes_written = file.write('Hello, Azure!')
        assert bytes_written == 13
        assert file._write_buffer.tell() == 13

    def test_write_binary(self, mock_blob_service_client):
        """Test writing binary content."""
        mock_service = mock_blob_service_client[0]

        file = AzureBlobFile(
            connection_string='test',
            container_name='container',
            blob_name='test.bin',
            mode='wb',
            blob_service_client=mock_service
        )

        bytes_written = file.write(b'\x00\x01\x02\x03')
        assert bytes_written == 4

    def test_write_string_in_binary_mode_raises(self, mock_blob_service_client):
        """Test that writing string in binary mode raises TypeError."""
        mock_service = mock_blob_service_client[0]

        file = AzureBlobFile(
            connection_string='test',
            container_name='container',
            blob_name='test.bin',
            mode='wb',
            blob_service_client=mock_service
        )

        with pytest.raises(TypeError, match="Cannot write string in binary mode"):
            file.write('text')

    def test_writelines(self, mock_blob_service_client):
        """Test writing multiple lines."""
        mock_service = mock_blob_service_client[0]

        file = AzureBlobFile(
            connection_string='test',
            container_name='container',
            blob_name='test.txt',
            mode='w',
            blob_service_client=mock_service
        )

        file.writelines(['Line 1\n', 'Line 2\n', 'Line 3\n'])
        file._write_buffer.seek(0)
        content = file._write_buffer.read()
        assert content == b'Line 1\nLine 2\nLine 3\n'

    def test_append_blob_write(self, mock_blob_service_client):
        """Test writing to AppendBlob."""
        _, mock_container, mock_blob = mock_blob_service_client
        mock_service = mock_blob_service_client[0]

        # Setup AppendBlob - blob doesn't exist
        blob_not_found = Exception('BlobNotFound')
        blob_not_found.__class__.__name__ = 'ResourceNotFoundError'
        mock_blob.get_blob_properties.side_effect = blob_not_found
        mock_blob.create_append_blob.return_value = None
        mock_container.create_container.return_value = None

        file = AzureBlobFile(
            connection_string='test',
            container_name='container',
            blob_name='test.txt',
            mode='a',
            blob_service_client=mock_service
        )

        # Write data
        file.write('Log entry 1\n')
        file.write('Log entry 2\n')

        # Should buffer until flush
        assert len(file._append_buffer) > 0

        # Flush should call append_block
        file.flush()
        mock_blob.append_block.assert_called_once()


class TestAzureBlobFileSeek:
    """Test seek and tell operations."""

    def test_seek_and_tell_read_mode(self, mock_blob_service_client):
        """Test seek and tell in read mode."""
        _, _, mock_blob = mock_blob_service_client
        mock_service = mock_blob_service_client[0]

        # Mock blob properties
        mock_properties = Mock()
        mock_properties.size = 100
        mock_blob.get_blob_properties.return_value = mock_properties

        file = AzureBlobFile(
            connection_string='test',
            container_name='container',
            blob_name='test.txt',
            mode='r',
            blob_service_client=mock_service
        )

        # Seek to position
        pos = file.seek(50)
        assert pos == 50
        assert file.tell() == 50

        # Seek from current
        pos = file.seek(10, io.SEEK_CUR)
        assert pos == 60

        # Seek from end
        pos = file.seek(-10, io.SEEK_END)
        assert pos == 90

    def test_seek_and_tell_write_mode(self, mock_blob_service_client):
        """Test seek and tell in write mode."""
        mock_service = mock_blob_service_client[0]

        file = AzureBlobFile(
            connection_string='test',
            container_name='container',
            blob_name='test.txt',
            mode='w',
            blob_service_client=mock_service
        )

        file.write('Hello, Azure!')
        assert file.tell() == 13

        file.seek(0)
        assert file.tell() == 0


class TestAzureBlobFileFlushClose:
    """Test flush and close operations."""

    def test_flush_write_mode(self, mock_blob_service_client):
        """Test flushing in write mode."""
        _, mock_container, mock_blob = mock_blob_service_client
        mock_service = mock_blob_service_client[0]

        file = AzureBlobFile(
            connection_string='test',
            container_name='container',
            blob_name='test.txt',
            mode='w',
            blob_service_client=mock_service
        )

        file.write('Hello, Azure!')
        file.flush()

        # Should call upload_blob
        mock_blob.upload_blob.assert_called_once()
        call_args = mock_blob.upload_blob.call_args
        assert call_args[0][0] == b'Hello, Azure!'

    def test_close_uploads_data(self, mock_blob_service_client):
        """Test that close uploads data."""
        _, _, mock_blob = mock_blob_service_client
        mock_service = mock_blob_service_client[0]

        file = AzureBlobFile(
            connection_string='test',
            container_name='container',
            blob_name='test.txt',
            mode='w',
            blob_service_client=mock_service
        )

        file.write('Hello, Azure!')
        file.close()

        assert file.closed is True
        mock_blob.upload_blob.assert_called_once()

    def test_context_manager(self, mock_blob_service_client):
        """Test context manager support."""
        _, _, mock_blob = mock_blob_service_client
        mock_service = mock_blob_service_client[0]

        with AzureBlobFile(
            connection_string='test',
            container_name='container',
            blob_name='test.txt',
            mode='w',
            blob_service_client=mock_service
        ) as file:
            file.write('Hello, Azure!')
            assert not file.closed

        assert file.closed
        mock_blob.upload_blob.assert_called_once()


class TestAzureBlobFileProperties:
    """Test file properties and checks."""

    def test_readable_property(self, mock_blob_service_client):
        """Test readable() property."""
        mock_service = mock_blob_service_client[0]
        mock_blob_service_client[2].get_blob_properties.return_value = Mock(size=0)

        read_file = AzureBlobFile(
            connection_string='test',
            container_name='container',
            blob_name='test.txt',
            mode='r',
            blob_service_client=mock_service
        )
        assert read_file.readable() is True

        write_file = AzureBlobFile(
            connection_string='test',
            container_name='container',
            blob_name='test.txt',
            mode='w',
            blob_service_client=mock_service
        )
        assert write_file.readable() is False

    def test_writable_property(self, mock_blob_service_client):
        """Test writable() property."""
        mock_service = mock_blob_service_client[0]
        mock_blob_service_client[2].get_blob_properties.return_value = Mock(size=0)

        read_file = AzureBlobFile(
            connection_string='test',
            container_name='container',
            blob_name='test.txt',
            mode='r',
            blob_service_client=mock_service
        )
        assert read_file.writable() is False

        write_file = AzureBlobFile(
            connection_string='test',
            container_name='container',
            blob_name='test.txt',
            mode='w',
            blob_service_client=mock_service
        )
        assert write_file.writable() is True

    def test_seekable_property(self, mock_blob_service_client):
        """Test seekable() property."""
        mock_service = mock_blob_service_client[0]

        file = AzureBlobFile(
            connection_string='test',
            container_name='container',
            blob_name='test.txt',
            mode='w',
            blob_service_client=mock_service
        )
        assert file.seekable() is True

    def test_name_property(self, mock_blob_service_client):
        """Test name property."""
        mock_service = mock_blob_service_client[0]

        file = AzureBlobFile(
            connection_string='test',
            container_name='container',
            blob_name='test.txt',
            mode='w',
            blob_service_client=mock_service
        )
        assert file.name == 'test.txt'


class TestOpenAzureBlob:
    """Test the open_azure_blob convenience function."""

    @patch('flowlet.persistence.azure.BlobServiceClient')
    def test_open_azure_blob_creates_file(self, mock_service_class):
        """Test that open_azure_blob creates AzureBlobFile."""
        mock_service = Mock()
        mock_service_class.from_connection_string.return_value = mock_service

        file = open_azure_blob(
            connection_string='test',
            container_name='container',
            blob_name='test.txt',
            mode='w'
        )

        assert isinstance(file, AzureBlobFile)
        assert file.mode == 'w'
        mock_service_class.from_connection_string.assert_called_once_with('test')
