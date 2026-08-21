"""Unit tests for the storage config delegation to cairndb's catalog."""
import pytest
from cairndb.storage.config import (
    AzureStorageConfig,
    FilesystemStorageConfig,
    GCSStorageConfig,
    S3StorageConfig,
)

from flowlet.config import FlowletConfig
from flowlet.storage.config import StorageConfig


@pytest.mark.unit
class TestDelegation:
    def test_filesystem_with_legacy_base_path(self, tmp_path):
        config = StorageConfig(type="filesystem", base_path=str(tmp_path))
        resolved = config.to_cairndb()
        assert isinstance(resolved, FilesystemStorageConfig)
        assert resolved.path == str(tmp_path)
        # and the store actually builds
        assert config.store is config.store  # cached

    def test_filesystem_with_cairndb_spelling(self, tmp_path):
        resolved = StorageConfig(type="filesystem", path=str(tmp_path)).to_cairndb()
        assert resolved.path == str(tmp_path)

    def test_legacy_azure_blob_aliases(self):
        resolved = StorageConfig(
            type="azure_blob",
            container_name="flowlet-logs",
            base_path="prod/",
            connection_string="cs",
        ).to_cairndb()
        assert isinstance(resolved, AzureStorageConfig)
        assert resolved.container == "flowlet-logs"
        assert resolved.prefix == "prod/"
        assert resolved.connection_string == "cs"

    def test_azure_account_url_auth(self):
        resolved = StorageConfig(
            type="azure",
            container="flowlet",
            account_url="https://acct.blob.core.windows.net",
        ).to_cairndb()
        assert resolved.account_url == "https://acct.blob.core.windows.net"
        assert resolved.connection_string is None

    def test_s3_passthrough(self):
        resolved = StorageConfig(
            type="s3", bucket="flowlet", prefix="prod/",
            region="eu-west-1", endpoint_url="http://minio:9000",
        ).to_cairndb()
        assert isinstance(resolved, S3StorageConfig)
        assert resolved.bucket == "flowlet"
        assert resolved.endpoint_url == "http://minio:9000"

    def test_gcs_passthrough(self):
        resolved = StorageConfig(
            type="gcs", bucket="flowlet", project="quadratic",
        ).to_cairndb()
        assert isinstance(resolved, GCSStorageConfig)
        assert resolved.project == "quadratic"

    def test_unknown_backend_is_cairndbs_error(self):
        from cairndb.core.exceptions import ConfigurationError

        with pytest.raises(ConfigurationError):
            StorageConfig(type="carrier-pigeon").to_cairndb()

    def test_flowlet_config_still_takes_the_legacy_dict(self, tmp_path):
        config = FlowletConfig(
            storage={"type": "filesystem", "base_path": str(tmp_path)}
        )
        assert config.store is not None