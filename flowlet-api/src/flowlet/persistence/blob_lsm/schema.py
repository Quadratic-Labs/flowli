"""Parquet schema definitions for LSM storage.

Maps attrs dataclasses to PyArrow schemas for efficient columnar storage.
"""

from typing import Any
from uuid import UUID
import pyarrow as pa
from attrs import asdict

from flowlet.interfaces.repository.models import (
    RunAttrModel,
    RunLogAttrModel,
)


class ParquetSchemaBuilder:
    """Converts attrs models to/from Parquet format."""

    # PyArrow schema for runs
    RUNS_SCHEMA = pa.schema([
        pa.field('run_id', pa.string(), nullable=False),  # UUID as string
        pa.field('run_type', pa.dictionary(pa.int8(), pa.string()), nullable=False),
        pa.field('name', pa.dictionary(pa.int16(), pa.string()), nullable=False),
    ])

    # PyArrow schema for logs
    LOGS_SCHEMA = pa.schema([
        pa.field('log_id', pa.string(), nullable=False),  # UUID as string
        pa.field('run_id', pa.string(), nullable=False),  # UUID as string (for partitioning)
        pa.field('timestamp', pa.timestamp('us', tz='UTC'), nullable=False),
        pa.field('status', pa.dictionary(pa.int8(), pa.string()), nullable=False),
        pa.field('log', pa.string(), nullable=False),
    ])

    # PyArrow schema for links
    LINKS_SCHEMA = pa.schema([
        pa.field('link_id', pa.string(), nullable=False),  # UUID as string
        pa.field('parent_run_id', pa.string(), nullable=False),
        pa.field('child_run_id', pa.string(), nullable=False),
    ])

    @staticmethod
    def run_to_dict(run: RunAttrModel) -> dict[str, Any]:
        """Convert RunAttrModel to dict for Parquet serialization."""
        data = asdict(run)
        data['run_id'] = str(data['run_id'])  # UUID to string
        return data

    @staticmethod
    def dict_to_run(data: dict[str, Any]) -> RunAttrModel:
        """Convert Parquet dict back to RunAttrModel."""
        data['run_id'] = UUID(data['run_id'])  # String to UUID
        return RunAttrModel(**data)

    @staticmethod
    def log_to_dict(log: RunLogAttrModel) -> dict[str, Any]:
        """Convert RunLogAttrModel to dict for Parquet serialization."""
        data = asdict(log)
        data['log_id'] = str(data['log_id'])  # UUID to string
        data['run_id'] = str(data['run_id'])  # UUID to string
        return data

    @staticmethod
    def dict_to_log(data: dict[str, Any]) -> RunLogAttrModel:
        """Convert Parquet dict back to RunLogAttrModel."""
        data['log_id'] = UUID(data['log_id'])  # String to UUID
        data['run_id'] = UUID(data['run_id'])  # String to UUID
        return RunLogAttrModel(**data)

    @staticmethod
    def link_to_dict(parent_id: UUID, child_id: UUID, link_id: UUID) -> dict[str, Any]:
        """Convert link IDs to dict for Parquet serialization."""
        return {
            'link_id': str(link_id),
            'parent_run_id': str(parent_id),
            'child_run_id': str(child_id),
        }

    @staticmethod
    def dict_to_link(data: dict[str, Any]) -> tuple[UUID, UUID, UUID]:
        """Convert Parquet dict back to link tuple."""
        return (
            UUID(data['parent_run_id']),
            UUID(data['child_run_id']),
            UUID(data['link_id']),
        )
