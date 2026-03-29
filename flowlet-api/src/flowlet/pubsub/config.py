"""Configuration classes for PubSub backends.

Provides Pydantic configuration models for each PubSub implementation,
following the discriminated union pattern used in storage and queue configuration.
"""
from typing import Literal, Union, Annotated

from pydantic import Field, Discriminator
from pydantic_settings import BaseSettings, SettingsConfigDict


class AzureWebPubSubConfig(BaseSettings):
    """Configuration for Azure Web PubSub.

    Attributes:
        type: PubSub type identifier, always "azure_web_pubsub".
        connection_string: Azure Web PubSub service connection string.
        hub: Hub name within the Azure Web PubSub service.

    Environment Variables:
        FLOWLET_PUBSUB_AZURE_CONNECTION_STRING: Azure connection string
        FLOWLET_PUBSUB_AZURE_HUB: Hub name (default: "flowlet")
    """
    model_config = SettingsConfigDict(
        env_prefix='FLOWLET_PUBSUB_AZURE',
        env_nested_delimiter='_',
        env_nested_max_split=1,
    )

    type: Literal["azure_web_pubsub"] = "azure_web_pubsub"
    connection_string: str = Field(
        description="Azure Web PubSub service connection string"
    )
    hub: str = Field(
        default="flowlet",
        description="Hub name within the Azure Web PubSub service"
    )


class InMemoryPubSubConfig(BaseSettings):
    """Configuration for in-memory PubSub (development/testing).

    Attributes:
        type: PubSub type identifier, always "memory".
    """
    model_config = SettingsConfigDict(
        env_prefix='FLOWLET_PUBSUB_MEMORY',
        env_nested_delimiter='_',
        env_nested_max_split=1,
    )

    type: Literal["memory"] = "memory"


class ZeroMQPubSubConfig(BaseSettings):
    """Configuration for ZeroMQ PubSub (local multi-process).

    Attributes:
        type: PubSub type identifier, always "zeromq".
        endpoint: ZMQ endpoint the publisher binds to.

    Environment Variables:
        FLOWLET_PUBSUB_ZEROMQ_ENDPOINT: ZMQ endpoint (default: "tcp://127.0.0.1:5555")
    """
    model_config = SettingsConfigDict(
        env_prefix='FLOWLET_PUBSUB_ZEROMQ',
        env_nested_delimiter='_',
        env_nested_max_split=1,
    )

    type: Literal["zeromq"] = "zeromq"
    endpoint: str = Field(
        default="tcp://127.0.0.1:5555",
        description="ZMQ endpoint the publisher binds to"
    )


PubSubConfig = Annotated[
    Union[AzureWebPubSubConfig, InMemoryPubSubConfig, ZeroMQPubSubConfig],
    Discriminator('type'),
]
"""Discriminated union type for all PubSub configurations.

Supports multiple PubSub backend types selected via the 'type' field.
Pydantic will automatically validate and parse the correct config type.

Example:
    >>> # Azure Web PubSub
    >>> config: PubSubConfig = {
    ...     "type": "azure_web_pubsub",
    ...     "connection_string": "...",
    ... }
    >>>
    >>> # In-Memory
    >>> config: PubSubConfig = {"type": "memory"}
    >>>
    >>> # ZeroMQ
    >>> config: PubSubConfig = {
    ...     "type": "zeromq",
    ...     "endpoint": "tcp://127.0.0.1:5556",
    ... }
"""
