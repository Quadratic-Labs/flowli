"""
Configuration management for Flowlet

Provides a global configuration system similar to Python's logging module,
with sensible defaults that can be overridden.
"""
from pydantic import BaseModel, Field

from .database import DatabaseSettings


class FlowletConfig(BaseModel):
    """Configuration for Flowlet framework"""

    database: DatabaseSettings = Field(
        default_factory=DatabaseSettings,
        description="Flow runs database settings",
    )

    class Config:
        arbitrary_types_allowed = True


# # Global configuration state
# _global_config: Optional[FlowletConfig] = None
# _global_db_session_factory: Optional[sessionmaker] = None


# def configure(
#     database_url: Optional[str] = None,
#     db_session_factory: Optional[sessionmaker] = None,
#     **kwargs
# ) -> FlowletConfig:
#     """
#     Configure the global Flowlet instance.

#     This function allows you to override default configuration settings.
#     Similar to logging.basicConfig(), this should be called once at application startup.

#     Args:
#         database_url: Database connection URL (e.g., "postgresql://user:pass@host/db")
#         db_session_factory: Custom SQLAlchemy sessionmaker (overrides database_url if provided)
#         **kwargs: Additional configuration options

#     Returns:
#         The configured FlowletConfig instance

#     Example:
#         >>> from flowlet import configure
#         >>> configure(database_url="postgresql://localhost/mydb")
#         >>> flowlet = get_flowlet()
#     """
#     global _global_config, _global_db_session_factory

#     # Build config from provided arguments
#     config_dict = {}
#     if database_url is not None:
#         config_dict["database_url"] = database_url
#     config_dict.update(kwargs)

#     _global_config = FlowletConfig(**config_dict)

#     # Store custom session factory if provided
#     if db_session_factory is not None:
#         _global_db_session_factory = db_session_factory
#     else:
#         _global_db_session_factory = None

#     return _global_config


# def get_config() -> FlowletConfig:
#     """
#     Get the current global configuration.

#     If configure() has not been called, returns default configuration.

#     Returns:
#         Current FlowletConfig instance
#     """
#     global _global_config

#     if _global_config is None:
#         _global_config = FlowletConfig()

#     return _global_config


# def get_db_session_factory() -> sessionmaker:
#     """
#     Get the configured database session factory.

#     Creates and caches the session factory based on current configuration.
#     If a custom factory was provided via configure(), returns that instead.

#     Returns:
#         SQLAlchemy sessionmaker instance
#     """
#     global _global_db_session_factory

#     # Return custom factory if provided
#     if _global_db_session_factory is not None:
#         return _global_db_session_factory

#     # Otherwise, create from config
#     from .database import get_engine, get_db_session_factory as create_factory, init_database

#     config = get_config()
#     engine = get_engine(config)
#     init_database(engine)
#     factory = create_factory(engine)

#     # Cache it
#     _global_db_session_factory = factory

#     return factory


# def reset_config():
#     """
#     Reset configuration to defaults.

#     Useful for testing or when you need to reconfigure the system.
#     """
#     global _global_config, _global_db_session_factory

#     _global_config = None
#     _global_db_session_factory = None
