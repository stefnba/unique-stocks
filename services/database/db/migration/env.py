# pylint: disable=no-member
import os
from logging.config import fileConfig
from sqlalchemy import create_engine
from dotenv import load_dotenv
from alembic import context

load_dotenv()


def connection_string() -> str:
    """
    Creates connection url for alembic
    """
    host = os.environ.get("DB_HOST", "localhost")
    port = os.environ.get("DB_PORT", 5432)
    database = os.environ.get("DB_NAME")
    user = os.environ.get("DB_ADMIN_USER")
    password = os.environ.get("DB_ADMIN_PASSWORD")
    return f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}"


# this is the Alembic Config object, which provides
# access to the values within the .ini file in use.
config = context.config


# Interpret the config file for Python logging.
# This line sets up loggers basically.
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

# table name for migrations table
migration_table_section = config.get_section("migration_table")
if migration_table_section is not None:
    migration_table_name = migration_table_section.get("table", "_migrations")

# add your model's MetaData object here
# for 'autogenerate' support
# from myapp import mymodel
# target_metadata = mymodel.Base.metadata
target_metadata = None

# other values from the config, defined by the needs of env.py,
# can be acquired:
# my_important_option = config.get_main_option("my_important_option")
# ... etc.


def run_migrations_offline() -> None:
    """Run migrations in 'offline' mode.

    This configures the context with just a URL
    and not an Engine, though an Engine is acceptable
    here as well.  By skipping the Engine creation
    we don't even need a DBAPI to be available.

    Calls to context.execute() here emit the given string to the
    script output.

    """
    context.configure(
        url=connection_string(),
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        version_table=migration_table_name,
    )

    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    """Run migrations in 'online' mode.

    In this scenario we need to create an Engine
    and associate a connection with the context.

    """
    connectable = create_engine(connection_string())

    with connectable.connect() as connection:
        context.configure(connection=connection, target_metadata=target_metadata, version_table=migration_table_name)

        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
