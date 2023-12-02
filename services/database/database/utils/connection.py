from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Connection(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    host: str = Field(alias="DB_HOST", default="localhost")
    port: int = Field(alias="DB_PORT", default=5432)
    dbname: str = Field(alias="DB_NAME", default=None)
    user: str = Field(alias="DB_ADMIN_USER", default=None)
    password: str = Field(alias="DB_ADMIN_PASSWORD", default=None)


def connection_string() -> str:
    """
    Creates connection url for alembic.
    """
    connection = Connection()
    return f"postgresql+psycopg2://{connection.user}:{connection.password}@{connection.host}:{connection.port}/{connection.dbname}"


def connection_model():
    """
    Creates connection dict that can be used for psycopg.
    """
    return Connection().model_dump()
