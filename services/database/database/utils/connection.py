from pydantic import BaseSettings, Field


class Connection(BaseSettings):
    host: str = Field(env="DB_HOST", default="localhost")
    port: int = Field(env="DB_PORT", default=5432)
    dbname: str = Field(env="DB_NAME_STOCKS", default=None)
    user: str = Field(env="DB_ADMIN_USER", default=None)
    password: str = Field(env="DB_ADMIN_PASSWORD", default=None)

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        case_sentive = False


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
    return Connection().dict()


# %%
