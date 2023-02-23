from pydantic import BaseSettings


class EnvBaseConfig(BaseSettings):
    """
    Base configuration for env variables that reads in .env file
    """
    class Config:
        env_file = '.env'
        env_file_encoding = 'utf-8'
        case_sentive = False
