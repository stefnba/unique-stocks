from pydantic import Field
from shared.config.env import EnvBaseConfig
from shared.config.types import EnvironmentTypes


class AppConfig(EnvBaseConfig):
    """
    Application configurations.
    """

    env: EnvironmentTypes = Field(env="ENV", default="Development")
