from typing import Literal

from pydantic import BaseModel

Env = Literal["Production", "Development"]


class AppConfig(BaseModel):
    """
    Application configurations.
    """

    env: Env = "Development"
