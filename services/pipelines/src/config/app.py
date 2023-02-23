from pydantic import BaseModel


class AppConfig(BaseModel):
    """
    Application configurations.
    """

    test = 1
