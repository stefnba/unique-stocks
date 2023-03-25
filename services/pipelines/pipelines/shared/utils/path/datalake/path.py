from pydantic import BaseModel
from shared.config import config
from shared.utils.path.datalake.builder import DatalakePathBuilder
from shared.utils.path.datalake.types import DatalakeFileTypes

DatalakeZones = config.datalake.zones
DatalakeFileTypes = DatalakeFileTypes


class DatalakePath(
    DatalakePathBuilder,
    BaseModel,
):
    """
    Base class for creating complete paths in the datalake.

    The following properties need to be specified:
    - directory
    - file_name
    - file_type

    All others are optional and only need to be specified if they are list in child class.


    """

    zone: DatalakeZones  # type: ignore

    def _get_args(self) -> dict:
        """
        Returns all args specified for pydantic BaseModel.
        method.

        Returns:
            _type_: _description_
        """
        return self.dict()

    class Config:
        extra = "allow"
        # exclude = {"_args", "_template_path"}
