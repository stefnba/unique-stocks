import pathlib
from typing import Sequence

from services.utils.path.types import FilePath, PathArgs


class PathBuilder:
    """
    Handles common operations for paths.
    """

    __components: list[str]

    def __init__(self, *args: PathArgs) -> None:
        self.__components = self._convert_args_to_list(*args)

    def join(self) -> str:
        return "/".join(self._parse_path_components(self.__components))

    def to_pathlib(self) -> pathlib.Path:
        """
        Convert path components to pathlib.Path class.

        Returns:
            pathlib.Path: Path representing a filesystem path.
        """
        return pathlib.Path(*self.__components)

    def resolve(self) -> str:
        _path = pathlib.Path(*self.__components)
        return _path.resolve().as_posix()

    def _parse_path_components(self, components: list[str]) -> list[str]:
        """


        Args:
            components (list[str]): _description_

        Returns:
            list[str]: Parsed components.
        """
        component_list: list[str] = []
        for index, comp in enumerate(components):
            if index == 0:
                _comp = comp.rstrip("/")
            else:
                _comp = comp.strip("/")

            if _comp == "..":
                print("parent")

            component_list.append(_comp)

        return component_list

    def _convert_args_to_list(self, *args: PathArgs) -> list[str]:
        """
        Takes args and turns them into list of path components.

        Returns:
            list[str]: Individual path components.
        """
        args_list: list[str] = []

        for arg in args:
            if isinstance(arg, list):
                arg_list: list[str] = []

                for _arg in arg:
                    if _arg is not None:
                        arg_list.extend(list(pathlib.Path(_arg).parts))

                args_list.extend(arg_list)

            if isinstance(arg, str):
                args_list.extend(list(pathlib.Path(arg).parts))

        return args_list

    @classmethod
    def parse_file_path(cls, *args: PathArgs) -> FilePath:
        _class = cls(*args)
        _path = _class.to_pathlib()

        name = _path.stem
        extension = _path.suffix
        dir_path = _path.parent.as_posix()

        if len(_path.suffixes) == 0:
            raise Exception("No valid path to a file was provided.")

        return FilePath(
            name=name, type=extension.strip("."), path=dir_path, extension=extension, path_components=list(_path.parts)
        )

    @classmethod
    def build_relative_to_base(cls, base_path: str, path: str | list[str | None]) -> str:
        """
        Builds an absolute dir path relative to a base path.

        Args:
            base_path (str): Can be a dir or also __file__
            path (list[str  |  None]): File system path relative to base_path

        Returns:
            str: Absolute file system path.
        """
        _path = pathlib.Path(base_path)
        if _path.is_file():
            base_path = _path.resolve().parent.as_posix()
        return cls(base_path, path).resolve()

    @classmethod
    def build_file_path(
        cls,
        file_path: str | Sequence[str | None],
        file_name: str | Sequence[str | None],
        file_type: str,
        file_name_sep="_",
    ):
        pass
