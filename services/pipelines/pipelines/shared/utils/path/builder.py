import pathlib
from typing import TYPE_CHECKING, Sequence, Type
from urllib.parse import urlparse

from shared.config import config
from shared.utils.path.types import FileNameParams, FilePath, PathParams, PathParamsOptional, UrlPath

DatalakeZones = config.datalake.zones

if TYPE_CHECKING:
    from shared.utils.path.data_lake.file_path import DataLakeFilePathModel


class PathBuilder:
    """
    Handles common operations for paths.
    """

    _components: list[str]

    def __init__(self, *args: PathParamsOptional) -> None:
        self._components = self._convert_args_to_list(*args)

    @classmethod
    def join_paths(cls, *args: PathParamsOptional) -> str:
        """
        Simple method to concatenate all path components with a "/".

        Returns:
            str: Joined path.
        """
        _path = cls(*args)
        return _path._join()

    def _join(self) -> str:
        """
        Simple helper method to concatenate all path components with a "/".

        Returns:
            str: Joined path.
        """
        return "/".join(self._clean_path_components(self._components))

    def to_pathlib(self) -> pathlib.Path:
        """
        Convert path components to pathlib.Path class.

        Returns:
            pathlib.Path: Path representing a filesystem path.
        """
        return pathlib.Path(*self._components)

    def _clean_path_components(self, components: list[str]) -> list[str]:
        """
        Takes and cleans a list of components.

        Args:
            components (list[str]): Components.

        Returns:
            list[str]: Cleaned components.
        """

        component_list: list[str] = []
        for index, comp in enumerate(components):
            if comp == "/":
                continue

            # keep absolute path
            if index == 0:
                _comp = comp.rstrip("/")
            else:
                _comp = comp.strip("/")

            # go one level up
            if _comp == "..":
                component_list.pop()

            component_list.append(_comp)

        return component_list

    def _convert_args_to_list(self, *args: PathParamsOptional) -> list[str]:
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


class UrlBuilder(PathBuilder):
    @classmethod
    def build_url(cls, *args: PathParamsOptional) -> str:
        _url = cls(*args)
        return _url._join_url()

    @classmethod
    def parse_url(cls, url: str) -> UrlPath:
        """
        Parse a URL into its components.

        Args:
            url (str): URL to be parsed.

        Returns:
            UrlPath: _description_
        """
        parsed_url = urlparse(url)
        return UrlPath(
            fragment=parsed_url.fragment,
            scheme=parsed_url.scheme,
            path=parsed_url.path,
            query=parsed_url.query,
            params=parsed_url.params,
            netloc=parsed_url.netloc,
            path_components=parsed_url.path.strip("/").split("/"),
        )

    def _join_url(self) -> str:
        components = self._clean_path_components(self._components)

        url = ""

        url_scheme = urlparse(components[0]).scheme
        if len(url_scheme) > 0:
            components.pop(0)
            url = f"{url_scheme}://"

        url += "/".join(components)
        return url


class FilePathBuilder(PathBuilder):
    @classmethod
    def convert_to_file_path(cls, path: PathParams | "DataLakeFilePathModel") -> str:
        """
        Converts path args of various formats into a file path.

        Args:
            path (PathArgs | DatalakePath | Type[DatalakePath]): Input path

        Returns:
            str: full path including file name and extension
        """
        from shared.utils.path.data_lake.file_path import DataLakeFilePath, DataLakeFilePathModel

        # string
        if isinstance(path, str):
            return path
        # list
        if isinstance(path, Sequence):
            _path = cls(path)
            return _path.join_file_paths()
        # class
        # if not isinstance(path, DataLakeFilePathModel) and issubclass(path, DataLakeFilePathModel):
        #     return str(DataLakeFilePath.Builder(path))
        if isinstance(path, DataLakeFilePathModel):
            return str(DataLakeFilePath.Builder(path))

    @classmethod
    def build_file_path(
        cls,
        file_path: PathParams,
        file_name: FileNameParams,
        file_type: str,
        file_name_sep: str = "__",
        make_absolute=False,
    ) -> str:
        """
        Constructs a full file path from a directory, file name, and file type input.

        Args:
            file_path (str | Sequence[str  |  None]): Must be an absolute path
            file_name (str | Sequence[str  |  None]): _description_
            file_type (str): _description_
            file_name_sep (str, optional): _description_. Defaults to "_".
        """

        _path = cls(file_path)

        if make_absolute and not _path._components[0].startswith("/"):
            _path._components = ["/"] + _path._components

        file_name = _path.__build_file_name(file_name=file_name, file_name_sep=file_name_sep)
        file_name_with_ext = f"{file_name.lstrip('/')}.{file_type.lstrip('.')}"
        return f"{_path.join_file_paths()}/{file_name_with_ext}"

    def __build_file_name(self, file_name: FileNameParams, file_name_sep: str = "_") -> str:
        """
        Helper function to build a file name from various input methods.

        Args:
            file_name (FileNameArgs): _description_

        Returns:
            str: _description_
        """
        if isinstance(file_name, Sequence) and not isinstance(file_name, str):
            return file_name_sep.join([component for component in file_name if component is not None])

        return file_name

    @classmethod
    def parse_file_path(cls, *args: PathParams) -> FilePath:
        _class = cls(*args)
        _path = _class.to_pathlib()

        stem_name = _path.stem
        name = _path.name
        extension = _path.suffix
        dir_path = _path.parent.as_posix()

        if len(_path.suffixes) == 0:
            raise Exception("No valid path to a file was provided.")

        return FilePath(
            name=name,
            stem_name=stem_name,
            type=extension.strip("."),
            dir_path=dir_path,
            extension=extension,
            path_components=list(_path.parts),
            full_path=_path.resolve().as_posix(),
        )

    @classmethod
    def build_relative_file_path(cls, base_path: str, path: str | list[str | None]) -> str:
        """
        Constructs an absolute file system path relative to a specified base path.

        Args:
            base_path (str): Can be a dir or also __file__.
            path (list[str  |  None]): File system path relative to base_path.

        Returns:
            str: Absolute file system path.
        """
        _path = pathlib.Path(base_path)
        if _path.is_file():
            base_path = _path.resolve().parent.as_posix()
        return cls(base_path, path).join_file_paths()

    def join_file_paths(self) -> str:
        """
        Joins together file paths, make it absolute and resolving all symlinks on the way and also normalizing it
        (for example turning slashes into backslashes under Windows).

        Returns:
            str: Complete resolved path.
        """
        _path = pathlib.Path(*self._components)
        return _path.resolve().as_posix()
