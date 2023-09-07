from typing import Optional, overload, TYPE_CHECKING, TypeAlias

from datetime import datetime
from shared.types import DataLakeDataFileTypes, DataLakeZone, DataProducts, DataSources


from utils.filesystem.data_lake.base import DataLakePathBase
from utils.filesystem.data_lake.types import Flavor


class PathPlaceholder:
    flavor: Flavor

    def __init__(self, flavor: Flavor = "hive") -> None:
        self.flavor = flavor


class PathElement:
    _value: str
    _key: str
    _flavor: Flavor

    def __init__(self, key: str, value: Optional[str] = None, flavor: Flavor = "hive"):
        self._key = key
        self.value = value
        self._flavor = flavor

    @property
    def key(self):
        return self._key

    @property
    def value(self):
        return self._value

    @value.setter
    def value(self, value):
        self._value = value

    @property
    def flavor(self):
        return self._flavor

    @flavor.setter
    def flavor(self, flavor: Flavor):
        self._flavor = flavor

    def build(self):
        if not self.value:
            return

        if self.flavor == "hive":
            return f"{self._key}={self.value}"
        return self.value


Pattern: TypeAlias = PathElement | str | PathPlaceholder


class DataLakePath(DataLakePathBase):
    _zone: DataLakeZone
    _format: DataLakeDataFileTypes
    _datetime: datetime = datetime.now()

    product: DataProducts

    dir_pattern: Optional[list[Pattern]] = [
        PathElement(key="product", flavor="value"),
        PathPlaceholder(),
        PathElement(key="year"),
        PathElement(key="month"),
        PathElement(key="day"),
    ]
    file_pattern: Optional[list[Pattern]] = [
        PathElement(key="date", flavor="value"),
        PathElement(key="time", flavor="value"),
        PathElement(key="product", flavor="value"),
        PathPlaceholder(flavor="value"),
    ]

    placeholder_pattern: Optional[list[Pattern]] = None

    def __init__(
        self,
        zone: DataLakeZone,
        format: DataLakeDataFileTypes = "parquet",
    ) -> None:
        self.zone = zone
        self._format = format

        self.date = self._datetime.strftime("%Y%m%d")
        self.time = self._datetime.strftime("%H%M%S")
        self.year = self._datetime.strftime("%Y")
        self.month = self._datetime.strftime("%m")
        self.day = self._datetime.strftime("%d")
        self.hour = self._datetime.strftime("%H")
        self.minute = self._datetime.strftime("%M")
        self.second = self._datetime.strftime("%S")

    @overload
    def build_from_pattern(self, pattern: list[Pattern], sep=..., flavor=...) -> str:
        ...

    @overload
    def build_from_pattern(self, pattern: list[Pattern] | None, sep=..., flavor=...) -> None | str:
        ...

    def build_from_pattern(self, pattern: Optional[list[Pattern]] = None, sep="/", flavor: Optional[Flavor] = None):
        if not pattern:
            return

        def get_path_element_value(p: Pattern):
            if isinstance(p, PathElement):
                key = p.key

                if flavor:
                    p.flavor = flavor

                if hasattr(self, key):
                    p.value = getattr(self, key)

                return p.build()
            if isinstance(p, PathPlaceholder):
                if self.placeholder_pattern:
                    return self.build_from_pattern(pattern=self.placeholder_pattern, sep=sep, flavor=p.flavor)
                return None
            return p

        items = [i for i in [get_path_element_value(p) for p in pattern] if i]
        return sep.join(items)

    @classmethod
    def raw(cls, source: DataSources, format: DataLakeDataFileTypes):
        p = cls(zone="raw", format=format)

        p.dir_pattern = [
            PathElement(key="product", flavor="value"),
            PathElement(key="source", value=source),
            PathPlaceholder(),
            PathElement(key="year"),
            PathElement(key="month"),
            PathElement(key="day"),
        ]

        return p

    @classmethod
    def curated(cls):
        p = cls(
            zone="curated",
        )
        p.dir_pattern = [
            PathElement(key="product", flavor="value"),
        ]
        p.file_pattern = None
        return p

    @classmethod
    def transformed(cls):
        p = cls(
            zone="curated",
        )
        p.dir_pattern = [
            PathElement(key="product", flavor="value"),
        ]
        p.file_pattern = None
        return p

    def add_element(self, **kwargs):
        for key, value in kwargs.items():
            setattr(self, key, value)

        return self

    @property
    def filename(self):
        name = self.build_from_pattern(pattern=self.file_pattern, sep="__")
        if name:
            return name + self.extension

    @property
    def directory(self):
        return (self.build_from_pattern(pattern=self.dir_pattern, sep="/") or "") + "/"
