from typing import Literal, Sequence, TypedDict

from typing_extensions import NotRequired


class MappingArgs(TypedDict):
    idType: Literal[
        "ID_ISIN", "TICKER", "COMPOSITE_ID_BB_GLOBAL", "ID_BB_GLOBAL_SHARE_CLASS_LEVEL", "ID_BB_GLOBAL", "ID_WERTPAPIER"
    ]
    idValue: str | int
    exchCode: NotRequired[str]
    micCode: NotRequired[str]
    currency: NotRequired[str]
    marketSecDes: NotRequired[Literal["Equity", "Index", "Curncy", "Comdty"]]


MappingInput = Sequence[MappingArgs]
