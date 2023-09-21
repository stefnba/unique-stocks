import polars as pl
from typing import Optional, TypedDict
from typing_extensions import NotRequired
from shared.loggers.logger import dataset as logger
from shared.loggers.events import dataset as log_events


class ValidateDatasetOptions(TypedDict):
    dataset_name: NotRequired[str]
    log_extra: NotRequired[dict]
    job_name: NotRequired[str]
    product: NotRequired[str]


class ValidateDataset:
    """Validate dataset for certain conditions and exludes and logs non-compliant records."""

    dataset: pl.DataFrame
    id_column: Optional[str | list[str]] = None
    options: ValidateDatasetOptions = {}

    def __init__(
        self,
        dataset: pl.DataFrame,
        id_column: Optional[str | list[str]] = None,
        options: ValidateDatasetOptions = {},
    ) -> None:
        self.dataset = dataset
        self.id_column = id_column
        self.options = options

    def return_dataset(self):
        """Return filtered dataset"""
        return self.dataset

    def is_not_null(
        self, column: str | list[str], id_column: Optional[str | list[str]] = None, message: Optional[str] = None
    ):
        """Columns must not be null."""

        self.__log_violation(
            violoation="IsNotNull",
            data=self.dataset.filter(pl.col(column).is_null()),
            id_column=id_column,
            message=message,
        )

        self.dataset = self.dataset.filter(pl.col(column).is_not_null())

        return self

    def is_null(
        self, column: str | list[str], id_column: Optional[str | list[str]] = None, message: Optional[str] = None
    ):
        """Columns must be null."""

        self.__log_violation(
            violoation="IsNull",
            data=self.dataset.filter(pl.col(column).is_not_null()),
            id_column=id_column,
            message=message,
        )

        self.dataset = self.dataset.filter(pl.col(column).is_null())

        return self

    def is_unique(
        self, column: str | list[str], id_column: Optional[str | list[str]] = None, message: Optional[str] = None
    ):
        """Column values must be unique."""

        self.dataset = self.dataset.filter(pl.col(column).is_unique())

        self.__log_violation(
            violoation="IsNotUnique",
            data=self.dataset.filter(~pl.col(column).is_unique()),
            id_column=id_column,
            message=message,
        )

        return self

    def __log_violation(
        self,
        violoation: str,
        data: pl.DataFrame,
        id_column: Optional[str | list[str]] = None,
        message: Optional[str] = None,
    ):
        _id_columns = id_column or self.id_column or self.dataset.columns

        if len(data) > 0:
            unique_df = data[_id_columns].unique(maintain_order=True)

            unique_values = []
            if isinstance(unique_df, pl.Series):
                unique_values = unique_df.to_list()
            if isinstance(unique_df, pl.DataFrame):
                unique_values = unique_df.to_dicts()

            logger.warn(
                msg=message,
                event=log_events.ValidationViolation(
                    dataset=self.options.get("dataset_name"),
                    job=self.options.get("job_name"),
                    product=self.options.get("product"),
                    violation=violoation,
                ),
                extra={"length": len(data), "unique_length": len(unique_values), "data": unique_values},
            )
