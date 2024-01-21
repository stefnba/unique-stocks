import typing as t

FileTypes = t.Literal["csv", "parquet", "txt", "json", "zip"]
S3Schemes = t.Literal["s3", "s3a", "s3n"]
ADLSSchemes = t.Literal["abfs", "abfss", "adl", "adls", "adl2", "abfss", "abfss"]
Schemes = t.Union[S3Schemes, ADLSSchemes, t.Literal["file"]]
