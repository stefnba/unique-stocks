import typing as t

from utils.filesystem.storage.types import ADLSSchemes, FileTypes, S3Schemes


def flatten_list(lst):
    """Flatten a nested list and ignore None values."""
    result = []
    for i in lst:
        if isinstance(i, list):
            result.extend(flatten_list(i))
        elif i is not None:
            result.append(i)
    return result


def is_valid_s3_schema(scheme: str) -> t.TypeGuard[S3Schemes]:
    """Check if scheme is a valid S3 scheme."""

    if scheme in t.get_args(S3Schemes):
        return True
    return False


def is_valid_adls_schema(scheme: str) -> t.TypeGuard[ADLSSchemes]:
    """Check if scheme is a valid ADLS scheme."""

    if scheme in t.get_args(ADLSSchemes):
        return True
    return False


def is_valid_file_type(type: str) -> t.TypeGuard[FileTypes]:
    """Check if the protocol is valid."""

    if type not in t.get_args(FileTypes):
        raise ValueError(f"File type {type} is not supported.")
    return True
