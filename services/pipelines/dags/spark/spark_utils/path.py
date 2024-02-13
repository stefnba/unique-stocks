import re
from urllib.parse import urlsplit


def is_adls_uri(uri: str) -> bool:
    format = uri.split("//")
    if re.match(r"abfs:", format[0], re.IGNORECASE):
        return True
    return False


def is_s3_uri(uri: str) -> bool:
    format = uri.split("//")
    if re.match(r"s3[na]?:", format[0], re.IGNORECASE):
        return True
    return False


def convert_to_adls_uri(uri: str | None, account_name: str | None) -> str:
    if not account_name:
        raise Exception("Account name is missing.")
    if not uri:
        raise Exception("URI is missing.")
    if not is_adls_uri(uri):
        raise Exception("Not a valid ADLS URI.")

    parsed = urlsplit(uri)
    container = parsed.netloc
    path = parsed.path.lstrip("/")

    return f"abfs://{container}@{account_name}.dfs.core.windows.net/{path}"


def convert_to_s3_uri(uri: str) -> str:
    if not uri:
        raise Exception("URI is missing.")
    if not is_s3_uri(uri):
        raise Exception(f"'{uri}' is not a valid S3 URI.")

    parsed = urlsplit(uri)
    bucket = parsed.netloc
    key = parsed.path.lstrip("/")

    return f"s3a://{bucket}/{key}"
