class S3UriParseFailure(Exception):
    """Exception raised when parsing S3 URI fails."""


class S3NotValidUri(Exception):
    """Exception raised when S3 URI is not valid."""


class ADLSUriParseFailure(Exception):
    """Exception raised when parsing ADLS URI fails."""
