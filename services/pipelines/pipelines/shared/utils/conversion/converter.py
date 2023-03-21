import json


def json_to_bytes(json_input: dict | str, encoding="utf-8") -> bytes:
    """
    Converts JSON object to bytes that can be used for file uploads.

    Args:
        input (dict | str): JSON

    Returns:
        _type_: JSON object as bytes
    """

    return json.dumps(json_input).encode(encoding)
