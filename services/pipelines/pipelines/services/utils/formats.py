import json


def convert_json_to_bytes(json_input: dict | str, encoding="utf-8"):
    """
    Converts JSON object to bytes that can be used for file uploads.

    Args:
        input (dict | str): JSON

    Returns:
        _type_: JSON object as bytes
    """

    return bytes(json.dumps(json_input).encode(encoding))
