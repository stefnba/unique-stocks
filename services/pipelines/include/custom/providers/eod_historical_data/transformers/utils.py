from functools import reduce


def deep_get(data: dict, keys: str | list[str]):
    """Helper to get dict within nested dict."""
    if isinstance(keys, str):
        keys = keys.split(".")

    return reduce(lambda d, key: d.get(key, None) if isinstance(d, dict) else None, keys, data)


def nested_fundamental(data: dict, keys: str | list[str], return_values_as_list=False):
    """Helper that return nested dict or values of that dict as a list."""
    obj = deep_get(data=data, keys=keys)

    if obj and return_values_as_list:
        return list(obj.values())

    return obj
