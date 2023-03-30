def flatten_list(input_list):
    """
    Flattens a nested list.
    """
    if not isinstance(input_list, list):  # if not list
        return [input_list]
    return [x for sub in input_list for x in flatten_list(sub)]  # recurse and collect
