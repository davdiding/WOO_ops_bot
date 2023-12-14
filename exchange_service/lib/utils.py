import pandas as pd


def query_dict(dictionary: dict, query: str) -> dict:
    """
    Query a dictionary with a query string
    :param dictionary: dictionary to query
    :param query: query string
    :return: queried dictionary
    """
    if not query:
        return dictionary

    df = pd.DataFrame(dictionary).T
    df = df.query(query)
    return df.to_dict(orient="index")


def nested_query_dict(dictionary: dict, key: str, query: str) -> dict:
    """
    Query a dictionary with a query string
    :param dictionary: dictionary to query
    :param query: query string
    :return: queried dictionary
    """
    if not query:
        return dictionary

    new_dict = {outer: inner[key] for outer, inner in dictionary.items() if key in inner}

    queried_dict = query_dict(new_dict, query)
    return {key: dictionary[key] for key in queried_dict.keys()}
