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
