SEARCH_ENGINE_QS_PARAM_MAPPING = {
    'google.com': 'q',
    'bing.com': 'q',
    'yahoo.com': 'p',
}

DEFAULT_QS_PARAM = 'q'


def get_qs_param(search_engine: str) -> str:
    """Get the query string param for the given search engine

    Args:
        search_engine (str): search engine

    Returns:
        str: query string param
    """
    return SEARCH_ENGINE_QS_PARAM_MAPPING.get(search_engine, DEFAULT_QS_PARAM)
