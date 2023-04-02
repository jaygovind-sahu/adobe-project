import ast
from typing import Tuple
from urllib import parse

import tldextract

from src.glue.search_keyword_performance.search_engines import get_qs_param


class SearchEngineData:
    """A class to parse search engine data
    """
    DEFAULT_KEYWORD = '<error>'

    def __init__(self, referrer: str) -> None:
        self.parse_result = parse.urlparse(referrer)

    @property
    def search_engine_name(self) -> str:
        """Extract search engine name

        Returns:
            str: the search engine domain name (google.com / yahoo.com, etc.)
        """
        return tldextract.extract(self.parse_result.netloc).registered_domain

    @property
    def search_keyword(self) -> str:
        """Extract the search keyword

        Returns:
            str: the search keyword - in lower case
        """
        query = dict(parse.parse_qsl(self.parse_result.query))
        qs_param = get_qs_param(self.search_engine_name)
        keyword = query.get(qs_param, self.DEFAULT_KEYWORD)
        return keyword.lower().strip()


def parse_referrer_data(referrer: str) -> Tuple[str, str]:
    """Parse referrer data by using the SearchEngineData class

    A referrer URL looks like this:

    "http://www.google.com/search?hl=en&client=firefox-a&rls=org.mozilla%3Aen-US%3Aofficial&hs=ZzP&q=Ipod&aq=f&oq=&aqi="

    Args:
        referrer (str): the full referrer URL

    Returns:
        Tuple[str, str]: search engine domain name and search keyword
    """
    search_engine_data = SearchEngineData(referrer)
    return (
        search_engine_data.search_engine_name,
        search_engine_data.search_keyword,
    )


def parse_revenue(product_string: str) -> float:
    """Parse revenue from product string

    A product string looks like this: 

    "Computers;HP Pavillion;1;1000;200|201,Office Supplies;Red Folders;4;4.00;205|206|207"

    - Records separated by commas, and fields separated by semi-colon

    Args:
        product_string (str): a hit data product string

    Returns:
        float: _description_
    """
    if not product_string:
        return 0.0
    str_parts = product_string.split(';')
    quantity = ast.literal_eval(str_parts[2])
    price = ast.literal_eval(str_parts[3])
    return quantity * price * 1.0
