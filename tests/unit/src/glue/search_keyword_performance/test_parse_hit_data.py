from src.glue.search_keyword_performance.parse_hit_data import parse_referrer_data, parse_revenue


def test_parse_refferer_data():
    assert parse_referrer_data(
        'http://www.google.com/search?hl=en&client=firefox-a&rls=org.mozilla%3Aen-US%3Aofficial&hs=ZzP&q=Ipod&aq=f&oq=&aqi='
    ) == ('google.com', 'ipod')

    assert parse_referrer_data(
        'http://search.yahoo.com/search?p=cd+player&toggle=1&cop=mss&ei=UTF-8&fr=yfp-t-701'
    ) == ('yahoo.com', 'cd player')


def test_parse_revenue():
    assert parse_revenue(
        'Electronics;Ipod - Touch - 32GB;1;290;;') == 290.0

    assert parse_revenue(
        'Electronics;Ipod - Touch - 16GB;2;249.99;;') == 499.98
