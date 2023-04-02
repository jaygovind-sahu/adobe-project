from src.glue.search_keyword_performance.search_engines import get_qs_param


def test_get_qs_params():
    assert get_qs_param('google.com') == 'q'
    assert get_qs_param('bing.com') == 'q'
    assert get_qs_param('yahoo.com') == 'p'
    assert get_qs_param('myseachengine.com') == 'q'
