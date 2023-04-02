import os
import pytest

from src.glue.search_keyword_performance import processor
from unittest import mock

fixture_data_path = os.path.join(
    os.path.dirname('.'), 'tests', 'fixtures', 'data')


@pytest.mark.usefixtures("spark_session")
def test_load_raw_data(spark_session):
    raw_data = processor.load_raw_data(
        os.path.join(fixture_data_path, 'raw_hit_data.tsv'), spark_session)
    assert raw_data.columns == [
        'hit_time_gmt', 'date_time', 'user_agent', 'ip', 'event_list',
        'geo_city', 'geo_region', 'geo_country', 'pagename', 'page_url',
        'product_list', 'referrer']
    return raw_data


@pytest.mark.usefixtures("spark_session")
def test_rank_hit_data_by_time(spark_session):
    raw_data = spark_session.createDataFrame([
        ('101', '10.0.0.1', '', 'http://www.google.com/?q=item1', ''),
        ('102', '10.0.0.1', '2', 'http://www.website.com/?q=item1', ''),
        ('103', '10.0.0.1', '1', 'http://www.website.com/?q=item1', 'some;product;list1'),
        ('108', '10.0.0.1', '', 'http://www.google.com/?q=b1', ''),
        ('109', '10.0.0.1', '', 'http://www.bing.com/?q=a1', ''),
        ('110', '10.0.0.1', '2', 'http://www.website.com/?q=a1', ''),
        ('111', '10.0.0.1', '1', 'http://www.website.com/?q=a1', 'some;product;a1'),
        ('201', '10.0.0.2', '', 'http://www.bing.com/?q=item2', ''),
        ('202', '10.0.0.2', '2', 'http://www.website.com/?q=item2', ''),
        ('203', '10.0.0.2', '', 'http://www.website.com/?q=item2', ''),
        ('204', '10.0.0.2', '1', 'http://www.website.com/?q=item2', 'some;product;list2'),
        ('205', '10.0.0.2', '', 'http://www.google.com/?q=item3', ''),
        ('206', '10.0.0.2', '2', 'http://www.website.com/?q=item3', ''),
        ('207', '10.0.0.2', '1', 'http://www.website.com/?q=item3', 'some;product;list3'),
        ('305', '10.0.0.9', '',
         'http://www.search.yahoo.com/?p=van', 'some;product,listy'),
    ], schema=['hit_time_gmt', 'ip', 'event_list', 'referrer', 'product_list'])
    output = processor.rank_hit_data_by_time(raw_data)

    expected_output = spark_session.createDataFrame([
        ('10.0.0.1', 0, False, True,
         'http://www.website.com/?q=item1', 'some;product;list1'),
        ('10.0.0.1', 0, True, False, 'http://www.google.com/?q=item1', ''),
        ('10.0.0.1', 1, False, True,
         'http://www.website.com/?q=a1', 'some;product;a1'),
        ('10.0.0.1', 1, True, False, 'http://www.bing.com/?q=a1', ''),
        ('10.0.0.2', 0, False, True,
         'http://www.website.com/?q=item2', 'some;product;list2'),
        ('10.0.0.2', 0, True, False, 'http://www.bing.com/?q=item2', ''),
        ('10.0.0.2', 1, False, True,
         'http://www.website.com/?q=item3', 'some;product;list3'),
        ('10.0.0.2', 1, True, False, 'http://www.google.com/?q=item3', ''),
        ('10.0.0.9', 0, True, False,
         'http://www.search.yahoo.com/?p=van', 'some;product,listy'),
    ], schema=['ip', 'journey_num', 'is_search', 'is_purchase', 'referrer', 'product_list'])

    assert sorted(output.collect()) == sorted(expected_output.collect())


@pytest.mark.usefixtures("spark_session")
def test_summarize_hit_data(spark_session):
    ranked_by_time = spark_session.createDataFrame([
        ('10.0.0.1', 0, False, True,
         'http://www.website.com/?q=item1', 'some;product;list1'),
        ('10.0.0.1', 0, True, False, 'http://www.google.com/?q=item1', ''),
        ('10.0.0.1', 1, False, True,
         'http://www.website.com/?q=a1', 'some;product;a1'),
        ('10.0.0.1', 1, True, False, 'http://www.bing.com/?q=a1', ''),
        ('10.0.0.2', 0, False, True,
         'http://www.website.com/?q=item2', 'some;product;list2'),
        ('10.0.0.2', 0, True, False, 'http://www.bing.com/?q=item2', ''),
        ('10.0.0.2', 1, False, True,
         'http://www.website.com/?q=item3', 'some;product;list3,some;product;list4'),
        ('10.0.0.2', 1, True, False, 'http://www.google.com/?q=item3', ''),
        ('10.0.0.9', 0, True, False,
         'http://www.search.yahoo.com/?p=van', 'some;product,listy'),
    ], schema=['ip', 'journey_num', 'is_search', 'is_purchase', 'referrer', 'product_list'])
    output = processor.summarize_hit_data(ranked_by_time)

    expected_output = spark_session.createDataFrame([
        ('http://www.google.com/?q=item1', 'some;product;list1'),
        ('http://www.bing.com/?q=a1', 'some;product;a1'),
        ('http://www.bing.com/?q=item2', 'some;product;list2'),
        ('http://www.google.com/?q=item3', 'some;product;list3'),
        ('http://www.google.com/?q=item3', 'some;product;list4'),
        ('http://www.search.yahoo.com/?p=van', None),
    ], schema=['referrer', 'product'])

    assert sorted(output.collect()) == sorted(expected_output.collect())


@pytest.mark.usefixtures("spark_session")
def test_parse_referrer_revenue_data(spark_session):
    hit_summary = spark_session.createDataFrame([
        ('http://www.google.com/?q=ipod',
         'Electronics;Ipod - Touch - 32GB;1;299.99;'),
        ('http://www.google.com/?q=IPod',
         'Electronics;Ipod - Touch - 16GB;1;250;'),
        ('http://www.google.com/?q=macbook+pro',
         'Electronics;Macbook Pro - 1TB;1;2290;'),
        ('http://www.bing.com/?q=dan+brown',
         'Books;Angels and Demons;1;15;'),
        ('http://www.search.yahoo.com/?p=van', None),
    ], schema=['referrer', 'product'])
    output = processor.parse_referrer_revenue_data(hit_summary)

    expected_output = spark_session.createDataFrame([
        ('google.com', 'ipod', '299.99'),
        ('google.com', 'ipod', '250.0'),
        ('google.com', 'macbook pro', '2290.0'),
        ('bing.com', 'dan brown', '15.0'),
        ('yahoo.com', 'van', '0.0'),
    ], schema=['search_engine_domain', 'search_keyword', 'revenue'])

    assert sorted(output.collect()) == sorted(expected_output.collect())


@pytest.mark.usefixtures("spark_session")
def test_agg_keyword_revenue(spark_session):
    search_data = spark_session.createDataFrame([
        ('google.com', 'ipod', 299.99),
        ('google.com', 'ipod', 250.0),
        ('google.com', 'macbook pro', 2290.0),
        ('bing.com', 'dan brown', 15.0),
        ('yahoo.com', 'van', 0.0),
    ], schema=['search_engine_domain', 'search_keyword', 'revenue'])
    output = processor.agg_keyword_revenue(search_data)

    expected_output = spark_session.createDataFrame([
        ('google.com', 'macbook pro', 2290.0),
        ('google.com', 'ipod', 549.99),
        ('bing.com', 'dan brown', 15.0),
        ('yahoo.com', 'van', 0.0),
    ], schema=['search_engine_domain', 'search_keyword', 'revenue'])

    assert sorted(output.collect()) == sorted(expected_output.collect())


@pytest.mark.usefixtures("spark_session")
def test_generate_perf_report(spark_session):
    raw_data = test_load_raw_data(spark_session)
    with (mock.patch(
            'src.glue.search_keyword_performance.processor.load_raw_data')
            as mock_load_raw_data):
        mock_load_raw_data.return_value = raw_data
        with (mock.patch(
                'src.glue.search_keyword_performance.processor.write_output')
                as mock_write_output):
            processor.generate_perf_report(
                'test-bucket', 'test-key', spark_session)
            mock_load_raw_data.assert_called_once_with(
                's3://test-bucket/test-key', spark_session)
            mock_write_output.assert_called_once()
            output_df, output_bucket = mock_write_output.call_args.args
            expected_output = spark_session.createDataFrame([
                ('google.com', 'ipod', 480.0),
                ('bing.com', 'zune', 250.0),
                ('yahoo.com', 'cd player', 0.0),
            ], schema=['search_engine_domain', 'search_keyword', 'revenue'])
            assert sorted(output_df.collect()) == sorted(
                expected_output.collect())
            assert output_bucket == 'test-bucket'
