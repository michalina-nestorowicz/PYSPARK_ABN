import pytest
import chispa
from src.codac_spark.utils.dfapp import DFApp
from pyspark.sql import SparkSession
import logging 

logger = logging.getLogger(__name__)

class TestDFFilterApp:
    spark = None
    headers = ['test1', 'test2', 'test3']


    @classmethod
    def setup_class(cls):
        cls.spark = SparkSession.builder.master("local[*]").appName("chispa").getOrCreate()


    @classmethod
    def teardown_class(cls):
        cls.spark.stop()


    @pytest.fixture(name='source_df')
    def fixture_source_df(cls):
        data = [(1, 'country1', 'account_type_1'),
                 (2, 'country1','account_type_2'),
                 (3, 'country2', 'account_type_3'),
                 (4, 'country3', 'account_type_4')]
        return cls.spark.createDataFrame(data,cls.headers)


    @pytest.fixture(name='expected_df_2_rows')
    def fixture_expected_df_2rows(cls):
        data = [(1, 'country1', 'account_type_1'),
                 (2, 'country1','account_type_2')]
        return cls.spark.createDataFrame(data,cls.headers)


    @pytest.fixture(name='expected_df_1_row')
    def fixture_expected_df_1rows(cls):
        data = [(1, 'country1', 'account_type_1')]
        return cls.spark.createDataFrame(data,cls.headers)


    def test_filter_df_return_filtered_one_column(self,expected_df_2_rows,source_df):
        data_to_filter = {'test2': ['country1', 'country4']}
        source_df_app = DFApp(source_df)
        test_df_app_filtered_one_column = source_df_app.filter_data(data_to_filter)\
        
        chispa.assert_df_equality(test_df_app_filtered_one_column, expected_df_2_rows,  ignore_row_order=True)


    def test_filter_df_return_filtered_two_columns(self,expected_df_1_row,source_df):
        data_to_filter = {'column2': ['country1', 'country4'], 'column3':'account_type_1'}
        source_df_app = DFApp(source_df)
        test_df_app_filtered_two_column = source_df_app.filter_data(data_to_filter)

        chispa.assert_df_equality(test_df_app_filtered_two_column, expected_df_1_row,  ignore_row_order=True)

