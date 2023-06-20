import pytest
import chispa
from utils.dfapp import DFApp
from pyspark.sql import SparkSession


class TestDFApp:
    spark = None
    source_headers = ['test1', 'test2', 'test3']
    source_data = [['value1', 'value2', 'value3']]
    renamed_headers = ['New1', 'New2', 'New3']

    @classmethod
    def setup_class(cls):
        cls.spark = SparkSession.builder.master("local[*]").appName("chispa").getOrCreate()

    @classmethod
    def teardown_class(cls):
        cls.spark.stop()

    @pytest.fixture(name='source_df')
    def fixture_source_df(cls):
        return cls.spark.createDataFrame(cls.source_data, cls.source_headers)

    @pytest.fixture(name='renamed_df')
    def fixture_renamed_df(cls):
        return cls.spark.createDataFrame(cls.source_data, cls.renamed_headers)

    def test_rename_data(self, source_df, renamed_df):
        source_df_app = DFApp(source_df)
        renamed_columns_dict = {self.source_headers[i]: self.renamed_headers[i] for i in range(len(self.source_headers))}
        source_df_app.rename_data(renamed_columns_dict)
        chispa.assert_df_equality(renamed_df, source_df_app.df)
   


