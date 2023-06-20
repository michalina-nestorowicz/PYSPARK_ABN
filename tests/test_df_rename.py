import pytest
import chispa
from utils.dfapp import DFApp
from pyspark.sql import SparkSession
import logging 
logger = logging.getLogger(__name__)

class TestDFRenameApp:
    spark = None
    source_headers = ['test1', 'test2', 'test3']
    source_data = [['value1', 'value2', 'value3']]
    renamed_headers = ['New1', 'New2', 'New3']

    @classmethod
    def setup_class(cls):
        cls.spark = SparkSession.builder.master("local[*]").appName("chispa").getOrCreate()
        cls.source_df = cls.spark.createDataFrame(cls.source_data, cls.source_headers)

    @classmethod
    def teardown_class(cls):
        cls.spark.stop()

    @pytest.fixture(name='renamed_one_df')
    def fixture_renamed_one_df(cls):
        return cls.spark.createDataFrame(cls.source_data, ['new','test2', 'test3'])
    
    @pytest.fixture(name='renamed_all_df')
    def fixture_renamed_all_df(cls):
        return cls.spark.createDataFrame(cls.source_data, cls.renamed_headers)
    
    def test_rename_data_one_column_change_one(self, renamed_one_df):
        source_df_app = DFApp(self.source_df)
        renamed_columns_dict_one = {'test1': 'new'}
        test_df_app_one = source_df_app.rename_data(renamed_columns_dict_one)
        chispa.assert_df_equality(renamed_one_df, test_df_app_one)

    def test_rename_data_all_columns_change_all(self, renamed_all_df):
        source_df_app = DFApp(self.source_df)
        renamed_columns_dict_all = {self.source_headers[i]: self.renamed_headers[i] for i in range(len(self.source_headers))}
        test_df_app_all = source_df_app.rename_data(renamed_columns_dict_all)
        chispa.assert_df_equality(renamed_all_df, test_df_app_all)

    def test_rename_no_column_no_change(self):
         source_df_app = DFApp(self.source_df)
         renamed_wrong_column = {'wrong': 'wrong_new_name'}
         test_df_app_none = source_df_app.rename_data(renamed_wrong_column)
         chispa.assert_df_equality(self.source_df, test_df_app_none)


   


