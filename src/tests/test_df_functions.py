import pytest
import chispa
from src.codac_spark.utils.dfapp import DFApp
import logging 

logger = logging.getLogger(__name__)

class TestDFApp:
    source_headers = ['test1', 'test2', 'test3']
    source_data = [(1, 'country1', 'account_type_1'),
                 (2, 'country1','account_type_2'),
                 (3, 'country2', 'account_type_3'),
                 (4, 'country3', 'account_type_4')]
    renamed_headers = ['New1', 'New2', 'New3']

    @pytest.fixture(name='source_df')
    def fixture_source_df(cls, spark_session):
        return spark_session.createDataFrame(cls.source_data, cls.source_headers)


    @pytest.fixture(name='renamed_one_df')
    def fixture_renamed_one_df(cls,spark_session):
        return spark_session.createDataFrame(cls.source_data, ['new','test2', 'test3'])
    
    @pytest.fixture(name='renamed_all_df')
    def fixture_renamed_all_df(cls,spark_session):
        return spark_session.createDataFrame(cls.source_data, cls.renamed_headers)
    
    @pytest.fixture(name='expected_df_2_rows')
    def fixture_expected_df_2rows(cls,spark_session):
        data = [(1, 'country1', 'account_type_1'),
                 (2, 'country1','account_type_2')]
        return spark_session.createDataFrame(data,cls.source_headers)

    @pytest.fixture(name='expected_df_1_row')
    def fixture_expected_df_1rows(cls, spark_session):
        data = [(1, 'country1', 'account_type_1')]
        return spark_session.createDataFrame(data,cls.source_headers)
    
    #test rename
    def test_rename_data_one_column_change_one(self, renamed_one_df, source_df):
        source_df_app = DFApp(source_df)
        renamed_columns_dict_one = {'test1': 'new'}
        test_df_app_one = source_df_app.rename_data(renamed_columns_dict_one)

        chispa.assert_df_equality(renamed_one_df, test_df_app_one,  ignore_row_order=True)

    def test_rename_data_all_columns_change_all(self, renamed_all_df, source_df):
        source_df_app = DFApp(source_df)
        renamed_columns_dict_all = {self.source_headers[i]: self.renamed_headers[i] for i in range(len(self.source_headers))}
        test_df_app_all = source_df_app.rename_data(renamed_columns_dict_all)

        chispa.assert_df_equality(renamed_all_df, test_df_app_all,  ignore_row_order=True)

    def test_rename_no_column_no_change(self, source_df):
         source_df_app = DFApp(source_df)
         renamed_wrong_column = {'wrong': 'wrong_new_name'}
         test_df_app_none = source_df_app.rename_data(renamed_wrong_column)

         chispa.assert_df_equality(source_df, test_df_app_none,  ignore_row_order=True)

    #test Filter
    def test_filter_df_return_filtered_one_column(self,expected_df_2_rows,source_df):
        data_to_filter = {'test2': ['country1', 'country4']}
        source_df_app = DFApp(source_df)
        test_df_app_filtered_one_column = source_df_app.filter_data(data_to_filter)
        
        chispa.assert_df_equality(test_df_app_filtered_one_column, expected_df_2_rows,  ignore_row_order=True)


    def test_filter_df_return_filtered_two_columns(self,expected_df_1_row,source_df):
        data_to_filter = {'test2': ['country1', 'country4'], 'test3':'account_type_1'}
        source_df_app = DFApp(source_df)
        test_df_app_filtered_two_column = source_df_app.filter_data(data_to_filter)

        chispa.assert_df_equality(test_df_app_filtered_two_column, expected_df_1_row,  ignore_row_order=True)

   


