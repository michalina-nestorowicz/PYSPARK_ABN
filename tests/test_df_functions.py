import utils.df_functions as functions
import chispa
from pyspark.sql import SparkSession

spark = (SparkSession.builder.master("local").appName("chispa").getOrCreate())


def test_rename_data():
    test_headers = ['test1', 'test2', 'test3']
    df = spark.createDataFrame([('value1', 'value2', 'value3')], test_headers)
    columns_to_rename = {'test1': 'New1', 'test2': 'New2', 'test3': 'New3'}
    result = functions.rename_data(df, columns_to_rename)
    expected_headers = ['New1', 'New2', 'New3']
    expected = spark.createDataFrame([('value1', 'value2', 'value3')], expected_headers)

    chispa.assert_df_equality(result, expected)


def test_filter_data():
    test_data = [(1, 'Poland', True, 'account_type_1'),
                 (2, 'England', True, 'account_type_2'),
                 (3, 'Poland', False, 'account_type_3')]
    test_headers = ['id', 'country','active', 'account_type']
    df = spark.createDataFrame(test_data, test_headers)
    columns_to_filter = {'country': ['Poland', 'France'], 'active': [True]}
    result = functions.filter_data(df, columns_to_filter)
    expected = spark.createDataFrame([(1, 'Poland', True, 'account_type_1')], test_headers)
    chispa.assert_df_equality(result, expected)


def test_select_data():
    test_data = [(1, 'value_test', 'value2', 'value_not_selected')]
    test_headers = ['id', 'column_test', 'column2', 'column_not_selected']
    df = spark.createDataFrame(test_data,test_headers)
    columns_to_select = ['id', 'column2']
    result = functions.select_data(df, columns_to_select)
    expected = spark.createDataFrame([(1, 'value2')], columns_to_select)
    chispa.assert_df_equality(result, expected)