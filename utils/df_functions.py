import pyspark.sql
from pyspark.sql.functions import col
import logging


logger = logging.getLogger(__name__)

def rename_data(input_df: pyspark.sql.DataFrame, column_to_rename_dict: dict) -> pyspark.sql.DataFrame:
    """_summary_

    :param input_df: _description_
    :type input_df: pyspark.sql.DataFrame
    :param column_to_rename_dict: _description_
    :type column_to_rename_dict: dict
    :return: _description_
    :rtype: pyspark.sql.DataFrame
    """
    for key, value in column_to_rename_dict.items():
        input_df = input_df.withColumnRenamed(key, value)
    return input_df


def filter_data(input_df: pyspark.sql.DataFrame, columns_to_filter: dict) -> pyspark.sql.DataFrame:
    for key, value in columns_to_filter.items():
        input_df = input_df.filter(col(key).isin(value))
    return input_df

def select_data(input_df: pyspark.sql.DataFrame, columns_to_select: list):
    input_df = input_df.select(*columns_to_select)
    return input_df
