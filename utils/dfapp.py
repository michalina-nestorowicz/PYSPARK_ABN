import pyspark.sql
from pyspark.sql.functions import col
import logging
from typing import Dict, List

logger = logging.getLogger(__name__)

class DFApp():
    def __init__(self,df: pyspark.sql.DataFrame):
        self.df = df

    def rename_data(self, column_to_rename_dict: Dict[str,str]) -> pyspark.sql.DataFrame:
        """Method renames specific columns in Dataframe. Renames key to value in provided dictionary

        :param column_to_rename_dict: Dictionary with key as old column name and value as a new column name
        :type column_to_rename_dict: Dict[str,str]
        :return: Returns renamed Dataframe
        :rtype: pyspark.sql.DataFrame
        """

        for key, value in column_to_rename_dict.items():
            self.df = self.df.withColumnRenamed(key, value)
        return self.df

    def filter_data(self, columns_to_filter: Dict[str,str]) -> pyspark.sql.DataFrame:
        """Method filters specific columns in Dataframe. Filters data from the given dictionary. It uses key as a column
     name, and value as a list that column value should be equal to

        :param columns_to_filter:  Dictionary with key as column name and value is a list that column value should be equal to
        :type columns_to_filter: Dict[str,str]
        :return: Returns filtered DataFrame
        :rtype: pyspark.sql.DataFrame    
        """
        for key, value in columns_to_filter.items():
            self.df = self.df.filter(col(key).isin(value))
        return self.df


    def select_data(self, columns_to_select: List) -> pyspark.sql.DataFrame:
        """Method return dataframe with only selected columns

        :param columns_to_select:  A list of columns to select
        :type columns_to_select: List
        :return: Returns Dataframe with selected columns
        :rtype: pyspark.sql.DataFrame
        """
        self.df = self.df.select(*columns_to_select)
        return self.df

