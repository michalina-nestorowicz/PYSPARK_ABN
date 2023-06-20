import logging.config
import sys
from utils.sparkapp import SparkApp
import yaml
import os
from utils.os_functions import get_arguments, check_file_correct
from utils.dfapp import DFApp
from typing import List

logger = logging.getLogger(__name__)

def get_df_config(config_path: str, list_countries:List):
    with open (config_path) as cs:
        df_config = yaml.safe_load(cs)
        logging.info(f'Loaded config file {cs}')

        column_to_rename_dict = df_config['column_to_rename_dict']
        selected_columns_df_one = df_config['selected_columns_df_one']
        selected_columns_df_two = df_config['selected_columns_df_two']
        columns_to_filter = {'country': list_countries}

        logging.info(f'Returning Dataframe configurations arguments')
        return column_to_rename_dict, selected_columns_df_one, selected_columns_df_two, columns_to_filter


def main():
    """ Main function. Here function gets arguments provided by user and creates 2 dataframes from provided csv path.
    Filters data in dataset one based on list of countries provided by user,
    selects only necessary columns in both dataFrames and joins both dataframes.
    Saves final dataframe in client_data folder.
    """
    list_countries, path_one, path_two = get_arguments()
    check_file_correct(path_list=[path_one,path_two], format='csv')

    (column_to_rename_dict,selected_columns_df_one, selected_columns_df_two, columns_to_filter) =  get_df_config(f'./config/df.yml', list_countries)
    
    with SparkApp('Codac') as spark:
        df_one = DFApp(spark.read(path_one))
        logging.info('Created DataFrame from dataset one')
        df_two = DFApp(spark.read(path_two))
        logging.info('Created DataFrame from dataset two')
        

        df_one.select_data(selected_columns_df_one)
        df_two.select_data(selected_columns_df_two)
        df_one.filter_data(columns_to_filter)
        logging.info(f'Dataframes data filtered')

        joined_df = DFApp(df_one.df.join(df_two.df, ['id']))
        joined_df.rename_data(column_to_rename_dict)
        logging.info(f'Dataframes joined by id column')

        results_path = os.path.join(os.getcwd(), 'client_data')
        joined_df.df.coalesce(1).write.option('header', 'true').format('csv').mode('overwrite').save(results_path)
        logging.info(f'Dataframes data save to: {results_path}')

        
if __name__ =='__main__':
    with open (f'./config/logging.yml') as cf:
        logging_conf = yaml.safe_load(cf)
        logging.config.dictConfig(logging_conf)
        logger = logging.getLogger(__name__)

        main()

