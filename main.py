import logging.config
import sys
from utils.sparkapp import SparkApp
import yaml
import os
from utils.os_functions import get_arguments, check_file_correct
from utils.dfapp import DFApp

logger = logging.getLogger(__name__)

def main():
    """ Main function. Here function gets arguments provided by user and creates 2 dataframes from provided csv path.
    Filters data in dataset one based on list of countries provided by user,
    selects only necessary columns in both dataFrames and joins both dataframes.
    Saves final dataframe in client_data folder.
    """
    list_countries, path_one, path_two = get_arguments()
    check_file_correct(path_list=[path_one,path_two], format='csv')

    with SparkApp('Codac') as spark:
        df_one = DFApp(spark.read(path_one))
        df_two = DFApp(spark.read(path_two))
        
        column_to_rename_dict = {'cc_t': 'credit_card_type',
                                 'id': 'client_identifier',
                                  'btc_a': 'bitcoin_address'}
        selected_columns_one = ['id', 'email', 'country']
        selected_columns_two = ['id','btc_a','cc_t']
        columns_to_filter = {'country': list_countries}
        df_one.select_data(selected_columns_one)
        df_two.select_data(selected_columns_two)
        df_one.filter_data(columns_to_filter)
        joined_df = DFApp(df_one.df.join(df_two.df, ['id']))
        joined_df.rename_data(column_to_rename_dict)
        results_path = os.path.join(os.getcwd(), 'client_data')
        joined_df.df.coalesce(1).write.option('header', 'true').format('csv').mode('overwrite').save(results_path)

        
if __name__ =='__main__':
    with open (f'./config/logging.yml') as cf:
        logging_conf = yaml.safe_load(cf)
        logging.config.dictConfig(logging_conf)
        logger = logging.getLogger(__name__)

        main()

