import logging.config
import os
from typing import List, Dict
from dataclasses import dataclass
import yaml
from utils.os_functions import get_arguments, check_file_correct
from utils.dfapp import DFApp
from utils.sparkapp import SparkApp

logger = logging.getLogger(__name__)


@dataclass
class DFConfig:
    """Data for reading dataframe donfig file"""
    column_to_rename_dict: Dict
    selected_columns_df_one: List
    selected_columns_df_two: List
    columns_to_filter: Dict

    @classmethod
    def load_config(cls, config_path: str, list_countries: List):
        """Load DataFrame configuration from a YAML file

        :param config_path:  Path to the YAML configuration file
        :type config_path: str
        :param list_countries: List of countries for filtering
        :type list_countries: List
        :return: An instance of the DFConfig class containing the loaded configuration
        :rtype: DFConfig
        """
        with open(config_path) as cs:
            try:
                df_config = yaml.safe_load(cs)
            except yaml.YAMLError as exc:
                logging.error(f'Error while parsing YAML file {exc}')
            logging.info(f'Loaded config file {cs}')

        column_to_rename_dict = df_config['column_to_rename_dict']
        selected_columns_df_one = df_config['selected_columns_df_one']
        selected_columns_df_two = df_config['selected_columns_df_two']
        columns_to_filter = {'country': list_countries}

        logging.info('Returning Dataframe configurations arguments')
        return DFConfig(column_to_rename_dict, selected_columns_df_one,
                        selected_columns_df_two, columns_to_filter)


def main():
    """ Main function. Here function gets arguments provided by user
    and creates 2 dataframes from provided csv path.
    Filters data in dataset one based on list of countries provided by user,
    selects only necessary columns in both dataFrames and joins both dataframes.
    Saves final dataframe in client_data folder.
    """
    list_countries, path_one, path_two = get_arguments()
    check_file_correct(path_list=[path_one, path_two], format='csv')
    config = DFConfig.load_config(config_path='src/codac_spark/config/df.yml', list_countries=list_countries)

    with SparkApp('Codac') as spark:
        df_one = DFApp(spark.read(path_one))
        logging.info('Created DataFrame from dataset one')
        df_two = DFApp(spark.read(path_two))
        logging.info('Created DataFrame from dataset two')

        df_one.select_data(config.selected_columns_df_one)
        df_two.select_data(config.selected_columns_df_two)
        df_one.filter_data(config.columns_to_filter)
        logging.info('Dataframes data filtered')

        joined_df = DFApp(df_one.df.join(df_two.df, ['id']))
        joined_df.rename_data(config.column_to_rename_dict)
        logging.info('Dataframes joined by id column')

        results_path = os.path.join(os.getcwd(), 'client_data')
        joined_df.df.coalesce(1).write.option('header', 'true').format('csv').mode('overwrite').save(results_path)
        logging.info(f'Dataframes data save to: {results_path}')


if __name__ == '__main__':
    with open('src/codac_spark/config/logging.yml') as cf:
        try:
            logging_conf = yaml.safe_load(cf)
        except yaml.YAMLError as exc:
            logging.error(f'Error while parsing YAML file: {exc}')
        logging.config.dictConfig(logging_conf)

        main()
