from pyspark.sql import SparkSession, DataFrame
import logging

logger = logging.getLogger(__name__)


class SparkApp():
    def __init__(self, app_name: str):
        """ Initialize spark instance

        :param app_name: name of the spark application
        :type app_name: str
        """
        self.app_name = app_name
        self.session = None

    def __enter__(self):
        """ Method for entering context manager"""
        logger.info('Initiating spark instance')
        self.session = (SparkSession.builder
                        .appName(self.app_name)
                        .master('local[*]')
                        .getOrCreate())

        sc = self.session.sparkContext
        sc.setLogLevel('WARN')
        return self

    def __exit__(self, exc_type=None, exc_val=None, exc_tb=None):
        """Method for exiting context manager"""
        if exc_type is not None:
            logger.error('Interrupting spark instance execution')
        else:
            logger.info('Stopping spark instance')
        self.session.stop()
        logger.info('Stoppped spark instance')

    def read(self, path: str, **opts) -> DataFrame:
        """Method for reading a CSV file into a DataFrame.

        :param path: path to the CSV file
        :type path: str
        :return: The DataFrame created from the CSV file
        :rtype: DataFrame
        """
        df = (self.session
              .read
              .option('sep', opts.get('sep', ','))
              .option("header", True)
              .option("inferSchema", True)
              .format(opts.get('format', 'csv'))
              .load(path))
        logger.info(f'Creating dataframe from path:{path}')
        return df
