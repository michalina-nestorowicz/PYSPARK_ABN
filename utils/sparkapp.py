from pyspark.sql import SparkSession, DataFrame
from pyspark import SparkConf
import logging

logger = logging.getLogger(__name__)

class SparkApp():
    def __init__(self, app_name: str):
        self.app_name = app_name

    def __enter__(self):
        logger.info(f'Initiating spark instance')
        self.session = (SparkSession.builder
                        .appName(self.app_name)
                        .master('local[*]')
                        .getOrCreate())
        
        sc = self.session.sparkContext
        sc.setLogLevel('WARN')
        return self
    
    def __exit__(self,exc_type = None, exc_val = None, exc_tb = None):
        if exc_type is not None:
            logger.error(f'Interrupting spark instance execution')
        else:
            logger.info(f'Stopping spark instance')
        self.session.stop()
        logger.info(f'Stoppped spark instance')

    def read(self,path) -> DataFrame:
        df =  (self.session
            .read
            .option("sep", ",")
            .option("header", True)
            .option("inferSchema", True)
            .csv(path)
            )
        logger.info(f'Creating dataframe from path:{path}')
        return df
        




