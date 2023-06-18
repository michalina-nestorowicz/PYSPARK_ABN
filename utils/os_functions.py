import argparse
import logging
import os
from typing import List

logger = logging.getLogger(__name__)

def get_arguments():
    """Functions parses arguments provided by command line to correct variable

    :return: _description_
    :rtype: _type_
    """
    logger.info(f'Acquiring arguments')
    parser = argparse.ArgumentParser()
    parser.add_argument('--one', type=str, required=True, help = 'first dataset')
    parser.add_argument('--two', type=str, required=True)
    parser.add_argument(
        "--country",  
        nargs="*",  # 0 or more values expected => creates a list
        type=str,
        default=['Netherlands']  # default if nothing is provided
    )
    args = parser.parse_args()
    logger.info(f'Arguments aquired: {args}')
    country_list = args.country if args.country else ['Netherlands']
    return country_list, args.one, args.two

def check_file_correct(path_list, format):
    for path in path_list:
        if not os.path.exists(path) or not path.endswith(format): 
            logger.critical('Paths files incorrect')
            raise OSError('Incorrect file paths')
                    
