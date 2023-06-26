import argparse
import logging
import os
from typing import List, Tuple

logger = logging.getLogger(__name__)


def get_arguments() -> Tuple:
    """Functions parses arguments provided by command line and returns them

    :return: A tuple containing the country list, the path for first,
      personal dataset, and the second path for financial dataset
    :rtype: Tuple
    """
    logger.info('Acquiring arguments')
    parser = argparse.ArgumentParser()
    parser.add_argument('--personal', type=str, required=True, help='personal dataset path')
    parser.add_argument('--financial', type=str, required=True, help='financial dataset path')
    parser.add_argument(
        "--country",
        nargs="*",  # 0 or more values expected => creates a list
        type=str,
        help='List of countries',
        default=['Netherlands']  # default if nothing is provided
    )
    args = parser.parse_args()
    logger.info(f'Arguments aquired: {args}')
    country_list = args.country if args.country else ['Netherlands']
    return country_list, args.personal, args.financial


def check_file_correct(path_list: List, format: str):
    """Checks if the given file paths are correct and have the specified format.

    :param path_list:  A list of file paths
    :type path_list: str
    :param format: The correct file format
    :type format: str
    :raises OSError: If any of the file paths are incorrect or do not have the specified format.
    """
    for path in path_list:
        if not os.path.exists(path) or not path.endswith(format):
            logger.critical('Paths files incorrect')
            raise OSError('Incorrect file paths')
