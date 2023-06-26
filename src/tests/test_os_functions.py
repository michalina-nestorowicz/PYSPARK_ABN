import pytest
import argparse
import logging
from src.codac_spark.utils.os_functions import get_arguments, check_file_correct
from unittest import mock

logger = logging.getLogger(__name__)


@mock.patch('src.codac_spark.utils.os_functions.argparse.ArgumentParser')
def test_get_arguments_valid_arguments(mock_parser):
    mock_instance = mock_parser.return_value
    mock_parser.add_argument = mock.Mock()

    mock_instance.parse_args.return_value = argparse.Namespace(
        personal='personal_data.csv',
        financial='financial_data.csv',
        country=['USA', 'Canada']
    )
    result = get_arguments()
    mock_instance.parse_args.assert_called_once()
    expected_result = (['USA', 'Canada'], 'personal_data.csv', 'financial_data.csv')
    assert result == expected_result


@mock.patch('src.codac_spark.utils.os_functions.argparse.ArgumentParser')
def test_get_arguments_missing_paths_arguments_raises_error(mock_parser):
    mock_instance = mock_parser.return_value
    mock_parser.add_argument = mock.Mock()

    mock_instance.parse_args.return_value = argparse.Namespace(
        country=['USA', 'Canada']
    )
    with pytest.raises(AttributeError):
        get_arguments()


@mock.patch('src.codac_spark.utils.os_functions.os.path.exists')
def test_check_file_correct(mock_exists):
    mock_exists.return_value = True
    path_list = ['/path/to/file1.txt', '/path/to/file2.txt']
    file_format = '.txt'
    assert check_file_correct(path_list, file_format) is None


@mock.patch('src.codac_spark.utils.os_functions.os.path.exists')
def test_check_file_not_exists_raises_error(mock_exists):
    mock_exists.return_value = False
    path_list = ['/path/to/file1.txt', '/path/to/file2.txt']
    file_format = '.txt'
    with pytest.raises(OSError):
        check_file_correct(path_list, file_format)


@mock.patch('src.codac_spark.utils.os_functions.os.path.exists')
def test_check_file_format_not_correct_raises_error(mock_exists):
    mock_exists.return_value = True
    path_list = ['/path/to/file1.txt', '/path/to/file2.txt']
    file_format = '.csv'
    with pytest.raises(OSError):
        check_file_correct(path_list, file_format)
