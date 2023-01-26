import argparse
from datetime import datetime


def parse_arguments() -> dict:
    """parse_arguments validate imput parameters and assign it to dict.
    In case of missing parameter(s) there is information about correct run statement

    Returns:
        dict: _description_
    """
    parser = argparse.ArgumentParser(
        description='Merge two dataset, prepare modification on it and save a result')
    parser.add_argument('-u', '--file_users',
                        type=str,
                        help='Name of users file',
                        required=True)
    parser.add_argument('-t', '--file_transactions',
                        type=str,
                        help='Name of transaction file',
                        required=True)
    parser.add_argument('-f', '--filter',
                        type=str,
                        help='Country filter',
                        required=True)
    now = datetime.now()
    parser.add_argument('-out', '--file_output',
                        type=str,
                        help='Optional output file name',
                        required=False,
                        default='output_data_' +
                        now.strftime("%d-%m-%Y_%H%M%S") + '.csv')
    args = parser.parse_args()
    return vars(args)
