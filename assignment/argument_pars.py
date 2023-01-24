import argparse
from datetime import datetime


def parse_arguments():
    parser = argparse.ArgumentParser(
        description='Program parameters')
    parser.add_argument('-file_users', 
                        type=str, 
                        help='Name of users file', 
                        required=True)
    parser.add_argument('-file_transactions', 
                        type=str, 
                        help='Name of transaction file', 
                        required=True)
    parser.add_argument('-filter', 
                        type=str, 
                        help='Country filter', 
                        required=True)
    now = datetime.now()
    parser.add_argument('-file_output', 
                        type=str, 
                        help='Optional output file name', 
                        required=False, 
                        default='output_data_' + 
                        now.strftime("%d-%m-%Y_%H%M%S") + '.csv')
    return vars(parser.parse_args)