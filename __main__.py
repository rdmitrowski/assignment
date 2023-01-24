# __main__.py
import argparse
from assignment.BitcoinTrading import BitcoinTrading
from assignment.argument_pars import parse_arguments

if __name__ == "__main__":
    BitcoinTrading(parse_arguments())
