from setuptools import setup, find_packages
import pathlib

here = pathlib.Path(__file__).parent.resolve()

setup(
    name='BitcoinTrading',
    version='1.0',
    description='Project that join load two files, prepare filering/modification operation and save ouput in file',
    packages=find_packages(exclude=['tests']),
    install_requires=['pytest','pyspark', 'chispa']
    )