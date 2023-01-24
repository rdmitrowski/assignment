from setuptools import setup, find_packages
import pathlib

here = pathlib.Path(__file__).parent.resolve()

setup(
    name='BitcoinTrading',
    version='1.0',
    description='Project that join load two files, prepare filering/modification operation and save ouput in file',
    #package_dir={"": "project1"},
    #packages=find_packages(where="src"),
    packages=find_packages(exclude=['tests']),
    install_requires=['pytest','pyspark','chispa']
    #test_suite="tests"
    #entry_points={'console_scripts: [run = NewBitcoinTrades.main:run]'}
    )

#python setup.py sdist bdist_wheel