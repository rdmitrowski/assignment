from setuptools import setup, find_packages
# import pathlib

# here = pathlib.Path(__file__).parent.resolve()

setup(
    name='BitcoinTrading',
    author="Rafal Dmitrowski",
    author_email="rafal.dmitrowski@capgemini.com",
    version='1.0',
    description='Project that load and join two files, \
                 prepare filtering/modification operation \
                 and save output in file',
    packages=find_packages(exclude=['tests']),
    install_requires=['pytest', 'pyspark', 'chispa']
    )
