#from distutils.core import setup
from setuptools import setup

setup(
    name = 'dfply',
    version = '0.0.1',
    author = 'Kiefer Katovich',
    author_email = 'kiefer.katovich@gmail.com',
    packages = [
        'dfply',
        'dfply.vendor',
        'dfply.vendor.pandas_ply',
        'dfply.data'
    ],
    #description = 'functional data manipulation for pandas',
    #long_description = open('README.rst').read(),
    #license = 'Apache License 2.0',
    #url = 'https://github.com/coursera/pandas-ply',
    classifiers = [],
)
