#from distutils.core import setup
from setuptools import setup

setup(
    name = 'dfply',
    version = '0.1.1',
    author = 'Kiefer Katovich',
    author_email = 'kiefer.katovich@gmail.com',
    packages = [
        'dfply',
        'dfply.vendor',
        'dfply.vendor.pandas_ply',
        'dfply.data'
    ],
    description = 'dplyr-style piping operations for pandas dataframes',
    long_description = open('README.md').read(),
    license = 'GNU General Public License v3.0',
    url = 'https://github.com/kieferk/dfply'
)
