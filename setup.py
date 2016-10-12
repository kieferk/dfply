from setuptools import setup, find_packages

setup(
    name = 'dfply',
    version = '0.1.9',
    author = 'Kiefer Katovich',
    author_email = 'kiefer.katovich@gmail.com',
    packages = find_packages(),
    include_package_data=True,
    package_data={'': '../data/*.csv'},
    install_requires=['numpy', 'pandas', 'six', 'pandas_ply'],
    description = 'dplyr-style piping operations for pandas dataframes',
    long_description = 'See https://github.com/kieferk/dfply/blob/master/README.md for details.',
    license = 'GNU General Public License v3.0',
    url = 'https://github.com/kieferk/dfply',
    test_suite='test',
)
