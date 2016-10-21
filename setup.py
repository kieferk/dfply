from setuptools import setup, find_packages

setup(
    name = 'dfply',
    version = '0.2.0',
    author = 'Kiefer Katovich',
    author_email = 'kiefer.katovich@gmail.com',
    keywords = 'pandas dplyr',
    packages = find_packages(),
    include_package_data=True,
    package_data={'dfply': ['data/diamonds.csv']},
    package_dir={'dfply':'dfply'},
    install_requires=['numpy', 'pandas', 'six', 'pandas_ply'],
    description = 'dplyr-style piping operations for pandas dataframes',
    long_description = 'See https://github.com/kieferk/dfply/blob/master/README.md for details.',
    license = 'GNU General Public License v3.0',
    url = 'https://github.com/kieferk/dfply',
    test_suite='test',
)
