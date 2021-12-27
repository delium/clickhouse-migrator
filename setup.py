from setuptools import find_packages, setup

from os import path

this_directory = path.abspath(path.dirname(__file__))
with open(path.join(this_directory, 'README.md'), encoding='utf-8') as f:
    long_description = f.read()

setup(name='clickhouse-migrator',
      packages=find_packages(),
      version='1.0.6',
      description='Migration library for Clickhouse',
      author='Delium Engineering',
      install_requires=['pandas', 'clickhouse_driver'],
      long_description=long_description,
      long_description_content_type='text/markdown',
      url='https://github.com/delium/clickhouse-migrator',
      author_email='oss@delium.co',
      tests_require=['pytest==5.1.1'],
      test_suite='tests',
      license='MIT',
      license_file='LICENSE',
      python_requires='>=3.6')
