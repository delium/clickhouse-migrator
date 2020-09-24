from setuptools import find_packages, setup

setup(name='clickhouse-migrator',
      packages=find_packages(),
      version='1.0.0',
      description='Migration library for Clickhouse',
      author='Simon Roy, Arvind Chinniah',
      install_requires=['pandas', 'clickhouse_driver'],
      tests_require=['pytest==5.1.1'],
      test_suite='tests',
      license='MIT')
