#!/usr/bin/env python
import os
from setuptools import setup

with open(os.path.join(os.path.split(__file__)[0], 'requirements.txt')) as f:
    reqs1 = f.readlines()

setup(name='ExampleIngest',
      version='0.1.0',
      description='Support Library for Toms ExampleIngest',
      url='https://bitbucket.org//Tomrepo/ExampleIngest/',
      author='Tom',
      author_email='hello@test.com',
      license='Proprietary',
      zip_safe=False,
      packages=['ExampleIngest'],
      setup_requires=[
          'setuptools>=38.6.0',
          'wheel>=0.31.0'],
      install_requires=[reqs1],
      test_suite='nose.collector',
      tests_require=['nose', 'coverage'],
      keywords='Tom',
      include_package_data=True
      )