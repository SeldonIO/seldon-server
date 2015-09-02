#!/usr/bin/env python
import os
from setuptools import setup, find_packages

setup(name='seldon',
      version='1.0',
      description='Seldon Python Utilities',
      author='Clive Cox',
      author_email='support@seldon.io',
      url='https://github.com/SeldonIO/seldon-server',
      license='Apache 2 License',
      install_requires=['wabbit_wappa','xgboost','kazoo','sklearn','scipy','numpy','boto','filechunkio'],
      packages=['seldon', 'seldon.pipeline'],
      )
