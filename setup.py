__author__="rob"
__date__ ="$Mar 10, 2011 5:37:46 PM$"

from setuptools import setup,find_packages

setup (
  name = 'pymongo-mr',
  version = '0.1',
  packages = find_packages(),

  install_requires=['pymongo>=1.8'],

  author = 'rob',
  author_email = 'bubblenut@gmail.com',

  summary = 'Multiprocess MongoDB aggregation in Python',
  url = '',
  license = '',
  long_description= 'Long description of the package',

)