#!/usr/bin/env python3

import sys
import distutils.util
from setuptools import setup

# check Python's version
if sys.version_info < (3, 6):
    sys.stderr.write('This module requires at least Python 3.6\n')
    sys.exit(1)

# check linux platform
platform = distutils.util.get_platform()
if not platform.startswith('linux'):
    sys.stderr.write("This module is not available on %s\n" % (platform))
    sys.exit(1)

classif = [
    'Development Status :: 5 - Production/Stable',
    'Environment :: Console',
    'Intended Audience :: Developers',
    'License :: OSI Approved :: GPLv3 License',
    'Natural Language :: English',
    'Operating System :: POSIX :: Linux',
    'Programming Language :: Python',
    'Programming Language :: Python :: 3.6',
    'Programming Language :: Python :: 3.7',
    'Programming Language :: Python :: 3.8',
    'Topic :: Software Development :: Libraries :: Python Modules',
]

# Do setup
setup(
    name='mirrors',
    version='0.0.1',
    description='Mirrors common library',
    author='Fpemud',
    author_email='fpemud@sina.com',
    license='GPLv3 License',
    platforms='Linux',
    classifiers=classif,
    url='http://github.com/fpemud/mirrors',
    download_url='',
    packages=find_packages("python3"),
    package_dir={"": "python3"},
)
