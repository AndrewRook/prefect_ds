import os
import re
import sys

from setuptools import setup, find_packages

sys.path.append("prefect_ds")
try:
    from prefect_ds import __version__
except ImportError:
    __version__ = "null"


CLASSIFIERS = [
    "Development Status :: 4 - Beta",
    "Natural Language :: English",
    "License :: OSI Approved :: Apache Software License",
    "Operating System :: OS Independent",
    "Programming Language :: Python :: 3 :: Only"
]
INSTALL_REQUIRES = [
    'prefect >= 0.9.0, <= 0.9.2',
    'pandas >= 0.25.3, <= 0.25.3'
]

EXTRAS_REQUIRE = {
    "dev": ["pytest >= 5.3.2, <= 5.3.2", "pytest-cov >= 2.8.1, <= 2.8.1"]
    }


HERE = os.path.abspath(os.path.dirname(__file__))
with open(os.path.join(HERE, 'README.md'),'r') as f:
    README = f.read()

###################################################################

if __name__ == "__main__":
    setup(
        name="prefect_ds",
        description='Tools for making Prefect work better for typical data science workflows',
        license='Apache License 2.0',
        url='https://github.com/AndrewRook/prefect_ds',
        version=__version__,
        author='Andrew Schechtman-Rook',
        maintainer='Andrew Schechtman-Rook',
        long_description=README,
        long_description_content_type="text/markdown",
        packages=find_packages(where="."),
        classifiers=CLASSIFIERS,
        install_requires=INSTALL_REQUIRES,
        extras_require=EXTRAS_REQUIRE
    )