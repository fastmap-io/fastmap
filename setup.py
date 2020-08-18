import os
import re
import setuptools
import sys

if sys.version_info[:2] < (3, 7):
    print("ERROR: this package requires Python 3.7 or later!")
    sys.exit(1)

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="fastmap",
    version=re.search("^__version__.*?([0-9.]+)",
                      open(os.path.join('fastmap', '__init__.py')).read(),
                      re.MULTILINE).group(1),
    author="fastmap.io team",
    author_email="contact@fastmap.io",
    description="Fastmap is a distributed drop-in replacement for `map`. It runs faster than the builtin map function in most cases >1 second. Fastmap is adaptively run both locally and the fastmap.io cloud service.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/fastmap-io/fastmap",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6',
    install_requires=[
        "psutil>=5,<6",
    ],
    # extras_require={
    #     'dev': [
    #         "nose>=1,<2",
    #     ]
    # },
    test_suite='nose.collector',
    tests_require=['nose'],
)
