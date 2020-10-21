import os
import re
import setuptools
import sys

if sys.version_info[:2] < (3, 7):
    print("ERROR: this package requires Python 3.7 or later!")
    sys.exit(1)

with open("README.md", "r") as fh:
    long_description = fh.read()

with open(os.path.join("fastmap", "lib.py")) as f:
    version = re.search(r"^CLIENT_VERSION \= \"([0-9.]+)\"", f.read(),
                        re.MULTILINE).group(1)

with open("requirements.txt") as f:
    requirements = map(str.strip, f.readlines())
    requirements = list(filter(lambda l: not l.startswith("#"), requirements))


setuptools.setup(
    name="fastmap",
    version=version,
    author="fastmap.io team",
    author_email="scott@fastmap.io",
    description="Fastmap is a drop-in replacement for `map` that " \
                "parallelizes your code on the cloud.",
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
    install_requires=requirements,
)
