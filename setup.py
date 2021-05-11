import os
import re
import setuptools
import sys

if sys.version_info[:2] < (3, 6):
    print("ERROR: this package requires Python 3.6 or later!")
    sys.exit(1)
if sys.version_info[:2] >= (3, 9):
    print("ERROR: this package cannot run on Python 3.9 or later!")
    # TODO. It's a pickling issue. Maybe dill needs a PR?
    sys.exit(1)


with open("README.md", "r") as fh:
    long_description = fh.read()

with open(os.path.join("fastmap", "sdk_lib.py")) as f:
    version = re.search(r"^CLIENT_VERSION \= \"([0-9.]+)\"", f.read(),
                        re.MULTILINE).group(1)

url_base = "https://github.com/fastmap-io/fastmap"
download_url = '%s/archive/fastmap-%s.tar.gz' % (url_base, version)

setuptools.setup(
    name="fastmap",
    version=version,
    author="fastmap.io team",
    author_email="scott@fastmap.io",
    description="Fastmap offloads and parallelizes arbitrary Python code "
                "via the free and open-source fastmap-server.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url=url_base,
    download_url=download_url,
    packages=setuptools.find_packages(),
    scripts=[
        "scripts/fastmap",
    ],
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6<3.9',
    install_requires=[
        "dill>=0.3.2,<0.4",
        "msgpack>=1.0.0,<1.1.0",
        "requests>=2.24,<3.0",
        "tabulate>=0.8.7,<0.9.0",
    ],
)
