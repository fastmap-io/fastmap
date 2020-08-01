import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="pmap",
    version="0.0.1",
    author="pmap.io team",
    author_email="contact@pmap.io",
    description="Parallelize map via the pmap.io cloud service. Minimal-setup and no frameworks to learn",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/pmap/pmap",
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
