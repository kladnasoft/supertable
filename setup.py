from setuptools import setup, find_packages

setup(
    name="supertable",
    version="1.0.0",
    packages=find_packages(include=["supertable", "supertable.*"]),
    include_package_data=True,
    author="Levente Kupas",
    author_email="lkupas@kladnasoft.com",
    description="A high-performance, lightweight transaction cataloging system designed for ultimate efficiency.",
    license="Super Table Public Use License (STPUL) v1.0",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    url="https://github.com/kladnasoft/supertable",
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: Other/Proprietary License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.10",
)
