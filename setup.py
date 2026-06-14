from pathlib import Path
from setuptools import setup, find_packages


def read_requirements() -> list[str]:
    req_file = Path(__file__).parent / "requirements.txt"
    if not req_file.exists():
        return []
    reqs = []
    for ln in req_file.read_text(encoding="utf-8").splitlines():
        ln = ln.strip()
        if ln and not ln.startswith("#"):
            reqs.append(ln)
    return reqs


readme = Path(__file__).parent / "README.md"
long_description = readme.read_text(encoding="utf-8") if readme.exists() else ""

setup(
    name="supertable",
    version="2.2.3",
    description="SuperTable — versioned data lake library for SQL analytics on Parquet + Redis.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    author="Levente Kupas",
    author_email="lkupas@kladnasoft.com",
    license="Super Table Public Use License (STPUL) v1.0",
    python_requires=">=3.10",
    packages=find_packages(include=["supertable", "supertable.*"]),
    include_package_data=True,
    install_requires=read_requirements(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3 :: Only",
        "License :: Other/Proprietary License",
        "Operating System :: OS Independent",
    ],
    extras_require={
        "s3": ["boto3>=1.34,<2.0"],
        "minio": ["minio>=7.2,<8.0"],
        "azure": ["azure-storage-blob>=12.26.0"],
        "gcp": ["google-cloud-storage>=3.1.0"],
        "all-cloud": [
            "boto3>=1.34,<2.0",
            "minio>=7.2,<8.0",
            "azure-storage-blob>=12.26.0",
            "google-cloud-storage>=3.1.0",
        ],
        "all": [
            "boto3>=1.34,<2.0",
            "minio>=7.2,<8.0",
            "azure-storage-blob>=12.26.0",
            "google-cloud-storage>=3.1.0",
        ],
    },
    entry_points={
        "console_scripts": [
            "supertable-demo-quickstart=supertable.demo.quickstart.controller:main",
            "supertable-demo-webshop-generate=supertable.demo.webshop.generate:main",
            "supertable-demo-webshop-load=supertable.demo.webshop.load:main",
            "supertable-demo-webshop-topup=supertable.demo.webshop.topup:main",
        ],
    },
)
