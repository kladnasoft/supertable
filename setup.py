from pathlib import Path
from setuptools import setup, find_packages

def read_requirements() -> list[str]:
    req_file = Path(__file__).parent / "requirements.txt"
    if not req_file.exists():
        return []
    lines = req_file.read_text(encoding="utf-8").splitlines()
    reqs = []
    for ln in lines:
        ln = ln.strip()
        if not ln or ln.startswith("#"):
            continue
        reqs.append(ln)
    return reqs

setup(
    name="supertable",
    version="1.2.0",
    packages=find_packages(include=["supertable", "supertable.*"]),
    include_package_data=True,
    author="Levente Kupas",
    author_email="lkupas@kladnasoft.com",
    description="A high-performance, lightweight transaction cataloging system designed for ultimate efficiency.",
    license="Super Table Public Use License (STPUL) v1.0",
    long_description=Path("README.md").read_text(encoding="utf-8"),
    long_description_content_type="text/markdown",
    url="https://github.com/kladnasoft/supertable",
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: Other/Proprietary License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.10",
    install_requires=read_requirements(),
    extras_require={
        # Optional cloud backends (install on demand)
        "s3": ["boto3>=1.34"],
        "minio": ["minio>=7.2"],
        "azure": ["azure-storage-blob>=12.24"],
        # "gcs": ["google-cloud-storage>=3.1.0"],  # enable once backend exists
        "all": ["boto3>=1.34", "minio>=7.2", "azure-storage-blob>=12.24"],
    },
)
