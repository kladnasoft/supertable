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

readme = (Path(__file__).parent / "README.md")
long_description = readme.read_text(encoding="utf-8") if readme.exists() else ""

setup(
    name="supertable",
    version="1.2.31",
    description="SuperTable revolutionizes data management by integrating multiple basic tables into a single, cohesive framework.",
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
    # Optional cloud backends (each includes redis for locking)
    extras_require={
        "s3": ["boto3>=1.34", "redis>=5.2.1"],
        "minio": ["minio>=7.2", "redis>=5.2.1"],
        "azure": ["azure-storage-blob>=12.24", "redis>=5.2.1"],
        "gcp": ["google-cloud-storage>=3.1.0", "redis>=5.2.1"],
        "all-cloud": [
            "boto3>=1.34",
            "minio>=7.2",
            "azure-storage-blob>=12.24",
            "google-cloud-storage>=3.1.0",
            "redis>=5.2.1",
        ],
    },
    # ⚠️ This exposes the CLI: `supertable config ...`
    entry_points={
        "console_scripts": [
            "supertable=supertable.config.cli:main",
        ],
    },
)
