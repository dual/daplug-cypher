import os
from pathlib import Path

from setuptools import find_packages, setup


def read_long_description() -> str:
    readme_path = Path(__file__).parent / "README.md"
    with readme_path.open(encoding="utf-8") as fh:
        return fh.read()


setup(
    name="daplug-cypher",
    version=os.getenv("CIRCLE_TAG", "0.1.0"),
    url="https://github.com/dual/daplug-cypher",
    author="Paul Cruse III",
    author_email="paulcruse3@gmail.com",
    description="A Neo4j and Neptune graph normalization layer using the adapter pattern.",
    long_description=read_long_description(),
    long_description_content_type="text/markdown",
    packages=find_packages(),
    python_requires=">=3.9",
    install_requires=[
        "boto3>=1.34",
        "jsonref>=0.2",
        "neo4j>=5.7.0",
        "PyYAML>=5.3",
        "simplejson>=3.17"
    ],
    keywords=[
        "cypher",
        "neptune",
        "graph",
        "bolt",
        "normalizer",
        "schema",
        "sns",
        "event-driven",
        "database",
        "adapter",
        "python-library",
    ],
    project_urls={
        "Homepage": "https://github.com/dual/daplug-cypher",
        "Documentation": "https://github.com/dual/daplug-cypher#readme",
        "Source Code": "https://github.com/dual/daplug-cypher",
        "Bug Reports": "https://github.com/dual/daplug-cypher/issues",
        "CI/CD": "https://circleci.com/gh/dual/daplug-cypher",
    },
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Environment :: Web Environment",
        "Environment :: Console",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: OS Independent",
        "Topic :: Database :: Front-Ends",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3 :: Only",
    ],
    license="Apache License 2.0",
    platforms=["any"],
    zip_safe=False,
)
