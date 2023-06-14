from setuptools import find_packages, setup

setup(
    name="biomassrecovery",
    version="0.0.2",
    author="Simon V. Mathis (AI4ER CDT, University of Cambridge)",
    author_email="author@example.com",
    description="A short description of the project.",
    url="url-to-github-page",
    packages=find_packages(),
    test_suite="src.tests.test_all.suite",
)
