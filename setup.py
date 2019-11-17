from setuptools import setup, find_packages

setup(
    name="bank_archive",
    packages=find_packages(),
    install_requires=["tabula-py", "pymupdf", "requests", "pandas", "lxml"],
)
