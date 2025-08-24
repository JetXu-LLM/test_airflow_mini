from setuptools import setup, find_packages

VERSION = "1.0.0"

def get_requirements():
    """Read requirements from file."""
    return ["python-dateutil>=2.8.0", "typing-extensions>=3.7.0"]

def get_long_description():
    """Get long description from README."""
    with open("README.md", "r") as f:
        return f.read()

setup(
    name="test-airflow-mini",
    version=VERSION,
    packages=find_packages(),
    install_requires=get_requirements(),
    long_description=get_long_description(),
    author="Test Author",
    python_requires=">=3.7"
)
