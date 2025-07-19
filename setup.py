from setuptools import setup, find_packages

VERSION = "2.0.0"  # 版本更新

def get_requirements():
    """Read requirements from file."""
    return [
        "python-dateutil>=2.8.0", 
        "typing-extensions>=3.7.0",
        "asyncio>=3.4.3",  # 新增异步支持
        "concurrent.futures>=3.1.0"  # 新增并发支持
    ]

def get_long_description():
    """Get long description from README."""
    with open("README.md", "r") as f:
        return f.read()

def get_package_data():
    """Get package data files."""
    return {
        'src': ['config/*.json'],  # 包含配置文件
    }

setup(
    name="test-airflow-mini",
    version=VERSION,
    packages=find_packages(),
    install_requires=get_requirements(),
    package_data=get_package_data(),
    long_description=get_long_description(),
    long_description_content_type="text/markdown",  # 新增
    author="Test Author",
    author_email="test@example.com",  # 新增
    description="A minimal test repository for knowledge graph construction testing",  # 新增
    url="https://github.com/test/test-airflow-mini",  # 新增
    classifiers=[  # 新增分类器
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
    ],
    python_requires=">=3.7",
    keywords="airflow workflow dag testing",  # 新增关键词
    project_urls={  # 新增项目链接
        "Bug Reports": "https://github.com/test/test-airflow-mini/issues",
        "Source": "https://github.com/test/test-airflow-mini",
    }
)
