"""
Setup configuration for the DataPy ETL Framework.

Provides installation configuration for the DataPy package with all
required dependencies and entry points for CLI usage.
"""

from setuptools import setup, find_packages
import os

# Read version from package
version_file = os.path.join(os.path.dirname(__file__), 'datapy', '__init__.py')
version = {}
with open(version_file) as f:
    exec(f.read(), version)

# Read long description from README if it exists
long_description = "A Python framework for creating reusable ETL components"
readme_path = os.path.join(os.path.dirname(__file__), 'README.md')
if os.path.exists(readme_path):
    with open(readme_path, 'r', encoding='utf-8') as f:
        long_description = f.read()

setup(
    name="datapy",
    version=version['__version__'],
    description=version['__description__'],
    long_description=long_description,
    long_description_content_type="text/markdown",
    python_requires=">=3.12",
    packages=find_packages(),
    include_package_data=True,
    
    # Core dependencies
    install_requires=[
        "pydantic>=2.0.0",
        "PyYAML>=6.0",
        "click>=8.0.0"
    ],
    
    # Optional dependencies for development
    extras_require={
        "dev": [
            "pytest>=7.0.0",
            "pytest-cov>=4.0.0"
        ]
    },
    
    # CLI entry points
    entry_points={
        "console_scripts": [
            "datapy=datapy.__main__:main",
        ],
    },
    
    # Package metadata
    classifiers=[
        "Development Status :: Beta",
        "Intended Audience :: Developers",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Topic :: Database",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.12",
        "Operating System :: OS Independent",
    ],
    
    # Package discovery
    package_data={
        "datapy": ["py.typed"],
    },
    zip_safe=False,
)