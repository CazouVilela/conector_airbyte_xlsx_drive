from setuptools import find_packages, setup

MAIN_REQUIREMENTS = [
    "google-api-python-client>=2.0.0",
    "google-auth>=2.0.0",
    "openpyxl>=3.1.0",
    "pyyaml>=6.0",
]

TEST_REQUIREMENTS = [
    "pytest>=7.0",
    "pytest-mock>=3.0",
]

setup(
    name="source_google_sheets_xlsx",
    version="1.1.0",
    description="Airbyte source connector for Google Sheets and XLSX files on Google Drive.",
    author="GrupoHub",
    author_email="dev@grupohub.com.br",
    packages=find_packages(),
    install_requires=MAIN_REQUIREMENTS,
    extras_require={"tests": TEST_REQUIREMENTS},
    package_data={"source_google_sheets_xlsx": ["spec.yaml"]},
    python_requires=">=3.10",
    entry_points={
        "console_scripts": [
            "source-google-sheets-xlsx=source_google_sheets_xlsx.source:run_cli",
        ],
    },
)
