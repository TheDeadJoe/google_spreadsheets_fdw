import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="google_spreadsheets_fdw",
    version="1.0.2",
    author="TheDeadJoe",
    description="Multicorn-based PostgreSQL foreign data wrapper for Google Spreadsheets",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/TheDeadJoe/google_spreadsheets_fdw",
    packages=setuptools.find_packages(),
    install_requires=[
        'gspread==3.6.0',
        'multicorn==1.4.0',
        'oauth2client==4.1.3',
    ],
    classifiers=[
        "Programming Language :: Python :: 3.5",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
)
