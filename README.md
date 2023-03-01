# Fusing GEDI shots and annual forest change maps to estimate tropical forest recovery rates across the Amazon basin

 [![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](https://opensource.org/licenses/MIT)
 <a href="https://github.com/psf/black"><img alt="Code style: black" src="https://img.shields.io/badge/code%20style-black-000000.svg"></a>

This repository contains the code and analysis pipelines used in the manuscript, "Fusing GEDI shots and annual forest change maps to estimate tropical forest recovery rates across the Amazon basin."

## Requirements
- Python 3.9+
- PostGIS 14+
- Java

## Data availability
This project uses the following data sources:

| Data source                                           | Availability                                                                  | Sensors                                     | Data                | Date range      | No. observations (used) | Area covered |
|-------------------------------------------------------|-------------------------------------------------------------------------------|---------------------------------------------|---------------------|-----------------|-------------------------|--------------|
| GEDI Level 4A (v002)                                  | [public](https://lpdaac.usgs.gov/products/gedi02_av002/)                      | Space-borne LiDAR                           | Full-waveform LiDAR | 2019-2020       | 150 Mio.                | 7.4 Mio. ha  |
| EU Joint Research Council (JRC) Annual Change dataset | [public](https://forobs.jrc.ec.europa.eu/TMF/download/)                       | Space-borne multispectral imagery (Landsat) | Landcover Raster    | 1992-2020       | -                       |              |

## Project Organization
```
├── LICENSE
├── Makefile           <- Makefile with commands like `make init` or `make lint-requirements`
├── README.md          <- The top-level README for developers using this project.
|
├── notebooks          <- Jupyter notebooks. Naming convention is a number (for ordering),
|   |                     the creator's initials, and a short `-` delimited description, e.g.
|   |                     `1.0_jqp_initial-data-exploration`.
│   ├── exploratory    <- Notebooks for initial exploration.
│
│
├── requirements       <- Directory containing the requirement files.
│
├── setup.py           <- makes project pip installable (pip install -e .) so src can be imported
├── src                <- Source code for use in this project.
│   ├── __init__.py    <- Makes src a Python module
│   │
│   ├── data           <- Functions to download and parse data
│   │
│   ├── processing     <- Functions to aggregate, overlay, and run data models
|   |
│   ├── spark          <- Spark-based end-to-end processing pipeline scripts
│   │
│   └── utils          <- Common utilities
│
└── setup.cfg          <- setup configuration file for linting rules
```

## General Guide to the project

![processing_pipeline](https://user-images.githubusercontent.com/16145172/220399119-55a1734f-9dfe-4a4a-b452-93ca54969b21.jpg)

The above diagram shows an overflow of the steps taken to generate the results. For those looking to reuse this code, note that you will need to take the following steps:

### BASIC SETUP
Fork this repository, create a new python environment, and install the project requirements, including python packages in `requirements/requirements.txt`.

Set appropriate environment variables:
Change `src/constants.py` to contain the database location and password for your database. Set the file paths to appropriate values.
Create a file called `.env` in your home directory with the following format:

```python
# Earthdata access
EARTHDATA_USER="<earthdata-username-here>"
EARTHDATA_PASSWORD="<earthdata-password-here>"
EARTH_DATA_COOKIE_FILE="~/.urs_cookies"

# User path constants
USER_PATH="<path-to-home-dir-containing-repo>"
DATA_PATH="<path-to-store-data>"

# Database constants
DB_HOST="<db-hostname>"
DB_NAME="<db-name>"
DB_USER="<db-user>"
DB_PASSWORD="<db-password>"
```

### DATA DOWNLOAD AND SETUP
GEDI data: The GEDI data is to be stored in a PostGIS database. Set up a new PostGIS database with the schema found in `schema.sql`.
Download the GEDI data for your region of interest and ingest it into PostGIS with the script found at `src/spark/gedi_download_pipeline.py`.
Note that this requires an existing earthdata account.

JRC data: Download the JRC rasters for your area of interest from the publicly available dataset.

### DATA PROCESSING
Run the overlay script, found at `src/spark/gedi_recovery_analysis_pipeline_monte_carlo.py`
Run with `--help` to see all of the arguments required.

### DATA ANALYSIS/FIGURES
All results can now be found in the directory you specified using the `--save_dir` flag for the overlay script.
The results include all of the output for a full monte carlo sample, which is probably more data than you are interested in. To get a summary of the data, aggregated by mean AGBD and mode recovery age across the monte carlo sample, use the notebook `exploratory/13-ah2174-gedi-monte-carlo-preprocessing.ipynb`
To reproduce the figures in the paper, use the notebook `28-ah2174-gedi-recovery-figures.ipynb`
This will also give you an idea of how to load and manipulate the output results to perform other analyses.

