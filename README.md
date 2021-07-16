# GEDI - Planet Biomass Mapping

 [![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](https://opensource.org/licenses/MIT)
 <a href="https://github.com/psf/black"><img alt="Code style: black" src="https://img.shields.io/badge/code%20style-black-000000.svg"></a>

## Requirements
- Python 3.9+

## Data availability
This project uses the following data sources:

| Data source                                           | Availability                                                                  | Sensors                                     | Data                | Date range      | No. observations (used) | Area covered |
|-------------------------------------------------------|-------------------------------------------------------------------------------|---------------------------------------------|---------------------|-----------------|-------------------------|--------------|
| GEDI Level 2A (v002)                                  | [public](https://lpdaac.usgs.gov/products/gedi02_av002/)                      | Space-borne LiDAR                           | Full-waveform LiDAR | 2019-2020       | 150 Mio.                | 7.4 Mio. ha  |
| Estimativa de biomassa na Amazonia (EBA)              | [proprietary](https://zenodo.org/record/4968706#.YPGe7OhKhEY)                 | Aerial LiDAR                                | Point cloud LiDAR   | 2016-2018       | 905                     | 574'000 ha   |
| Sustainable Landscapes (SL)                           | [public](https://daac.ornl.gov/CMS/guides/LiDAR_Forest_Inventory_Brazil.html) | Aerial LiDAR                                | Point cloud LiDAR   | 2008, 2011-2018 | 186                     | 40'000       |
| EU Joint Research Council (JRC) Annual Change dataset | [public](https://forobs.jrc.ec.europa.eu/TMF/download/)                       | Space-borne multispectral imagery (Landsat) | Landcover Raster    | 1992-2020       | -                       |              |

An geographic map of all air-borne LiDAR data with metadata is available [here](https://simonmathis.org/projects/sequestration/map.html).
The created dataset

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
├── report             <- Generated analysis as HTML, PDF, LaTeX, etc.
│   ├── figures        <- Generated graphics and figures to be used in reporting
│   └── sections       <- LaTeX sections. The report folder can be linked to your overleaf
|                         report with github submodules.
│
├── requirements       <- Directory containing the requirement files.
│
├── setup.py           <- makes project pip installable (pip install -e .) so src can be imported
├── src                <- Source code for use in this project.
│   ├── __init__.py    <- Makes src a Python module
│   │
│   ├── data_loading   <- Scripts to download or generate data
│   │
│   ├── preprocessing  <- Scripts to turn raw data into clean data and features for modeling
|   |
│   ├── models         <- Scripts to train models and then use trained models to make
│   │                     predictions
│   │
│   └── tests          <- Scripts for unit tests of your functions
│
└── setup.cfg          <- setup configuration file for linting rules
```

## Code formatting
To automatically format your code, make sure you have `black` installed (`pip install black`) and call
```black . ``` 
from within the project directory.

---

Project template created by the [Cambridge AI4ER Cookiecutter](https://github.com/ai4er-cdt/ai4er-cookiecutter).
