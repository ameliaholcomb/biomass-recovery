# GEDI - Planet Biomass Mapping

 [![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](https://opensource.org/licenses/MIT)
 <a href="https://github.com/psf/black"><img alt="Code style: black" src="https://img.shields.io/badge/code%20style-black-000000.svg"></a>

## Requirements
- Python 3.9+

## Data availability
This project uses the following data sources:

| Data source                              | Availability                                                                  | Platform | LiDAR         | Date range      |
|------------------------------------------|-------------------------------------------------------------------------------|----------|---------------|-----------------|
| GEDI Level 2A (v002)                     | [public](https://lpdaac.usgs.gov/products/gedi02_av002/)                      | Space    | Full-waveform | 2019-2020       |
| Estimativa de biomassa na Amazonia (EBA) | [proprietary](https://zenodo.org/record/4968706#.YPGe7OhKhEY)                 | Aerial   | Point cloud   | 2016-2018       |
| Sustainable Landscapes (SL)              | [public](https://daac.ornl.gov/CMS/guides/LiDAR_Forest_Inventory_Brazil.html) | Aerial   | Point cloud   | 2008, 2011-2018 |


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
