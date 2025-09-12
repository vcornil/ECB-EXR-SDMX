# European Central Bank's Exchange Rates Retriever

[![Python](https://img.shields.io/badge/Python-3.9%2B-blue.svg)](https://www.python.org/)
[![License](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

## Overview

This project provides a Python class and command-line tool to retrieve official exchange rates from the European Central Bank (ECB) using the SDMX (Statistical Data and Metadata eXchange) API. It supports fetching daily and monthly rates for 32 major currencies against the Euro, and exporting results in multiple formats.

## Features

- Fetch daily and monthly exchange rates from the ECB SDMX API
- Calculate month-end closing rates, monthly averages, and year-to-date averages
- Export results to CSV, Excel (`.xlsx`), PDF, TXT, JSON, or display on screen
- Flexible period selection (year, month, start month)
- Easy-to-use command-line interface and Python class

## Installation

1. **Clone the repository:**
   ```sh
   git clone https://github.com/yourusername/ecb-exr-sdmx.git
   cd ecb-exr-sdmx
   ```

2. **Install dependencies:**
   ```sh
   pip install -r requirements.txt
   ```
   *Required packages include: `pandas`, `numpy`, `fpdf`, `openpyxl`, `tabulate`, `sdmx` (or `pandasdmx`), etc.*

## Usage

### Command Line

Retrieve exchange rates for a specific period and export them in various formats.

```sh
python ecbexr.py [-y YEAR] [-m MONTH] [-s START_MONTH] [-f FORMAT] [-p PATH]
```

**Arguments:**
- `-y`, `--year`: Year to get the rates for (between 2001 and current year, optional).
- `-m`, `--month`: Month to get the rates for (1-12, optional).
- `-s`, `--start`: First month of the period (1-12, optional, default is January).
- `-f`, `--format`: Output format (`csv`, `xlsx`, `pdf`, `txt`, `json`, `screen`; default is `screen`).
- `-p`, `--path`: Path for the generated file (optional, default is script folder).

**Examples:**

- Show rates for previous month on screen:
  ```sh
  python ecbexr.py
  ```

- Export rates for March 2024 as CSV:
  ```sh
  python ecbexr.py -y 2024 -m 3 -f csv
  ```

- Export rates for Jan–Mar 2023 as Excel file to a custom folder:
  ```sh
  python ecbexr.py -y 2023 -m 3 -s 1 -f xlsx -p /path/to/output
  ```

### As a Python Class

You can also use the [`Ecbexr`](ecbexr.py) class in your own Python scripts:

```python
from ecbexr import Ecbexr

ecb = Ecbexr(target_year=2024, target_month=3, from_month=1)
rates = ecb.rates()
print(rates)
```

## Output Formats

- **CSV:** Comma-separated values
- **Excel:** `.xlsx` spreadsheet
- **PDF:** Formatted table
- **TXT:** Text table
- **JSON:** Dictionary structure
- **Screen:** Pretty-printed table

## API Reference

### `Ecbexr` Class

**Constructor:**
```python
Ecbexr(target_year: int = None, target_month: int = None, from_month: int = None)
```
- `target_year`: Year for the rates (defaults to previous month’s year)
- `target_month`: Month for the rates (defaults to previous month)
- `from_month`: First month of the period (defaults to January)

**Method:**
```python
rates() -> dict
```
Returns a dictionary with currency codes as keys and rate info as values.

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.

## Contributing

Contributions, issues, and feature requests are welcome! 

## Acknowledgements

- European Central Bank SDMX API
- [pandasdmx](https://github.com/khaeru/pandasdmx)
- [FPDF](https://pyfpdf.github.io/)
- [tabulate](https://pypi.org/project/tabulate/)
- [openpyxl](https://openpyxl.readthedocs.io/en/stable/)

---

*For questions or support, please open an issue or contact the maintainer.*