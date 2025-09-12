import sdmx
import pandas as pd
from datetime import datetime, date, timedelta
import numpy as np
import pprint
import argparse
from tabulate import tabulate
from fpdf import FPDF
from fpdf.enums import XPos, YPos, Align
import csv
from openpyxl import Workbook
from typing import Dict, Union, cast
import os
from pathlib import Path
import sys

class Ecbexr:
    # Class constants
    SDMX_CLIENT = "ECB"
    SDMX_DATASET = "EXR"
    SDMX_DAILY_KEY = "D..EUR.SP00.A"  # Daily data
    SDMX_MONTHLY_KEY = "M..EUR.SP00.A"  # Monthly data

    def __init__(
        self,
        target_year: int | None = None,
        target_month: int | None = None,
        from_month: int | None = None,
    ):

        # initialize internal variables
        self._start_month = 1
        self._end_month = 1
        self._end_year = 2025
        self._start_year = 2025

        # Default values for target year is current year iif month is not January else previous year if no value provided
        if target_year == None:
            self.end_year = (
                datetime.now().year
                if datetime.now().month > 1
                else datetime.now().year - 1
            )
        else:
            self.end_year = target_year

        # Default values for target month is previous month iif month is not January else December if no value provided
        if target_month == None:
            self.end_month = (
                datetime.now().month - 1 if datetime.now().month > 1 else 12
            )
        else:
            self.end_month = target_month

        # Default values for starting month is January if no value provided
        if from_month == None:
            self.start_month = 1
        else:
            self.start_month = from_month

        # Default values for starting year is _target_year if start month if lower than end month
        self.start_year = (
            self._end_year
            if self.start_month <= self.end_month
            else (self._end_year - 1)
        )

    # Getter for the end year
    @property
    def end_year(self) -> int:
        return self._end_year

    # Setter for the end year
    @end_year.setter
    def end_year(self, value: int):
        """
        Setter for the year property with type and range validation.
        """
        if not isinstance(value, int):
            raise TypeError("End Year must be an integer.")
        if not 2001 <= value <= datetime.now().year:
            raise ValueError(
                f"End Year must be between 2001 and {datetime.now().year}."
            )
        self._end_year = value

    # Getter for the end month
    @property
    def end_month(self) -> int:
        return self._end_month

    # Setter for the end month
    @end_month.setter
    def end_month(self, value: int) -> None:
        """
        Setter for the end month property with type and range validation.
        """
        if not isinstance(value, int):
            raise TypeError("End Month must be an integer.")
        if not 1 <= value <= 12:
            raise ValueError("End Month must be between 1 and 12.")
        self._end_month = value
        # Also reset start year, in case it is needed
        self.start_year = (
            self._end_year if self.start_month <= value else (self._end_year - 1)
        )

    # Getter for the start month
    @property
    def start_month(self) -> int:
        return self._start_month

    # Setter for the start month
    @start_month.setter
    def start_month(self, value: int) -> None:
        """
        Setter for the start month property with type and range validation.
        """
        if not isinstance(value, int):
            raise TypeError("Start Month must be an integer.")
        if not 1 <= value <= 12:
            raise ValueError("Start Month must be between 1 and 12.")
        self._start_month = value
        # Also reset start year, in case it is needed
        self.start_year = (
            self._end_year if value <= self.end_month else (self._end_year - 1)
        )

    def rates(self) -> dict[str, dict[str, float | str | None]]:
        """
        Retrieve exchange rates for a specific month and compute:
        1. Month-end closing rate (from daily data - last trading day of the month)
        2. YTD average rate (average of monthly rates from Jan to target month)
        3. Target month average (official ECB monthly average for the specific month)

        Returns:
            dict: Dictionary with currency codes as keys and rate info as values
        """

        # Set the Client
        client = sdmx.Client(Ecbexr.SDMX_CLIENT)

        try:
            # Step 1: Get daily data ONLY for the target month to get month-end closing rate - limiting the number of days as only the last quotation day is needed
            if self.end_month == 12:
                last_day = 31
            else:
                next_month = datetime(self.end_year, self.end_month + 1, 1)
                last_day_date = next_month - timedelta(days=1)
                last_day = last_day_date.day

            daily_start = f"{self.end_year}-{self.end_month:02d}-{(last_day-6):02d}"  # at least 1 week of data to tke weekends and holidays into account
            daily_end = f"{self.end_year}-{self.end_month:02d}-{last_day:02d}"

            # print(f"Fetching daily data for month-end rates: {daily_start} to {daily_end}...")

            daily_response = client.data(
                Ecbexr.SDMX_DATASET,
                key=Ecbexr.SDMX_DAILY_KEY,
                params={"startPeriod": daily_start, "endPeriod": daily_end},
            )

            daily_df = sdmx.to_pandas(daily_response.data)

            # Step 2: Get monthly data from start month+year to target month for averages
            monthly_start = f"{self.start_year}-{self.start_month:02d}"
            monthly_end = f"{self.end_year}-{self.end_month:02d}"

            # print(f"Fetching monthly data for averages: {monthly_start} to {monthly_end}...")

            monthly_response = client.data(
                Ecbexr.SDMX_DATASET,
                key=Ecbexr.SDMX_MONTHLY_KEY,
                params={"startPeriod": monthly_start, "endPeriod": monthly_end},
            )

            monthly_df = sdmx.to_pandas(monthly_response.data)

            if daily_df.empty or monthly_df.empty:
                print("No valid data retrieved.")
                return {}

            # print(f"Daily data shape: {daily_df.shape}")
            # print(f"Monthly data shape: {monthly_df.shape}")
            # print(daily_df.items)

            # Process daily data for month-end rates
            closing_rates: dict[str, dict[str, float | str | None]] = {}
            if not daily_df.empty:
                daily_df = daily_df.reset_index()

                if "TIME_PERIOD" in daily_df.columns:
                    daily_df["date"] = pd.to_datetime(daily_df["TIME_PERIOD"])
                else:
                    date_cols = [
                        col
                        for col in daily_df.columns
                        if "TIME" in col.upper() or "DATE" in col.upper()
                    ]
                    if date_cols:
                        daily_df["date"] = pd.to_datetime(daily_df[date_cols[0]])

                # Identify currency and value columns
                daily_currency_col = None
                for col in daily_df.columns:
                    if "CURRENCY" in col.upper() and "DENOM" not in col.upper():
                        daily_currency_col = col
                        break

                daily_value_col = (
                    "value"
                    if "value" in daily_df.columns
                    else daily_df.select_dtypes(include=[np.number]).columns[-1]
                )

                if daily_currency_col:
                    # print(f"Daily - Currency column: {daily_currency_col}, Value column: {daily_value_col}")

                    # Get month-end rate for each currency (last trading day)
                    daily_df = daily_df.sort_values(["date", daily_currency_col])

                    for currency in daily_df[daily_currency_col].unique():
                        if pd.isna(currency) or currency == "EUR":
                            continue

                        currency_daily = daily_df[
                            daily_df[daily_currency_col] == currency
                        ].copy()
                        currency_daily = currency_daily.dropna(subset=[daily_value_col])

                        if len(currency_daily) > 0:
                            # Get the last (most recent) rate in the month -> last row -> iloc[-1]
                            closing_rate: float | None = float(
                                currency_daily.iloc[-1][daily_value_col]
                            )
                            closing_date: str | None = currency_daily.iloc[-1][
                                "date"
                            ].strftime("%Y-%m-%d")

                            # store the closing rate
                            closing_rates[currency] = {
                                "rate": closing_rate,
                                "date": closing_date,
                            }

            # Process monthly data for averages
            monthly_averages = {}
            ytd_averages = {}

            if not monthly_df.empty:
                monthly_df = monthly_df.reset_index()

                if "TIME_PERIOD" in monthly_df.columns:
                    monthly_df["month_period"] = pd.to_datetime(
                        monthly_df["TIME_PERIOD"]
                    )
                else:
                    date_cols = [
                        col
                        for col in monthly_df.columns
                        if "TIME" in col.upper() or "DATE" in col.upper()
                    ]
                    if date_cols:
                        monthly_df["month_period"] = pd.to_datetime(
                            monthly_df[date_cols[0]]
                        )

                # Identify currency and value columns
                monthly_currency_col = None
                for col in monthly_df.columns:
                    if "CURRENCY" in col.upper() and "DENOM" not in col.upper():
                        monthly_currency_col = col
                        break

                monthly_value_col = (
                    "value"
                    if "value" in monthly_df.columns
                    else monthly_df.select_dtypes(include=[np.number]).columns[-1]
                )

                if monthly_currency_col:
                    # print(f"Monthly - Currency column: {monthly_currency_col}, Value column: {monthly_value_col}")

                    # Filter to current year and up to target month
                    monthly_df = monthly_df[
                        (
                            (monthly_df["month_period"].dt.year == self.end_year)
                            & (monthly_df["month_period"].dt.month <= self.end_month)
                        )
                        | (
                            (monthly_df["month_period"].dt.year == self.start_year)
                            & (monthly_df["month_period"].dt.month >= self.start_month)
                        )
                    ]

                    for currency in monthly_df[monthly_currency_col].unique():
                        if pd.isna(currency) or currency == "EUR":
                            continue

                        currency_monthly = monthly_df[
                            monthly_df[monthly_currency_col] == currency
                        ].copy()
                        currency_monthly = currency_monthly.dropna(
                            subset=[monthly_value_col]
                        )
                        currency_monthly = currency_monthly.sort_values("month_period")

                        if len(currency_monthly) == 0:
                            continue

                        # Get target month average
                        target_month_data = currency_monthly[
                            currency_monthly["month_period"].dt.month == self.end_month
                        ]

                        if len(target_month_data) > 0:
                            monthly_averages[currency] = float(
                                target_month_data.iloc[0][monthly_value_col]
                            )

                        # Calculate YTD average (average of monthly averages)
                        if len(currency_monthly) > 0:
                            ytd_averages[currency] = float(
                                currency_monthly[monthly_value_col].mean()
                            )

            # Combine results
            all_currencies = (
                set(closing_rates.keys())
                | set(monthly_averages.keys())
                | set(ytd_averages.keys())
            )

            results = {}
            for currency in all_currencies:
                if currency == "EUR":
                    continue

                # Get closing rate
                closing_rate = cast(
                    float | None, closing_rates.get(currency, {}).get("rate")
                )
                closing_date = cast(
                    str | None, closing_rates.get(currency, {}).get("date")
                )

                # Get averages
                monthly_avg = monthly_averages.get(currency)
                ytd_avg = ytd_averages.get(currency)

                results[currency] = {
                    "Currency": currency,
                    "Closing": closing_rate,
                    "Month_Average": monthly_avg,
                    "YTD_Average": ytd_avg,
                    "Period": closing_date,
                    "has_closing": closing_rate is not None,
                    "has_monthly": monthly_avg is not None,
                    "has_ytd": ytd_avg is not None,
                }

            # print(f"✓ Successfully processed {len(results)} currencies")

            return results

        except Exception as e:
            print(f"Error retrieving data: {e}")
            import traceback

            traceback.print_exc()
            return {}


class PDF(FPDF):
    def __init__(self, title, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.title = title  # Store the title

    def header(self):
        self.set_font("Helvetica", "B", 16)
        self.cell(0, 10, self.title, new_x=XPos.LMARGIN, new_y=YPos.NEXT, align="C")
        self.ln(10)

    def print_table_header(self, headers, col_width):
        self.set_font("Helvetica", "B", 10)
        self.set_fill_color(230, 230, 230)  # Light grey
        self.set_draw_color(150, 150, 150)  # Middle grey
        for i, header in enumerate(headers):
            align = "L" if i == 0 else "R"
            self.cell(col_width, 10, header, border=1, align=align, fill=True)
        self.cell(0, 0, "", new_x=XPos.LMARGIN, new_y=YPos.NEXT)
        self.ln(10)

    def footer(self):
        self.set_y(-15)  # Position 15mm from bottom
        self.set_font("Helvetica", "I", 8)
        date_str = datetime.now().strftime("%Y-%m-%d %H:%M")
        page_text = f"Page {self.page_no()} | Generated on {date_str}"
        self.cell(0, 10, page_text, align="C")

    def add_intro_note(self, text):
        self.set_font("Helvetica", "", 11)
        self.set_text_color(50, 50, 50)  # Optional: darker grey
        self.multi_cell(0, 10, text)
        self.cell(0, 5, "", new_x=XPos.LMARGIN, new_y=YPos.NEXT)  # spacing before table

    def add_footer_note(self, text):
        self.set_font("Helvetica", "", 11)
        self.set_text_color(80, 80, 80)  # Optional: soft grey
        self.cell(0, 5, "", new_x=XPos.LMARGIN, new_y=YPos.NEXT)  # spacing
        self.multi_cell(0, 10, text)

    def table(self, headers, rows):
        self.set_font("Helvetica", "", 10)
        col_width = self.epw / len(headers)  # Equal column width
        self.print_table_header(headers, col_width)

        # Set light grey fill color (RGB: 230, 230, 230)
        # self.set_fill_color(230, 230, 230)

        # for header in headers:
        #    self.cell(col_width, 10, header, border=1, fill=True)
        # self.ln()

        self.set_font("Helvetica", "", 10)

        # Set middle grey border color
        self.set_draw_color(150, 150, 150)

        for row in rows:

            # Check if there's enough space left, else add page and reprint header
            if self.get_y() > self.page_break_trigger - 10:
                self.add_page()
                self.print_table_header(headers, col_width)
                self.set_font("Helvetica", "", 10)

            for i, item in enumerate(row):
                align = "L" if i == 0 else "R"
                # Handle float formatting for non-first columns
                if i != 0:  # and isinstance(item, str):
                    item_clean = str(item).strip().lower()
                    # print(item_clean)
                    if item_clean not in ["", "na", "null", "none"]:
                        try:
                            num = float(item)
                            formatted = f"{num:.8f}"
                            # print(formatted)
                        except ValueError:
                            formatted = item  # fallback if conversion fails
                    else:
                        formatted = ""  # treat as empty
                else:
                    formatted = str(item)

                # Determine borders
                if i == 0:
                    border = "LB"  # Left + Bottom
                elif i == len(row) - 1:
                    border = "RB"  # Right + Bottom
                else:
                    border = "B"  # Only Bottom

                self.cell(col_width, 10, formatted, border=border, align=align)
            self.ln()


# Example usage
if __name__ == "__main__":

    # For the example, enable the use of arguments - Can also be useful for testing purpose ;-)

    # Create the parser
    parser = argparse.ArgumentParser(
        description="ECB - Retrieve Exchange Rates from European Central Bank for a given Period."
    )

    # Add arguments
    parser.add_argument(
        "-y",
        "--year",
        type=int,
        help="Year to get the rates for. Between 2001 and now (optional - default = year of previous month)",
    )
    parser.add_argument(
        "-m",
        "--month",
        type=int,
        help="Month to get the rates for. Between 1 and 12 (optional - default = previous month)",
    )
    parser.add_argument(
        "-s",
        "--start",
        type=int,
        help="First month of the period. Between 1 and 12 (optional - default = 1  == Jan)",
    )
    parser.add_argument(
        "-f",
        "--format",
        type=str,
        help="Output format like csv | xlsx | pdf | txt | json | screen (optional - default = screen)",
    )
    parser.add_argument(
        "-p",
        "--path",
        type=str,
        help="Path for the generated file (optional - default = application folder)",
    )

    # Parse the arguments
    args = parser.parse_args()

    sdmx_year = None if args.year == None else args.year
    sdmx_month = None if args.month == None else args.month
    sdmx_start = None if args.start == None else args.start
    output: str | None = (
        "screen" if args.format == None else args.format.strip().lower()
    )

    #print(f"{datetime.now()}: Retrieving exchange rates from the ECB for the provided arguments...")
    ecbexr = Ecbexr(sdmx_year, sdmx_month, sdmx_start)
    rates_dict = ecbexr.rates()

    if args.path is not None:
        base_dir = Path(args.path)
    else:
        # __file__ = chemin du script, .parent = répertoire du script
        base_dir = Path(__file__).parent
    filename = base_dir / f"ExchangeRates_{ecbexr.end_year}-{ecbexr.end_month:02d}"

    # Convert dictionary to table
    #print(f"{datetime.now()}: Preparing data...")

    # Columns to exclude
    exclude_columns = ["has_closing", "has_monthly", "has_ytd", "month_end_date"]

    # Prepare headers by filtering keys from the first row
    headers = [
        key for key in next(iter(rates_dict.values())) if key not in exclude_columns
    ]

    # Prepare rows
    rows = [
        [rates_dict[outer_key][key] for key in headers]
        for outer_key in sorted(rates_dict.keys())
    ]

    #print(f"{datetime.now()}: Generating output...")
    if output == "pdf":

        title = (
            f"Exchange rates for the period: {ecbexr.end_year}-{ecbexr.end_month:02d}"
        )
        pdf = PDF(title)
        pdf.add_page()

        # Add intro text before the table
        pdf.add_intro_note("Currency rates from the European Central Bank.")
        pdf.table(headers, rows)
        pdf.add_footer_note(
            "Note: All values are rounded to 8 decimal places. Missing entries are left blank."
        )

        # Save the PDF
        pdf.output(f"{filename}.pdf")

        print(
            f"{filename}.pdf generated ..."
        )

    elif output == "csv":

        # Write to CSV
        with open(
            f"{filename}.csv",
            "w",
            newline="",
            encoding="utf-8",
        ) as f:
            writer = csv.writer(f)
            writer.writerow(headers)  # write header
            writer.writerows(rows)  # write data rows

        print(
            f"{filename}.csv generated ..."
        )

    elif output == "txt":
        # Open (or create) a file in write mode
        with open(
            f"{filename}.txt",
            "w",
            encoding="utf-8",
        ) as file:
            file.write(
                f"\n Rates from ECB for period {ecbexr.end_year}-{ecbexr.end_month:02d} using {ecbexr.start_month:02d} as starting month"
            )
            file.write("\n")
            file.write("=" * 70)
            file.write("\n")
            file.write("\n")
            file.write(tabulate(rows, headers=headers, tablefmt="grid"))

        print(
            f"{filename}.txt generated ..."
        )

    elif output == "json":
        # Open (or create) a file in write mode
        with open(
            f"{filename}.json",
            "w",
            encoding="utf-8",
        ) as file:
            pprint.pprint(rates_dict, stream=file)

        print(
            f"{filename}.json generated ..."
        )

    elif output == "xlsx":

        # Create workbook and sheet
        wb = Workbook()
        ws = wb.active
        if ws is not None:
            ws.title = f"Exr_{ecbexr.end_year}-{ecbexr.end_month:02d}"

            # Write headers
            ws.append(headers)

            # Write rows
            for outer_key in sorted(rates_dict.keys()):
                row = [rates_dict[outer_key][key] for key in headers]
                ws.append(row)

            # Save file
            wb.save(f"{filename}.xlsx")

            print(
                f"{filename}.xlsx generated ..."
            )

    else:

        print(
            f"\n Retrieving rates from ECB for period {ecbexr.end_year}-{ecbexr.end_month:02d} using {ecbexr.start_month:02d} as starting month",
            "\n",
            "=" * 70,
        )
        print("\n\n Rates:\n", "-" * 6)

        # Print table
        print(tabulate(rows, headers=headers, tablefmt="grid"))
