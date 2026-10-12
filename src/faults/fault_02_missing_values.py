"""
Inject a missing/null fault into one TRADINGPRICE partition.

- Reads one clean CSV partition
- Sets RRP to null for a continuous block of time
- Writes the corrupted copy to a separate fault directory
- Does not modify the clean source file
"""

from pathlib import Path
import pandas as pd

INPUT_FILE = Path("src/data-clean/TRADING_PRICE/trading_price_2025-06-04.csv")
OUTPUT_FOLDER = Path("src/data-faults/s2")

TIME_COLUMN = "SETTLEMENTDATE"
TARGET_COLUMN = "RRP"
AEMO_TIME_FORMAT = "%Y/%m/%d %H:%M:%S"

FAULT_START = pd.Timestamp("2025-06-04 12:00:00")
FAULT_END = pd.Timestamp("2025-06-04 14:00:00")


def main() -> None:
    if not INPUT_FILE.exists():
        raise FileNotFoundError(f"Input file not found: {INPUT_FILE}")

    data = pd.read_csv(INPUT_FILE, dtype=str, keep_default_na=False)

    required_columns = {TIME_COLUMN, TARGET_COLUMN}

    if not required_columns.issubset(data.columns):
        missing_columns = required_columns - set(data.columns)
        raise KeyError(
            f"Missing required columns: {sorted(missing_columns)}"
        )

    timestamps = pd.to_datetime(data[TIME_COLUMN], format=AEMO_TIME_FORMAT)

    rows_to_corrupt = (timestamps >= FAULT_START) & (timestamps <= FAULT_END)       

    if not rows_to_corrupt.any():
        raise ValueError(
            "No rows were found within the selected fault time block."
        )

    data.loc[rows_to_corrupt, TARGET_COLUMN] = ""
    
    OUTPUT_FOLDER.mkdir(parents=True, exist_ok=True)
    output_file = OUTPUT_FOLDER / INPUT_FILE.name
    data.to_csv(output_file, index=False)

    print(f"Fault period: {FAULT_START} to {FAULT_END}")
    print(f"Rows set to null: {rows_to_corrupt.sum():,}")


if __name__ == "__main__":
    main()