'''
Several Goals contained within this file.

1. Reduce the amount of data to work on. Reduce each file from a month to a week of data (SCADA: 4.5m --> 900k)
2. Perform daily partitions (Makes fault injections and governance more streamlined)
    2a. DISPATCH_UNIT_SCADA --> 7 Daily Partitions
    2b. DISPATCH_REGION_SUM --> 7 Daily Partitions
    2c. TRADING_PRICE --> 7 Daily Partitions
    2d. DU_DETAIL_SUMMARY --> Keep as One

3. 
'''



import pandas as pd
import csv
from pathlib import Path

#Definitions

INPUT_FILE = Path("src/data-raw/PUBLIC_ARCHIVE#DISPATCH_UNIT_SCADA#FILE01#202506010000.CSV")
OUTPUT_FOLDER = Path("src/data-clean/DISPATCH_UNIT_SCADA")

START_DAY = pd.Timestamp("2025-06-01").date()
END_DAY = pd.Timestamp("2025-06-07").date()

CHUNK_SIZE = 100_000

#Read AEMO column names from the header
def read_aemo_columns(file_path: Path) -> list[str]:
    with file_path.open("r", encoding="utf-8-sig", newline="") as file:
        reader = csv.reader(file)
        for row in reader:
            if row and row[0] =="I":
                return row[4:]
    raise ValueError(f"No AEMO I row was found in {file_path.name}")

#Get market day for each timestamp (Reduce by 5 minutes to get final for each day)
def get_market_day(settlement_dates: pd.Series) -> pd.Series:
    parsed_dates = pd.to_datetime(settlement_dates, format="%Y/%m/%d %H:%M:%S", errors="raise")
    return (parsed_dates - pd.Timedelta(minutes=5)).dt.date

#Output file path for each day
def get_output_path(market_day) -> Path:
    return (OUTPUT_FOLDER
            /f"year={market_day.year:04d}"
            /f"month={market_day.month:02d}"
            /f"day={market_day.day:02d}"
            /f"dispatch_unit_scada={market_day.isoformat()}.csv")

#Append a chunk of data to a daily csv
def append_daily_csv(data: pd.DataFrame, output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    already_exists = output_path.exists()

    data.to_csv(
        output_path,
        mode="a",
        header=not already_exists,
        index=False,
        encoding="utf-8",
        lineterminator="\n")

def process_scada() -> None:
    if not INPUT_FILE.exists():
        raise FileNotFoundError("SCADA source file not found")
    if OUTPUT_FOLDER.exists() and any(OUTPUT_FOLDER.rglob("*.csv")):
        raise FileExistsError("Output CSV files arealdy exist in Output Folder")

    data_columns = read_aemo_columns(INPUT_FILE)

    control_columns = ["_ROW_TYPE", "_DATASET", "_TABLE", "_VERSION"]
    all_columns = control_columns + data_columns

    total_rows_written = 0
    rows_by_day: dict[str, int] = {}

    chunks = pd.read_csv(
        INPUT_FILE,
        header=None,
        names=all_columns,
        dtype=str,
        chunksize=CHUNK_SIZE,
        encoding="utf-8-sig",
        keep_default_na=False,
        low_memory=False,
        skiprows=2)

    for chunk in chunks:
        data = chunk.loc[chunk["_ROW_TYPE"] == "D", data_columns].copy()
        if data.empty:
            continue

        market_days = get_market_day(data["SETTLEMENTDATE"])
        keep = (market_days >= START_DAY) & (market_days <= END_DAY)
        selected = data.loc[keep].copy()
        selected_days = market_days.loc[keep]

        if selected.empty:
            continue

        for market_day, row in selected_days.groupby(selected_days).groups.items():
            daily_rows = selected.loc[row, data_columns]
            output_path = get_output_path(market_day)
            append_daily_csv(daily_rows, output_path)

            row_count = len(daily_rows)
            total_rows_written += row_count
            day_key = market_day.isoformat()
            rows_by_day[day_key] = rows_by_day.get(day_key, 0) + row_count
    print(f"Scada preprocessing complete with {total_rows_written:,} rows written.")

    for day, row_count in sorted(rows_by_day.items()):
        print(f"{day}: {row_count:,} rows")

if __name__ == "__main__":
    process_scada()