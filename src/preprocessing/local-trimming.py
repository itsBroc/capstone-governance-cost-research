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
from datetime import timedelta
from pathlib import Path

#Definitions

SCADA_INPUT = Path("src/data-raw/PUBLIC_ARCHIVE#DISPATCH_UNIT_SCADA#FILE01#202506010000.CSV")
REGION_SUM_INPUT = Path("src/data-raw/PUBLIC_ARCHIVE#DISPATCHREGIONSUM#FILE01#202506010000.CSV")
TRADING_PRICE_INPUT = Path("src/data-raw/PUBLIC_ARCHIVE#TRADINGPRICE#FILE01#202506010000.CSV")
DU_DETAIL_INPUT = Path("src/data-raw/PUBLIC_ARCHIVE#DUDETAILSUMMARY#FILE01#202506010000.CSV")

SCADA_OUTPUT = Path("src/data-clean/DISPATCH_UNIT_SCADA")
REGION_SUM_OUTPUT = Path("src/data-clean/DISPATCH_REGION_SUM")
TRADING_PRICE_OUTPUT = Path("src/data-clean/TRADING_PRICE")
DU_DETAIL_OUTPUT = Path("src/data-clean/DU_DETAIL_SUMMARY")

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

def check_input_file(file_path: Path) -> None:
    if not file_path.exists():
        raise FileNotFoundError(f"File Not Found: {file_path.resolve()}")

def check_output_folder(output_folder: Path) -> None:
    if output_folder.exists() and any(output_folder.rglob("*.csv")):
        raise FileExistsError(f"CSV files already exist in: {output_folder.resolve()}")

def read_aemo_chunks(file_path: Path, data_columns: list[str]):
    control_columns = ["_ROW_TYPE", "_DATASET", "_TABLE", "_VERSION"]
    all_columns = control_columns + data_columns

    return pd.read_csv(
    file_path,
    header=None,
    names=all_columns,
    dtype=str,
    chunksize=CHUNK_SIZE,
    encoding="utf-8-sig",
    keep_default_na=False,
    low_memory=False,
    skiprows=2)


#Output file path for each day
def get_output_path(output_file: Path, table_name: str, market_day) -> Path:
    return output_file / f"{table_name}_{market_day.isoformat()}.csv"

### Processing Functions

"""
Used for:
    - DISPATCH_UNIT_SCADA
    - DISPATCHREGIONSUM
    - TRADINGPRICE
"""
def process_table(input_file: Path, output_folder: Path, table_name: str, collect_duids: bool = False,) -> set[str]:
    check_input_file(input_file)
    check_output_folder(output_folder)

    data_columns = read_aemo_columns(input_file)

    total_rows_written = 0
    rows_by_day: dict[str, int] = {}
    duids: set[str] = set()

    chunks = read_aemo_chunks(input_file, data_columns)

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

        if collect_duids:
            duids.update(duid for duid in selected["DUID"].unique() if duid != "")


        for market_day, row in selected_days.groupby(selected_days).groups.items():
            daily_rows = selected.loc[row, data_columns]
            output_path = get_output_path(output_folder, table_name, market_day)
            append_daily_csv(daily_rows, output_path)

            row_count = len(daily_rows)
            total_rows_written += row_count
            day_key = market_day.isoformat()
            rows_by_day[day_key] = rows_by_day.get(day_key, 0) + row_count
    print(f"{table_name} preprocessing complete with {total_rows_written:,} rows written.")

    for day, row_count in sorted(rows_by_day.items()):
        print(f"{day}: {row_count:,} rows")

    return duids

def process_du_detail_summary(duids: set[str]) -> None:
    check_input_file(DU_DETAIL_INPUT)
    check_output_folder(DU_DETAIL_OUTPUT)

    data_columns = read_aemo_columns(DU_DETAIL_INPUT)

    start = START_DAY.strftime("%Y/%m/%d") + " 00:00:00"
    day_following_end = END_DAY + timedelta(days=1)
    end = day_following_end.strftime("%Y/%m/%d") + " 00:00:00"

    retained_chunks: list[pd.DataFrame] = []

    chunks = read_aemo_chunks(DU_DETAIL_INPUT, data_columns)

    for chunk in chunks:
        data = chunk.loc[chunk["_ROW_TYPE"] == "D", data_columns].copy()
        if data.empty:
            continue

        duid_is_relevant = data["DUID"].isin(duids)

        overlaps_week = (data["START_DATE"] < end) & ((data["END_DATE"] == "") | (data["END_DATE"] > start))

        selected = data.loc[duid_is_relevant & overlaps_week, data_columns].copy()

        if not selected.empty:
            retained_chunks.append(selected)

        if selected.empty:
            continue

    if not retained_chunks:
        raise ValueError("No DUDETAILSUMMARY records matched the retained SCADA DUIDS")

    result = pd.concat(retained_chunks, ignore_index=True)
    output_path=(DU_DETAIL_OUTPUT/"du_detail_summary_2025-06-01_to_2025-06-07.csv")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    result.to_csv(output_path, index=False, encoding="utf-8", lineterminator="\n")

    print(f"DUDETAILSUMMARY preprocessing complete with {len(result):,} rows written.")

def main() -> None:
    duids = process_table(
        input_file=SCADA_INPUT,
        output_folder=SCADA_OUTPUT,
        table_name="dispatch_unit_scada",
        collect_duids=True
    )

    process_table(
        input_file=REGION_SUM_INPUT,
        output_folder=REGION_SUM_OUTPUT,
        table_name="dispatch_region_sum"
    )

    process_table(
        input_file=TRADING_PRICE_INPUT,
        output_folder=TRADING_PRICE_OUTPUT,
        table_name="trading_price"
    )

    process_du_detail_summary(duids=duids)       

if __name__ == "__main__":
    main()
