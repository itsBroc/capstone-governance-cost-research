"""
Inject a schema change fault into one DISPATCHREGIONSUM partition.

- Reads one clean CSV partition
- Renames REGIONID to REGION_ID
- Writes the corrupted copy to a separate fault directory
- Does not modify the clean source file
"""

from pathlib import Path

import pandas as pd


# Change this filename to the partition you want to corrupt.
INPUT_FILE = Path(
    "src/data-clean/DISPATCH_REGION_SUM/year=2025/month=06/day=04/dispatch_region_sum_2025-06-04.csv"
)

OUTPUT_FOLDER = Path(
    "src/data-faults"
)

ORIGINAL_COLUMN = "REGIONID"
FAULTY_COLUMN = "REGION_ID"


def main() -> None:
    if not INPUT_FILE.exists():
        raise FileNotFoundError(f"Input file not found: {INPUT_FILE}")

    data = pd.read_csv(INPUT_FILE, low_memory=False)

    if ORIGINAL_COLUMN not in data.columns:
        raise KeyError(
            f"Column '{ORIGINAL_COLUMN}' not found in {INPUT_FILE.name}"
        )

    data = data.rename(
        columns={ORIGINAL_COLUMN: FAULTY_COLUMN}
    )

    OUTPUT_FOLDER.mkdir(parents=True, exist_ok=True)

    output_file = OUTPUT_FOLDER / (
        f"{INPUT_FILE.stem}_SCHEMA_FAULT.csv"
    )

    data.to_csv(output_file, index=False)

    print(f"Source file: {INPUT_FILE}")
    print(f"Corrupted file: {output_file}")
    print(f"Renamed column: {ORIGINAL_COLUMN} -> {FAULTY_COLUMN}")


if __name__ == "__main__":
    main()