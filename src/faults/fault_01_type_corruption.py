"""
Inject a type/format fault into one DISPATCH_UNIT_SCADA partition.

- Reads one clean CSV partition
- Replaces 3% of SCADAVALUE entries with non-numeric values
- Writes the corrupted copy to a separate fault directory
- Does not modify the clean source file
"""

from pathlib import Path
import pandas as pd


INPUT_FILE = Path("src/data-clean/DISPATCH_UNIT_SCADA/dispatch_unit_scada_2025-06-04.csv")
OUTPUT_FOLDER = Path("src/data-faults")

TARGET_COLUMN = "SCADAVALUE"
CORRUPTION_RATE = 0.03
RANDOM_SEED = 42

def main() -> None:
    if not INPUT_FILE.exists():
        raise FileNotFoundError(f"Input file not found: {INPUT_FILE}")

    data = pd.read_csv(INPUT_FILE, low_memory=False)

    if TARGET_COLUMN not in data.columns:
        raise KeyError(
            f"Column '{TARGET_COLUMN}' not found in {INPUT_FILE.name}"
        )

    numeric_rows = pd.to_numeric(
        data[TARGET_COLUMN],
        errors="coerce",
    ).notna()

    rows_to_corrupt = data[numeric_rows].sample(
        frac=CORRUPTION_RATE,
        random_state=RANDOM_SEED,
    ).index

    data[TARGET_COLUMN] = data[TARGET_COLUMN].astype("object")
    data.loc[rows_to_corrupt, TARGET_COLUMN] = "INVALID"

    OUTPUT_FOLDER.mkdir(parents=True, exist_ok=True)
    output_file = OUTPUT_FOLDER / INPUT_FILE.name
    data.to_csv(output_file, index=False)

    print(f"Rows corrupted: {len(rows_to_corrupt):,}")
    print(f"Corruption rate: {CORRUPTION_RATE:.0%}")


if __name__ == "__main__":
    main()