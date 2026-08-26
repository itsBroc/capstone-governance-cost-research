"""
Inject a join-key fault into one DISPATCH_UNIT_SCADA partition.

- Reads one clean SCADA CSV partition
- Selects 3% of unique DUID values
- Replaces them with validly formatted but nonexistent DUIDs
- Causes those records to fail matching DU_DETAIL_SUMMARY during the join
- Writes the corrupted copy to a separate fault directory
- Does not modify the clean source file
"""

from pathlib import Path
import pandas as pd

INPUT_FILE = Path("src/data-clean/DISPATCH_UNIT_SCADA/dispatch_unit_scada_2025-06-04.csv")
OUTPUT_FOLDER = Path("src/data-faults/s5")

TARGET_COLUMN = "DUID"
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

    unique_duids = data[TARGET_COLUMN].dropna().drop_duplicates()

    duids_to_corrupt = unique_duids.sample(
        frac=CORRUPTION_RATE,
        random_state=RANDOM_SEED,
    )

    replacement_map = {
        duid: f"UNMAPPED_{i:04d}"
        for i, duid in enumerate(duids_to_corrupt, start=1)
    }

    data[TARGET_COLUMN] = data[TARGET_COLUMN].replace(replacement_map)

    OUTPUT_FOLDER.mkdir(parents=True, exist_ok=True)
    output_file = OUTPUT_FOLDER / INPUT_FILE.name
    data.to_csv(output_file, index=False)

    print(f"DUIDs corrupted: {len(duids_to_corrupt):,}")
    print(f"Corruption rate: {CORRUPTION_RATE:.0%}")


if __name__ == "__main__":
    main()