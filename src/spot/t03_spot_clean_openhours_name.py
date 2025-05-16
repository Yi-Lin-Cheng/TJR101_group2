from pathlib import Path

import pandas as pd

if Path("/opt/airflow/data").exists():
    data_dir = Path("/opt/airflow/data/spot")
else:
    data_dir = Path("data/spot")


def main():
    read_file = data_dir / "spot05_extract_googlemap.csv"
    save_file = data_dir / "spot06_cleaned_final.csv"
    data = pd.read_csv(
        read_file,
        encoding="utf-8",
        engine="python",
    )
    data["b_hours"] = data["b_hours"].str.replace("", "\n")
    data.to_csv(
        save_file,
        encoding="utf-8",
        header=True,
        index=False,
    )


if __name__ == "__main__":
    main()
