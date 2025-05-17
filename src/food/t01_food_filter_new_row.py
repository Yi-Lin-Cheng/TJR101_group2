import re
from pathlib import Path

import pandas as pd

if Path("/opt/airflow/data").exists():
    data_dir = Path("/opt/airflow/data/food")
else:
    data_dir = Path("data/food")


def main():
    latest_file = data_dir / "restaurant01_open_data_raw_latest.csv"
    previous_file = data_dir / "restaurant01_open_data_raw_previous.csv"
    new_file = data_dir / "restaurant02_added_rows_from_newdata_filtered.csv"
    data_latest = pd.read_csv(latest_file, encoding="utf-8", engine="python")
    data_previous = pd.read_csv(previous_file, encoding="utf-8", engine="python")

    data_new = data_latest[
        (~data_latest["Class"].astype(str).str.contains("6", na=False))
        & (~data_latest["Region"].str.contains(r"連江|金門|澎湖", na=False))
        & (~data_latest["Town"].str.contains(r"琉球|蘭嶼|綠島", na=False))
        & (~data_latest["Id"].isin(data_previous["Id"]))
    ]

    data_new.to_csv(new_file, encoding="utf-8", header=True, index=False)


if __name__ == "__main__":
    main()
