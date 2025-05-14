import re
from pathlib import Path

import pandas as pd

if Path("/opt/airflow/data").exists():
    data_dir = Path("/opt/airflow/data/food")
else:
    data_dir = Path("data/food")


def main():
    read_file = data_dir / "food05_extract_googlemap.csv"
    save_file = data_dir / "food06_cleaned_final.csv"
    data = pd.read_csv(
        read_file,
        encoding="utf-8",
        engine="python",
    )
    data["b_hours"] = data["b_hours"].str.replace("", "\n")
    data = data.drop_duplicates(subset="f_name")
    data["f_name"] = data["f_name"].apply(
        lambda x: re.split(r" |\(", x)[0] if isinstance(x, str) and len(x) > 100 else x
    )
    data.to_csv(
        save_file,
        encoding="utf-8",
        header=True,
        index=False,
    )


if __name__ == "__main__":
    main()
