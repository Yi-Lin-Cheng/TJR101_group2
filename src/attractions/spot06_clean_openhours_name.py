import uuid
from pathlib import Path

import pandas as pd

data_dir = Path("data", "spot")


def generate_uuid(row):
    return str(uuid.uuid5(uuid.NAMESPACE_DNS, row["s_name"] + row["address"]))


def main():
    read_file = data_dir / "spot05_extract_googlemap.csv"
    save_file = data_dir / "spot06_cleaned_final.csv"
    data = pd.read_csv(
        read_file,
        encoding="utf-8",
        engine="python",
    )
    data = data.drop_duplicates(subset=["gmaps_url"])
    data["b_hours"] = data["b_hours"].str.replace("", "\n")
    data["spot_id"] = data.apply(generate_uuid, axis=1)
    data.to_csv(
        save_file,
        encoding="utf-8",
        header=True,
        index=False,
    )


if __name__ == "__main__":
    main()
