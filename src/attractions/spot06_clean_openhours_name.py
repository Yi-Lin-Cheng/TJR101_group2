from pathlib import Path

import pandas as pd

file_path = Path("data", "spot")

data = pd.read_csv(
    file_path / "spot05_extract_googlemap.csv",
    encoding="utf-8",
    engine="python",
)
data = data.drop_duplicates(subset=["gmaps_url"])

data["b_hours"] = data["b_hours"].str.replace("", "\n")


data.to_csv(
    file_path / "spot06_cleaned_final.csv",
    encoding="utf-8",
    header=True,
    index=False,
)
