import re
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
data["s_name"] = data["s_name"].apply(
    lambda name: re.split(r"\||│|｜|\-|－|/|／", name)[0] if len(name) > 20 else name
)
data["geo_loc"] = data["geo_loc"].str.replace(",", " ")

data.to_csv(
    file_path / "spot06_cleaned_final.csv",
    encoding="utf-8",
    header=True,
    index=False,
)
