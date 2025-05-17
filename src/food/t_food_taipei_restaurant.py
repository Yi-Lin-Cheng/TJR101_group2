import re
from pathlib import Path

import pandas as pd

from tasks import normalize_address
from utils import to_half_width

data_dir = Path("data", "food")


def clean_name(matched_data):
    # 清理名稱
    matched_data = matched_data.copy()
    matched_data.loc[matched_data["f_name"].str.len() > 20, "f_name"] = (
        matched_data["f_name"].str.split(r"\||│|丨|｜|\-|－|/|／").str[0]
    )
    matched_data["f_name"] = matched_data["f_name"].apply(to_half_width)
    return matched_data


def main():
    data = pd.read_csv(
        data_dir / "taipei_restaurant.csv", encoding="utf-8", engine="python"
    )

    data["address"] = data["address"].apply(normalize_address)
    data = clean_name(data)

    data.to_csv(
        data_dir / "taipei_restaurant_cleaned.csv", index=False, encoding="utf-8"
    )


if __name__ == "__main__":
    main()
