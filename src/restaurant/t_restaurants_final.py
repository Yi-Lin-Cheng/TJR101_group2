import re
import uuid
from pathlib import Path

import pandas as pd

from tasks import fuzzy_match, normalize_address
from utils import to_half_width

data_dir = Path("data", "restaurant")


def filter_types_and_town(data):
    data = data[
        (data["business_status"] != "CLOSED_PERMANENTLY")
        & (
            (data["types"].str.contains("restaurant"))
            | (data["types"].str.contains("cafe"))
        )
    ].copy()

    data["region_open"] = data["region_open"].str.replace("臺", "台", regex=False)
    data = data[
        data.apply(
            lambda row: row["address"] and row["region_open"] in row["address"], axis=1
        )
        & data.apply(
            lambda row: row["address"] and row["town_open"] in row["address"], axis=1
        )
    ]
    return data


def compare_name_and_add(data):
    data["comm"] = data["comm"].fillna(0).astype(int)
    data["add_open"] = data["add_open"].astype(str).apply(normalize_address)
    data["address"] = data["address"].astype(str).apply(normalize_address)

    # 開放資料跟GOOGLE到的資料名稱或地址一樣的留下，其餘的歸類到待處理
    matched_data1 = data[
        (data["name_open"] == data["f_name"]) | (data["add_open"] == data["address"])
    ].copy()
    pending_data = data[
        ~((data["name_open"] == data["f_name"]) | (data["add_open"] == data["address"]))
    ].copy()

    # 留下的資料裡面同一個地點可能有多筆資料，留下評論數高的
    matched_data1 = matched_data1.loc[matched_data1.groupby("id_open")["comm"].idxmax()]
    matched_data1 = matched_data1.drop_duplicates(subset=["place_id"])
    print(f"依據相同名稱或地址留下的筆數{len(matched_data1)}")

    matched_ids = set(matched_data1["id_open"])
    pending_data = pending_data[
        pending_data["id_open"].apply(lambda x: x not in matched_ids)
    ].reset_index(drop=True)

    # 留下名稱相近的
    matched_indices = []
    for i in range(len(pending_data)):
        name1 = pending_data.loc[i, "name_open"]
        name2 = pending_data.loc[i, "f_name"]
        if len(name1) + len(name2) >= 20:
            match, score = fuzzy_match(name1, name2, 65)
        else:
            match, score = fuzzy_match(name1, name2)

        if match:
            matched_indices.append(i)

    matched_data2 = pending_data.loc[matched_indices].copy()
    pending_data = pending_data.drop(matched_indices).copy()

    # 留下的資料裡面同一個地點可能有多筆資料，留下評論數高的
    matched_data2 = matched_data2.loc[matched_data2.groupby("id_open")["comm"].idxmax()]
    matched_data2 = matched_data2.drop_duplicates(subset=["place_id"])

    print(f"依據相近的名稱留下的筆數{len(matched_data2)}")
    # 合併要留下的資料
    matched_data = pd.concat(
        [matched_data1, matched_data2], ignore_index=True
    ).reset_index(drop=True)
    # 刪除重複名稱
    matched_data = matched_data.drop_duplicates(subset="place_id")
    print(f"過濾同ID最後留下的筆數{len(matched_data)}")
    return matched_data


def clean_name(matched_data):
    # 清理名稱
    matched_data = matched_data.copy()
    matched_data.loc[matched_data["f_name"].str.len() > 20, "f_name"] = (
        matched_data["f_name"].str.split(r"\||│|丨|｜|\-|－|/|／").str[0]
    )
    # matched_data["f_name"] = matched_data["f_name"].apply(
    #     lambda name: (
    #         re.split(r"\||│|丨|｜|\-|－|/|／", name)[0] if len(name) > 20 else name
    #     )
    # )
    matched_data["f_name"] = matched_data["f_name"].apply(to_half_width)
    return matched_data


def classify_f_type(types):
    if "restaurant" in types:
        return "餐廳"
    else:
        return "咖啡廳"


def main():
    read_file = data_dir / "restaurant03_googleapi_newdata.csv"
    save_file = data_dir / "restaurant04_compare_name_and_add_new.csv"
    df = pd.read_csv(read_file, encoding="utf-8", engine="python")
    df = filter_types_and_town(df)
    df = compare_name_and_add(df)
    df = clean_name(df)

    df["food_id"] = [str(uuid.uuid4()) for _ in range(len(df))]
    df["gmaps_url"] = "https://www.google.com/maps/place/?q=place_id:" + df["place_id"]
    df["f_type"] = df["types"].apply(classify_f_type)  # 自動分類
    df["geo_loc"] = df.apply(
        lambda row: f"POINT({round(row['lat'], 5):.5f} {round(row['lng'], 5):.5f})",
        axis=1,
    )
    df.rename(columns={"region_open": "county", "town_open": "area"}, inplace=True)
    # 留下要得欄位
    df = df[
        [
            "food_id",
            "f_name",
            "county",
            "address",
            "rate",
            "geo_loc",
            "gmaps_url",
            "f_type",
            "comm",
            "area",
        ]
    ]

    df.to_csv(
        save_file,
        encoding="utf-8",
        header=True,
        index=False,
    )


if __name__ == "__main__":
    main()