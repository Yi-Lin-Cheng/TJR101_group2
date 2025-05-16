import pandas as pd
from pathlib import Path
from rapidfuzz import fuzz
import math
import re

# -------- 檔案路徑 --------
file_path = Path(__file__).resolve().parents[2] / "data" / "hotel" / "accomo04_clean_word.csv"
df = pd.read_csv(file_path, encoding="utf-8")

# -------- Haversine 計算距離 --------
def haversine(lat1, lng1, lat2, lng2):
    R = 6371
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    d_phi = math.radians(lat2 - lat1)
    d_lambda = math.radians(lng2 - lng1)
    a = math.sin(d_phi/2)**2 + math.cos(phi1)*math.cos(phi2)*math.sin(d_lambda/2)**2
    return R * (2 * math.atan2(math.sqrt(a), math.sqrt(1 - a)))

# -------- 比對與計算 --------
match_score = []

for _, row in df.iterrows():
    name_open = row["name_open"]
    name = row["name"]
    name_open_clean = row["name_open_clean"]
    name_clean = row["name_clean"]
    add_open = row["add_open"]
    add = row["add"]
    region = row["region"]
    town = row["town"]
    lat_open = row["lat_open"]
    lng_open = row["lng_open"]
    lat = row["lat"]
    lng = row["lng"]

    # 清理空白
    add_open_clean = add_open.replace(" ", "")
    add_clean = add.replace(" ", "")

    name_score = fuzz.token_sort_ratio(name_open_clean, name_clean)
    add_score = fuzz.ratio(add_open_clean, add_clean)
    partial_score = fuzz.partial_ratio(name_clean, name_open_clean)

    distance_km = -1
    if pd.notna(lat_open) and pd.notna(lat) and pd.notna(lng_open) and pd.notna(lng):
        distance_km = haversine(lat_open, lng_open, lat, lng)

    # 去除數字與英字
    core_name_clean = re.sub(r"[0-9a-zA-Z]+", "", name_clean)

    # 比對分數分類
    if ((name_open == name or name_open_clean == name_clean or name_clean in name_open_clean) and add_open_clean == add_clean) \
        or (name_score >= 90 and add_open_clean == add_clean):
        match_score.append(100)
    elif name_score >= 85 and add_score >= 85:
        match_score.append(90)
    elif name_score >= 80 and (region in add_open_clean or town in add_open_clean):
        match_score.append(80)
    elif partial_score >= 80 and distance_km >= 0 and distance_km <= 3:
        match_score.append(75)
    else:
        match_score.append(0)

# -------- 新增欄位 --------
df["match_score"] = [round(s) for s in match_score]

def compute_distance(row):
    if pd.notna(row["lat_open"]) and pd.notna(row["lat"]) and pd.notna(row["lng_open"]) and pd.notna(row["lng"]):
        return haversine(row["lat_open"], row["lng_open"], row["lat"], row["lng"])
    return -1

df["distance_km"] = df.apply(compute_distance, axis=1)

# -------- 挑選比對佳者 --------
df = df.sort_values(by=["name", "lat", "lng", "match_score", "distance_km"], ascending=[True, True, True, False, True])
df = df.drop_duplicates(subset=["name", "lat", "lng"], keep="first")

# -------- 篩選比對分數 --------
df = df[df["match_score"] >= 75].copy()

# -------- 新增 rating_score5 欄位 --------
df.insert(df.columns.get_loc("rating") + 1, "rating_score5", df["rating"].apply(lambda x: round(x / 2, 1) if pd.notna(x) else x))

# -------- 最終輸出欄位 --------
final_cols = [
    "id_open", "name", "add", "region_open", "town_open",
    "lng", "lat", "rating", "rating_score5", "user_rating_total", "type", "facilities", "url", "img_url"
]
df = df[final_cols]

# -------- 輸出檔案 --------
output_path = file_path.parent / "accomo05_match_result.csv"
df.to_csv(output_path, index=False, encoding="utf-8-sig")

print(f"儲存完成：{output_path}")