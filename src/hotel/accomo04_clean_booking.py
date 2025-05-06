import re

import pandas as pd
from rapidfuzz import fuzz


# -------- 地址標準化函式 --------
def normalize_address(text: str) -> str:
    text = re.sub(r"\s+", "", text.replace("-", "之").replace(",", ""))
    text = re.sub(r"f", "樓", text, flags=re.IGNORECASE)

    city_pattern = r"(台北市|新北市|桃園市|台中市|台南市|高雄市|基隆市|新竹市|嘉義市|新竹縣|苗栗縣|彰化縣|南投縣|雲林縣|嘉義縣|屏東縣|台東縣|花蓮縣|宜蘭縣)"
    area_pattern = r"([\u4e00-\u9fa5]{2}(區|鄉|鎮|市))|北區|東區|南區|西區|中區|那瑪夏區|阿里山鄉|三地門鄉|太麻里鄉"

    text = re.sub(r"(台灣)?\d{3,6}$", "", text)
    text = re.sub(rf"^([0-9]{{3,6}})?台灣", "", text)
    text = re.sub(rf"({city_pattern})([0-9]{{3,5}})?", r"\1", text)
    text = re.sub(r"No\.?([0-9]+(?:之[0-9]+)?)", r"\1號", text, flags=re.IGNORECASE)
    text = re.sub(r"No", "", text, flags=re.IGNORECASE)

    if re.match(rf"^{city_pattern}", text):
        return text

    core = text
    floor_match = re.search(r"([0-9]+樓)(?:之[0-9]+)?", text)
    floor = floor_match.group(1) if floor_match else ""
    core = core.replace(floor, "")

    city = re.search(city_pattern, text)
    city_str = city.group(0) if city else ""
    core = core.replace(city_str, "")

    area = re.search(area_pattern, text)
    area_str = area.group(0) if area else ""
    core = core.replace(area_str, "")

    core = re.sub(r"(號)+", "號", core)
    no = re.search(r"[0-9]+(之[0-9]+)?號", text)
    no_str = no.group(0) if no else ""
    core = core.replace(no_str, "")

    return f"{city_str}{area_str}{core}{no_str}{floor}"

# -------- 名稱正規化 --------
def normalize_text(text):
    text = re.sub(r"\s+", "", text)
    text = text.replace("臺", "台")
    return text

# -------- 模糊比對函式 --------
def fuzzy_match(name1, name2, threshold=80):
    name1 = normalize_text(str(name1))
    name2 = normalize_text(str(name2))
    scores = {
        "ratio": fuzz.ratio(name1, name2),
        "partial": fuzz.partial_ratio(name1, name2),
        "token_sort": fuzz.token_sort_ratio(name1, name2),
        "token_set": fuzz.token_set_ratio(name1, name2),
    }
    best_score = max(scores.values())
    return best_score >= threshold, best_score

# -------- 讀取資料並做前處理 --------
data = pd.read_csv("accomo03_extract_booking.csv", encoding="utf-8", engine="python")
data = data[~data["name"].isna()]
data["add_open"] = data["add_open"].astype(str).apply(normalize_address)
data["add"] = data["add"].astype(str).apply(normalize_address)

# -------- 完全比對：名稱或地址完全一致 --------
matched_data1 = data[
    (data["name_open"] == data["name"]) | (data["add_open"] == data["add"])
].copy()
matched_data1["match_score"] = 100  # 完全比對給分 100

# -------- 其餘資料進入模糊比對 --------
pending_data = data[
    ~((data["name_open"] == data["name"]) | (data["add_open"] == data["add"]))
].reset_index(drop=True)

print(f"完全比對筆數：{len(matched_data1)}")

# -------- 模糊比對（使用原始邏輯，長名稱放寬門檻） --------
matched_indices = []
match_scores = []

for i in range(len(pending_data)):
    name1 = pending_data.loc[i, "name_open"]
    name2 = pending_data.loc[i, "name"]

    if len(name1) + len(name2) >= 20:
        match, score = fuzzy_match(name1, name2, threshold=65)
    else:
        match, score = fuzzy_match(name1, name2, threshold=80)

    if match:
        matched_indices.append(i)
        match_scores.append(score)  # 不限 80 分，保留所有成功比對的實際分數

matched_data2 = pending_data.loc[matched_indices].copy()
matched_data2["match_score"] = match_scores  # 加入模糊比對分數
pending_data = pending_data.drop(matched_indices)

print(f"模糊比對成功筆數：{len(matched_data2)}")

# -------- 合併最終資料 --------
matched_data = pd.concat([matched_data1, matched_data2], ignore_index=True).reset_index(drop=True)

# 1. 先把分數高的排前面
matched_data = matched_data.sort_values("match_score", ascending=False)

# 2. 如果 name + lng + lat 一樣，就只保留第一筆（分數最高那筆）
matched_data = matched_data.drop_duplicates(subset=["name", "lng", "lat"], keep="first")

# 3. 整理 index
matched_data = matched_data.reset_index(drop=True)

print(f"合併後比對成功總筆數：{len(matched_data)}")

# -------- 輸出結果 --------
matched_data.to_csv(
    "accomo04_booking_test_with_score.csv",
    encoding="utf-8",
    header=True,
    index=False,
)    index=False,
