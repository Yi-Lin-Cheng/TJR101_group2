import re
import unicodedata
from pathlib import Path

import pandas as pd
from accomo.remove_phrases import remove_phrases

if Path("/opt/airflow/data").exists():
    data_dir = Path("/opt/airflow/data/accomo")
else:
    data_dir = Path("data/accomo")


# ---------------------
# normalize_text
# ---------------------
def normalize_text(text):
    if pd.isna(text):
        return ""
    text = str(text)
    text = unicodedata.normalize("NFKC", text)
    text = text.upper().replace("臺", "台").replace("区", "區").replace("舘", "館")
    text = re.sub(r"\s+", " ", text)
    return text.strip()


# remove_phrases 全部轉為標準格式，方便後續比對
remove_phrases = [normalize_text(p) for p in remove_phrases]

# ---------------------
# 中文數字轉阿拉伯數字
# ---------------------
zh_number_map = {
    "三十": "30",
    "二十九": "29",
    "二十八": "28",
    "二十七": "27",
    "二十六": "26",
    "二十五": "25",
    "二十四": "24",
    "二十三": "23",
    "二十二": "22",
    "二十一": "21",
    "二十": "20",
    "十九": "19",
    "十八": "18",
    "十七": "17",
    "十六": "16",
    "十五": "15",
    "十四": "14",
    "十三": "13",
    "十二": "12",
    "十一": "11",
    "十": "10",
    "九": "9",
    "八": "8",
    "七": "7",
    "六": "6",
    "五": "5",
    "四": "4",
    "三": "3",
    "二": "2",
    "一": "1",
}


def normalize_chinese_number(text):
    def convert(match):
        zh = match.group(1)
        unit = match.group(2)
        return zh_number_map.get(zh, zh) + unit

    pattern = (
        "("
        + "|".join(sorted(zh_number_map.keys(), key=lambda x: -len(x)))
        + ")(段|號|樓|巷|弄)"
    )
    return re.sub(pattern, convert, text)


# ---------------------
# 地址清理
# ---------------------
def clean_address(text):
    if pd.isna(text):
        return ""
    text = normalize_text(text)
    text = re.sub(r"(\d+)[\-－—‒–－](\d+)", r"\1之\2", text)
    text = re.sub(r"[Bb]?\d+([\-~至]?\d*)?[F樓]", "", text)
    text = re.sub(r"\([^)]*\)", "", text)
    text = re.sub(r"[\u2022\u30FB;:、，。!！「」『』【】﹁﹂？?~～/\\]", "", text)
    text = re.sub(r"\s+", " ", text).strip()
    text = normalize_chinese_number(text)
    return text


# ---------------------
# nmae清理
# ---------------------
def clean_name_advanced(text):
    if pd.isna(text):
        return ""

    # 1. 正規化文字
    text = normalize_text(text)

    # 2. 優先移除干擾詞
    for phrase in remove_phrases:
        if phrase in text:
            text = text.replace(phrase, "")

    # # 3. 移除雜訊符號（含標點）
    # text = re.sub(r"[\(（][^)\）]*[\)）]|[|\u4E28‧•#&.?!~／\\\-]", "", text)
    text = re.sub(
        r"[\(（][^)\）]*[\)）]|[|\u4E28‧•#&.。,?？!~／\\\-_：、|の]", "", text
    )

    # text = re.sub(r"[|\u4E28‧•#&.?!~／\\\-]", "", text)

    # 4. 移除旅館/民宿官方編號
    pattern = r"(台北市|新北市|桃園市|台中市|台南市|高雄市|基隆市|新竹市|嘉義市|新竹縣|苗栗縣|彰化縣|南投縣|雲林縣|嘉義縣|屏東縣|台東縣|花蓮縣|宜蘭縣)(旅館|民宿)(編號)?\d{1,5}號"
    text = re.sub(pattern, "", text)

    return text.strip()


# ---------------------
# 移除住宿類型字尾
# ---------------------
hotel_pattern = [
    "渡假飯店",
    "觀光酒店",
    "國際觀光飯店",
    "溫泉酒店",
    "渡假莊園",
    "民宿",
    "會館",
    "旅店",
    "大飯店",
    "飯店",
    "酒店",
    "精品酒店",
    "旅社",
    "青年旅館",
    "青旅",
    "背包客棧",
    "商務旅館",
    "商旅",
    "渡假村",
    "精品旅館",
    "汽車旅館",
    "客棧",
    "行館",
    "驛站",
]
hotel_pattern = sorted(hotel_pattern, key=lambda x: -len(x))


def remove_hotel_suffix(text):
    if pd.isna(text):
        return ""
    for keyword in hotel_pattern:
        text = text.replace(keyword, "")
    return text.strip()


# ---------------------
# 英文地址清理
# ---------------------
def clean_english_address(text):
    if pd.isna(text):
        return ""
    text = normalize_text(text)
    text = re.sub(r"\s+", " ", text).strip()
    text = re.sub(r"ROAD", "RD.", text)
    text = re.sub(r"STREET", "ST.", text)
    text = re.sub(r"VILLAGE", "VIL.", text)
    text = re.sub(r"SECTION", "SEC.", text)
    text = re.sub(r"號", "NO.", text)
    text = re.sub(r"巷", "LN.", text)
    return text


# ---------------------
# 中文地址翻英文
# ---------------------
def translate_address(addr, region_zh, town_zh):
    result = addr
    for zh, en in rd_dict.items():
        result = result.replace(zh, en)
    for zh, en in vil_dict.items():
        result = result.replace(zh, en)
    if town_zh in result and town_zh in town_dict:
        result = result.replace(town_zh, town_dict[town_zh])
    if region_zh in result and region_zh in region_dict:
        result = result.replace(region_zh, region_dict[region_zh])
    return clean_english_address(result)


# ---------------------
# 主流程
# ---------------------
def main():
    df = pd.read_csv(data_dir / "accomo03_extract_booking.csv")
    if df.empty:
        return

    for col in ["lat_open", "lng_open", "lat", "lng"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    region_map = pd.read_csv(data_dir / "region.csv")
    town_map = pd.read_csv(data_dir / "town.csv")
    vil_map = pd.read_csv(data_dir / "vil.csv")
    rd_map = pd.read_csv(data_dir / "rd.csv")

    region_dict = dict(zip(region_map["region_zh"], region_map["region_en"]))
    town_dict = dict(zip(town_map["town_zh"], town_map["town_en"]))
    vil_dict = dict(zip(vil_map["vil_zh"], vil_map["vil_en"]))
    rd_dict = dict(zip(rd_map["rd_zh"], rd_map["rd_en"]))

    df = df.dropna(subset=["url"])
    df = df[
        (df["lat_open"].between(21.8, 25.4))
        & (df["lng_open"].between(120.0, 122.0))
        & (df["lat"].between(21.8, 25.4))
        & (df["lng"].between(120.0, 122.0))
    ]

    for col in [
        "name_open",
        "region_open",
        "town_open",
        "add_open",
        "name",
        "add",
        "region",
        "town",
    ]:
        df[col] = df[col].apply(normalize_text)

    df["add_open"] = df["add_open"].apply(clean_address)
    df["add"] = df["add"].apply(clean_address)
    df["name_open"] = df["name_open"].apply(clean_name_advanced)
    df["name"] = df["name"].apply(clean_name_advanced)
    df["name_open_clean"] = df["name_open"].apply(remove_hotel_suffix)
    df["name_clean"] = df["name"].apply(remove_hotel_suffix)

    df["add_open_eng"] = df.apply(
        lambda row: translate_address(
            row["add_open"], row["region_open"], row["town_open"]
        ),
        axis=1,
    )

    final_cols = [
        "id_open",
        "name_open",
        "name_open_clean",
        "name",
        "name_clean",
        "add_open",
        "add",
        "add_open_eng",
        "region_open",
        "town_open",
        "region",
        "town",
        "lng_open",
        "lat_open",
        "lng",
        "lat",
        "class_open",
        "rating",
        "user_rating_total",
        "type",
        "facilities",
        "url",
        "img_url",
    ]

    df = df[final_cols]
    df.to_csv(data_dir / "accomo04_clean_word.csv", index=False, encoding="utf-8")
    print("清理完成：accomo04_clean_word.csv 已儲存")


if __name__ == "__main__":
    main()
