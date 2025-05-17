import re

import pandas as pd

data_name = {
    "third_step_e": "./data/accupass/e_03_accupass_latlon.csv",
    "four_step_t": "./data/accupass/t_04_accupass_clean.csv",
}


def save_to_csv(data, key):
    filename = data_name.get(key)
    data.to_csv(filename, index=False, encoding="utf-8")


def read_from_csv(key):
    filename = data_name.get(key)
    df = pd.read_csv(filename, encoding="utf-8")
    return df


def clean_address(text):
    """address地址清理"""
    # 移除台灣
    # text = re.sub(r"台灣\s*", "", text)

    # 統一樓層格式
    text = re.sub("F", "樓", text)

    # 統一縣市格式
    text = re.sub(r"臺([北中南東])", r"台\1", text)
    text = re.sub(r"(\b台中(?:市)?)\1", "", text)

    # 統一括號格式
    text = re.sub(r'^"(.*)"$', r"\1", text)
    text = re.sub(r"（(.*?)）", r"(\1)", text)

    # 清理地址
    text = re.sub(r"(路|段|街)[\s\u3000\u200b\u200c\u200d\uFEFF]+", r"\1", text)
    text = re.sub(r"(巷)[\s\u3000\u200b\u200c\u200d\uFEFF]+", r"\1", text)
    text = re.sub(r"(號|樓)[\s\u3000\u200b\u200c\u200d\uFEFF]+", r"\1", text)
    text = re.sub(r"[\s\u3000\u200b\u200c\u200d\uFEFF](巷|號|樓)", r"\1", text)
    text = re.sub(r"(之)[0-9][0-9][0-9]+", r"\1", text)

    # 清理英文
    text = re.sub(
        r",?\s?(Taipei|New Taipei|Kaohsiung|Taichung|Tainan|Chiayi|Keelung|Hsinchu)\s?City,?\s?Taiwan",
        "",
        text,
    )

    # 清理郵遞區號
    text = re.sub(r"^\d{3,6}\s*", "", text)
    text = re.sub(r"(縣|市)[\d]{3,6}", r"\1", text)

    text = re.sub("Sherlock Board game store", "夏洛克桌遊專賣店", text)

    # 清理語助詞與標記
    text = re.sub(r"可\s*Google\s*地圖|請自行查詢|參考(?:大略)?位置|查地圖", "", text)
    text = re.sub(r"[；。\.]{1,}", "。", text)
    text = re.sub(r",", "", text)

    return text.strip()


def clean_address_add(text):
    text = re.sub(r"^(基隆|台北|新北|桃園|台中|台南|高雄)(?![縣市])", r"\1市", text)
    text = re.sub(
        r"^(宜蘭|苗栗|彰化|南投|雲林|屏東|花蓮|台東)(?![縣市])", r"\1縣", text
    )
    return text


def add_region_town(df):
    df["city"] = df["address"].str.extract(r"(\w{1,3}?[縣市])")
    return df


def add_start_end_date(df):
    """開始 & 結束時間"""
    s_times = []
    e_times = []

    df["e_time"] = df["e_time"].str.replace(r"\([A-Za-z]{3}\)", "", regex=True)

    pattern = re.compile(
        (
            r"(?P<start_date>\d{4}\.\d{2}\.\d{2}\s*)"
            r"(?P<start_time>\d{2}\:\d{2})"
            r"\s*\-\s*"
            r"(?:"
            r"(?P<end_full_date>\d{4}\.\d{2}\.\d{2}\s*\d{2}\:\d{2})"
            r"|(?P<end_date_and_time>\d{2}\.\d{2}\s*\d{2}\:\d{2})"
            r"|(?P<end_time>\d{2}\:\d{2})"
            r")"
        )
    )

    for text in df["e_time"]:
        match = pattern.match(text)
        if match:
            s_time = f"{match.group('start_date')} {match.group('start_time')}"
            s_times.append(s_time)

            if match.group("end_full_date"):
                e_time = f"{match.group('end_full_date')}"

            elif match.group("end_date_and_time"):
                e_time = f"{match.group('start_date')[:4]} {match.group('end_date_and_time')}"

            elif match.group("end_time"):
                e_time = f"{match.group('start_date')} {match.group('end_time')}"

            e_times.append(e_time)

        else:
            s_times.append(pd.NaT)
            e_times.append(pd.NaT)

    df["s_time"] = s_times
    df["s_time"] = df["s_time"].str.replace(r"\s+", " ", regex=True)
    df["s_time"] = pd.to_datetime(df["s_time"])

    df["e_time"] = e_times
    df["e_time"] = df["e_time"].str.replace(r"\s+", " ", regex=True)
    df["e_time"] = df["e_time"].str.replace(
        r"(\d{4})\s(\d{2}\.\d{2})", r"\1.\2", regex=True
    )
    df["e_time"] = pd.to_datetime(df["e_time"])
    return df


def clean_dropna_first(df):
    df.dropna(subset=["geo_loc"], inplace=True)
    df.dropna(subset=["address"], inplace=True)
    return df


def clean_dropna_end(df):
    df.dropna(subset=["city"], inplace=True)
    df.drop(columns="Unnamed: 0", errors="ignore")
    return df


def t_accupass_data_clean():
    df = read_from_csv("third_step_e")

    df = clean_dropna_first(df)
    df["address"] = df["address"].apply(clean_address)
    df["address"] = df["address"].apply(clean_address_add)
    df = add_region_town(df)
    df = add_start_end_date(df)

    df = df.rename(columns={"city": "county"})
    df = df[
        [
            "e_name",
            "s_time",
            "e_time",
            "county",
            "address",
            "geo_loc",
            "pic_url",
            "accupass_url",
            "tag",
        ]
    ]

    save_to_csv(df, "four_step_t")
    print("清理完成!")


if __name__ == "__main__":
    t_accupass_data_clean()
