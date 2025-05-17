import re
from pathlib import Path

import pandas as pd

from hotel.facility_map import replacements  # 匯入自定義設施縮寫對照表

# -------- 讀取來源資料 --------
if Path("/opt/airflow/data").exists():
    data_dir = Path("/opt/airflow/data/hotel")
else:
    data_dir = Path("data/hotel")
input_path = data_dir / "accomo05_match_result.csv"
df = pd.read_csv(input_path, encoding="utf-8")


def main():
    # -------- accomo_id：使用 id_open --------
    df["accomo_id"] = df["id_open"]

    # -------- 保留原始經緯度欄位，不產生 geo_loc --------

    # -------- 欄位轉換 --------
    df["a_name"] = df["name"]
    df["county"] = df["region_open"]
    df["address"] = df["add"]
    df["rate"] = df["rating_score5"]
    df["pic_url"] = df["img_url"]
    df["b_url"] = df["url"]
    df["ac_type"] = df["type"]
    df["comm"] = df["user_rating_total"]
    df["area"] = df["town_open"]

    # -------- 設施推薦簡句（fac 欄位） --------
    def generate_fac_note(text):
        if pd.isna(text):
            return ""

        # 清理步驟：移除「熱門設施」與括號內文字
        text = text.replace("熱門設施", "")
        text = re.sub(r"[\(（][^\)）]*[\)）]", "", text).strip()

        used = []
        for k in sorted(replacements.keys(), key=lambda x: -len(x)):
            if k in text:
                used.append(replacements.get(k, k))
                text = text.replace(k, "")
            if len(used) >= 3:
                break

        return "提供" + " ".join(used) if used else ""

    df["fac"] = df["facilities"].apply(generate_fac_note)

    # -------- 選擇輸出欄位順序（對應 SQL 欄位）--------
    # 加入 lat, lng 欄位，不要 geo_loc
    df["geo_loc"] = df.apply(lambda row: f"POINT({row["lat"]} {row["lng"]})", axis=1)
    output_cols = [
        "accomo_id",
        "a_name",
        "county",
        "address",
        "rate",
        "geo_loc",
        "pic_url",
        "b_url",
        "ac_type",
        "comm",
        "area",
        "fac",
    ]
    df_out = df[output_cols]

    # -------- 儲存輸出檔案 --------
    output_path = data_dir / "accomo06_for_db.csv"
    df_out.to_csv(output_path, index=False, encoding="utf-8")

    print(f"轉換完成，檔案已儲存：{output_path}")


if __name__ == "__main__":
    main()
