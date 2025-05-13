import pandas as pd
from pathlib import Path

data_dir = Path("data", "food")

base_file = data_dir / "restaurant05_extract_googlemap.csv"
cafe_file = data_dir / "cleaned_cafe_final.csv"
taipei_file = data_dir / "taipei_restaurant_cleaned.csv"
new_file = data_dir / "food05_extract_googlemap.csv"
def main():
    data1 = pd.read_csv(
            base_file, encoding="utf-8", engine="python"
        )
    data2 = pd.read_csv(
            cafe_file, encoding="utf-8", engine="python"
        )
    data3 = pd.read_csv(
            taipei_file, encoding="utf-8", engine="python"
        )

    all_data = pd.concat([data1, data2,data3], ignore_index=True)

    all_data.to_csv(new_file,encoding="utf-8",
            header=True,
            index=False,
        )

if __name__ == "__main__":
    main()