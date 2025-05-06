from charset_normalizer import from_path


def encoding_transform(file_path):
    """
    Guess file encoding and convert to UTF-8 if needed.

    Args:
        file_path (str): Path to the input file.

    Rewrites the file with UTF-8 encoding if the original is different.
    """
    # 自動偵測並讀取檔案
    result = from_path(file_path)

    # 通常第一個結果就是最準的
    best_guess = result.best()

    # 取得推測的編碼
    encoding = best_guess.encoding
    print(f"偵測到編碼：{encoding}")
    if encoding.lower() == "utf-8":
        return

    # 讀取文字內容
    content = best_guess.text

    # 轉成 UTF-8 存檔
    with open(file_path, "w", encoding="utf-8", newline="") as f:
        f.write(content)
