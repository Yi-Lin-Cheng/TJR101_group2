import re
from rapidfuzz import fuzz


def normalize_text(text):
    # 可以根據需要加入繁簡轉換或特殊字符處理
    text = re.sub(r"\s+", "", text)  # 移除所有空白
    text = text.replace("臺", "台")  # 正規化
    return text


def fuzzy_match(name1, name2, threshold=80):
    name1 = normalize_text(str(name1))
    name2 = normalize_text(str(name2))
    # 計算多種相似度分數
    scores = {
        "ratio": fuzz.ratio(name1, name2),
        "partial": fuzz.partial_ratio(name1, name2),
        "token_sort": fuzz.token_sort_ratio(name1, name2),
        "token_set": fuzz.token_set_ratio(name1, name2),
    }
    best_score = max(scores.values())
    return best_score >= threshold, best_score
