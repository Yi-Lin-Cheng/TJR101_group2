import re

from rapidfuzz import fuzz


def normalize_text(text):
    text = re.sub(r"\s+", "", text)  # 移除所有空白
    text = text.replace("臺", "台")  # 正規化
    return text


def fuzzy_match(name1: str, name2: str, threshold=80):
    """
    Perform fuzzy matching between two names using multiple strategies,
    and return the highest similarity score.

    Args:
        name1 (str): The first name to compare.
        name2 (str): The second name to compare.
        threshold (int, optional): The minimum score to consider as a match. Default is 80.

    Returns:
        tuple:
            match (bool): Whether the highest score exceeds the threshold.
            best_score (int): The highest similarity score among the matching strategies.
    """
    name1 = normalize_text(str(name1))
    name2 = normalize_text(str(name2))
    # 計算多種相似度分數
    scores = {
        "ratio": fuzz.ratio(name1, name2),  #計算需要的最少編輯次數/較長字串字數
        "partial": fuzz.partial_ratio(name1, name2),    #找出重複的字數/較長字串字數
        "token_sort": fuzz.token_sort_ratio(name1, name2),  #將字串依空白切開為 token，排序後再合併為新字串再比對
        "token_set": fuzz.token_set_ratio(name1, name2),    #會取兩個 token 的交集與差集，並比對「共同詞語」與「完整詞語」的組合。
    }
    best_score = max(scores.values())
    match = best_score >= threshold
    return match, best_score
