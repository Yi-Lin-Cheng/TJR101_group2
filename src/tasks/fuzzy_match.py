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
        "ratio": fuzz.ratio(name1, name2),
        "partial": fuzz.partial_ratio(name1, name2),
        "token_sort": fuzz.token_sort_ratio(name1, name2),
        "token_set": fuzz.token_set_ratio(name1, name2),
    }
    best_score = max(scores.values())
    match = best_score >= threshold
    return match, best_score
