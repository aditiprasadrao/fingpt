import re
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

_analyzer = SentimentIntensityAnalyzer()

def clean_text(text: str) -> str:
    """
    Basic cleaning: remove URLs, control characters, and extra whitespace.
    """
    if not text:
        return ""
    text = re.sub(r"http\S+", "", text)          # remove URLs
    text = re.sub(r"[\r\n\t]+", " ", text)       # remove newlines/tabs
    text = re.sub(r"[^\\w\\s\\-\\'\\\"]+", " ", text)  # keep words, whitespace, basic punctuation
    text = re.sub(r"\\s+", " ", text).strip()
    return text

def score_text(text: str) -> float:
    """
    Return VADER compound score in [-1.0, 1.0].
    """
    if text is None:
        return 0.0
    cleaned = clean_text(text)
    vs = _analyzer.polarity_scores(cleaned)
    return float(vs.get("compound", 0.0))
