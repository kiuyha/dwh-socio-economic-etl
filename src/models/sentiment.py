import os
from core import log
from transformers import pipeline, AutoTokenizer, AutoModelForSequenceClassification
from typing import List
import pandas as pd
from src.utils.types import SentimentDict

# ── konstanta ────────────────────────────────────────────────────
PRETRAINED = "mdhugol/indonesia-bert-sentiment-classification"
_BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
MODEL_DIR = os.path.join(_BASE_DIR, "models", "sentiment")
LABEL_INDEX = {
    "LABEL_0": "positive",
    "LABEL_1": "neutral",
    "LABEL_2": "negative",
}

# ── load model: local dulu, kalau tidak ada baru download ────────
if os.path.exists(MODEL_DIR):
    log.info(f"Loading sentiment model from local: {MODEL_DIR}")
    _model     = AutoModelForSequenceClassification.from_pretrained(MODEL_DIR)
    _tokenizer = AutoTokenizer.from_pretrained(MODEL_DIR)
else:
    log.info("Downloading sentiment model from HuggingFace...")
    _model     = AutoModelForSequenceClassification.from_pretrained(PRETRAINED)
    _tokenizer = AutoTokenizer.from_pretrained(PRETRAINED)
    os.makedirs(MODEL_DIR, exist_ok=True)
    _model.save_pretrained(MODEL_DIR)
    _tokenizer.save_pretrained(MODEL_DIR)
    log.info(f"Model saved to {MODEL_DIR}")

_sentiment_pipeline = pipeline(
    task="sentiment-analysis",
    model=_model,
    tokenizer=_tokenizer,
    device=-1,  # CPU; ganti ke 0 kalau pakai GPU
)

log.info("Sentiment pipeline ready")


# ── fungsi utama ─────────────────────────────────────────────────
def predict_sentiment_batch(
    texts: pd.Series,
    batch_size: int = 16,
) -> List[SentimentDict]:
    clean_texts: List[str] = [
        t if (t and isinstance(t, str)) else ""
        for t in texts
    ]

    try:
        results = _sentiment_pipeline(
            clean_texts,
            batch_size=batch_size,
            truncation=True,
            max_length=512,
        )

        output: List[SentimentDict] = []
        for item in results:
            label_raw  = item["label"]
            label      = LABEL_INDEX.get(label_raw, "neutral")
            confidence = round(float(item["score"]), 4)
            output.append({"label": label, "confidence": confidence})

        return output

    except Exception as e:
        log.error(f"Error in sentiment extraction: {e}")
        raise