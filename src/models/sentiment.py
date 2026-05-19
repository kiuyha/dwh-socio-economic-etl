from core import log
from transformers import pipeline, AutoTokenizer, AutoModelForSequenceClassification
from typing import List
import pandas as pd
from src.utils.types import SentimentDict

def predict_sentiment_batch(texts: pd.Series, batch_size: int = 16) -> List[SentimentDict]:
    sentiment_pipeline = pipeline(
        task="sentiment-analysis", # type: ignore
        model="",
        tokenizer="",
        device=0
    )

    if sentiment_pipeline is None:
        log.warning("sentiment pipeline not available, returning empty results")
        raise Exception("sentiment pipeline not available")
    try:
        valid_texts = (t if t and isinstance(t, str) else "" for t in texts)

        pipeline_iterator = sentiment_pipeline(
            valid_texts,
            batch_size=batch_size,
            truncation=True,
            max_length=512
        )

        label_sentiment = 'neutral'
        confidence = 0.5
        
        return [
            {
                "label" : label_sentiment,
                "confidence": confidence
            }
        ]

    except Exception as e:
        log.error(f"Error in sentiment extraction: {e}")
        raise e