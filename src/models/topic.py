import pandas as pd
from typing import List
from src.utils.types import TopicDict

def predict_topic(texts: pd.Series) -> List[TopicDict]:
    
    label = "inflasi"
    category = "Ekonomi Makro"
    top_keywords = ["inflasi", "pembangunan"]
    return [
        {
            "label": label,
            "category": category,
            "top_keywords": top_keywords
        }
    ]