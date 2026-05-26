import os
import pandas as pd
from typing import List, Union
from core import log
from src.utils.types import TopicDict

import gensim.corpora as corpora
from gensim.models import LdaModel

# Constants
_BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
MODEL_DIR = os.path.join(_BASE_DIR, "models", "topic")

# Globals for caching
_lda_model: LdaModel | None = None
_dictionary: corpora.Dictionary | None = None

def _load_model() -> tuple[LdaModel, corpora.Dictionary]:
    lda_path = os.path.join(MODEL_DIR, "lda.model")
    dict_path = os.path.join(MODEL_DIR, "dictionary.gensim")

    if not (os.path.exists(lda_path) and os.path.exists(dict_path)):
        raise FileNotFoundError(f"Model files missing. Ensure lda.model and dictionary.gensim exist in {MODEL_DIR}")

    log.info(f"Loading topic model from local: {MODEL_DIR}")
    dictionary = corpora.Dictionary.load(dict_path)
    lda_model = LdaModel.load(lda_path)
    
    return lda_model, dictionary

def _auto_label(keywords: List[str]) -> tuple[str, str]:
    """
    Assign label and category based on dominant keywords.
    """
    keyword_set = set(keywords)

    rules = [
        ({"inflasi", "harga", "naik", "turun", "bahan", "pokok"},        "Inflasi & Harga",     "Ekonomi Makro"),
        ({"kerja", "pengangguran", "phk", "buruh", "gaji", "upah"},      "Ketenagakerjaan",     "Ekonomi Mikro"),
        ({"miskin", "kemiskinan", "bantuan", "sosial", "subsidi"},       "Kemiskinan & Sosial", "Kesejahteraan"),
        ({"pajak", "apbn", "anggaran", "belanja", "fiskal"},             "Fiskal & Pajak",      "Ekonomi Makro"),
        ({"rupiah", "dolar", "kurs", "nilai", "tukar", "bank"},          "Moneter & Kurs",      "Ekonomi Makro"),
        ({"investasi", "modal", "saham", "pasar", "bursa"},              "Investasi & Pasar",   "Keuangan"),
        ({"pangan", "beras", "pangan", "petani", "pertanian"},           "Pangan & Pertanian",  "Sektor Riil"),
        ({"energi", "bbm", "listrik", "minyak", "gas"},                  "Energi",              "Sektor Riil"),
        ({"utang", "hutang", "pinjaman", "kredit", "cicilan"},           "Utang & Kredit",      "Keuangan"),
        ({"korupsi", "pejabat", "pemerintah", "kebijakan", "politik"},   "Kebijakan & Politik", "Politik"),
    ]

    for keyword_rule, label, category in rules:
        if keyword_rule & keyword_set:
            return label, category

    return "Lainnya", "Umum"

def predict_topic(texts: pd.Series) -> List[TopicDict]:
    global _lda_model, _dictionary

    if _lda_model is None or _dictionary is None:
        _lda_model, _dictionary = _load_model()

    output: List[TopicDict] = []

    for text in texts:
        # Accommodate preprocessed text as either a string or list of tokens
        if isinstance(text, str):
            tokens = text.split()
        elif isinstance(text, list):
            tokens = text
        else:
            tokens = []

        if not tokens:
            output.append({
                "label": "Tidak Terklasifikasi",
                "category": "Umum",
                "top_keywords": []
            })
            continue

        bow = _dictionary.doc2bow(tokens)
        topic_dist = _lda_model.get_document_topics(bow)

        if not topic_dist:
            output.append({
                "label": "Tidak Terklasifikasi",
                "category": "Umum",
                "top_keywords": []
            })
            continue

        # Get the topic with the highest probability
        best_topic_id = max(topic_dist, key=lambda x: x[1])[0]
        top_keywords = [
            word for word, _ in _lda_model.show_topic(best_topic_id, topn=10)
        ]

        label, category = _auto_label(top_keywords)
        output.append({
            "label": label,
            "category": category,
            "top_keywords": top_keywords[:5] 
        })

    return output