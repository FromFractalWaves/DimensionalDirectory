# app/services/processors/text.py
from app.services.processors.base import DocumentProcessor
from typing import List, Tuple
from transformers import AutoTokenizer
import hashlib

class TextDocumentProcessor(DocumentProcessor):
    def __init__(self, batch_type: str = "sentence"):
        self.tokenizer = AutoTokenizer.from_pretrained("bert-base-uncased")
        self.batch_type = batch_type  # "sentence", "paragraph", "page"

    def preprocess(self, raw_content: str) -> List[Tuple[str, str, str]]:
        """
        Preprocess raw content into addressable units with hashes for deduplication.
        Returns [(address, text, hash)].
        """
        if self.batch_type == "sentence":
            units = [s.strip() for s in raw_content.split(".") if s.strip()]
        elif self.batch_type == "paragraph":
            units = [p.strip() for p in raw_content.split("\n\n") if p.strip()]
        elif self.batch_type == "page":
            units = [p.strip() for p in raw_content.split("\f") if p.strip()]
        else:
            raise ValueError(f"Unsupported batch_type: {self.batch_type}")

        # Compute hash for each unit
        return [(f"{i}", unit, hashlib.sha256(unit.encode('utf-8')).hexdigest()) 
                for i, unit in enumerate(units)]

    def tokenize(self, text: str) -> List[Tuple[str, str]]:
        tokens = self.tokenizer.tokenize(text)
        filtered_tokens = [t for t in tokens if not t.startswith("##") and t not in ["[CLS]", "[SEP]"]]
        return [(f"{i}", token) for i, token in enumerate(filtered_tokens)]