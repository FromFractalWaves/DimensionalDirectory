from abc import ABC, abstractmethod
from typing import Dict, List, Tuple

class DocumentProcessor(ABC):
    """Abstract base class for document processing."""

    @abstractmethod
    def preprocess(self, raw_content: str) -> List[Tuple[str, str]]:
        """Preprocess raw content into addressable units. Returns [(address, text)]."""
        pass

    @abstractmethod
    def tokenize(self, text: str) -> List[Tuple[str, str]]:
        """Tokenize text into addressable tokens. Returns [(address, token)]."""
        pass