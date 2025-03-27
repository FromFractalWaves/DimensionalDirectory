# app/services/dimensional_directory_service.py
from app.core.dd_manager import DimensionalDirectory
from app.core.content_mapper import DocumentMapper
from app.core.lstable_manager import LStableManager
from typing import Optional, Dict, List

class DimensionalDirectoryService:
    def __init__(self, base_path: str):
        self.dd = DimensionalDirectory(base_path=base_path)
        self.document_mapper = self.dd.document_mapper
        self.lstable_manager = self.dd.lstable_manager

    def create_document(self, content: str, dbidL: str, dbidS: Optional[str] = None, 
                       title: Optional[str] = None, source: Optional[str] = None, 
                       metadata: Optional[Dict] = None) -> Dict:
        dbidL, dbidS = self.lstable_manager.register_mapping(dbidL, dbidS)
        return self.document_mapper.create_document(content, dbidL, dbidS, title, source, metadata)

    def get_document(self, doc_uuid: str) -> Optional[Dict]:
        return self.document_mapper.get_document(doc_uuid)

    def get_all_documents(self) -> List[Dict]:
        return self.document_mapper.get_all_documents()

    def get_sentence(self, sentence_uuid: str) -> Optional[Dict]:
        return self.document_mapper.get_sentence(sentence_uuid)

    def get_all_sentences(self) -> List[Dict]:
        return self.document_mapper.get_all_sentences()

    def search_by_token(self, query: str) -> List[Dict]:
        return self.document_mapper.search_by_token(query)

    def find_documents_with_sentence(self, sentence_text: str) -> List[Dict]:
        return self.document_mapper.find_documents_with_sentence(sentence_text)

    def register_mapping(self, dbidL: str, dbidS: Optional[str] = None, description: Optional[str] = None) -> Tuple[str, str]:
        return self.lstable_manager.register_mapping(dbidL, dbidS, description)

    def get_mapping(self, dbidL: Optional[str] = None, dbidS: Optional[str] = None) -> Optional[Dict]:
        return self.lstable_manager.get_mapping(dbidL, dbidS)

    def list_mappings(self) -> List[Dict]:
        return self.lstable_manager.list_mappings()