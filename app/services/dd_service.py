import os
import uuid
import numpy as np
from typing import Dict, List, Optional, Tuple, Any, Union

from app.core.addressing.zero_index_mapper import ZeroIndexMapper
from app.services.address_manager import AddressManager
from app.services.document_manager import DocumentManager
from app.services.function_service import FunctionService

class DimensionalDirectoryService:
    """
    Main service for the Dimensional Directory system.
    
    This class orchestrates the interactions between different components and
    provides a high-level API for the application layer.
    """
    
    def __init__(self, base_path: str):
        """
        Initialize the Dimensional Directory Service.
        
        Args:
            base_path: Base directory for storing data
        """
        self.base_path = base_path
        
        # Create directories if they don't exist
        os.makedirs(base_path, exist_ok=True)
        
        # Initialize paths for database and HDF5 storage
        self.db_path = os.path.join(base_path, "metadata.db")
        self.hdf5_path = os.path.join(base_path, "embeddings.hdf5")
        
        # Initialize core components
        self.zero_index_mapper = ZeroIndexMapper(self.db_path, self.hdf5_path)
        self.address_manager = AddressManager(self.db_path, self.hdf5_path)
        self.document_manager = DocumentManager(self.db_path, self.hdf5_path, self.address_manager)
        self.function_service = FunctionService(self.db_path, self.zero_index_mapper)
    
    def create_document(self, content: str, dbidL: str, dbidS: Optional[str] = None,
                        title: Optional[str] = None, source: Optional[str] = None,
                        metadata: Optional[Dict] = None) -> Dict[str, Any]:
        """
        Create a new document in the system.
        
        Args:
            content: Document content
            dbidL: Long identifier
            dbidS: Optional short identifier (will be generated if not provided)
            title: Optional document title
            source: Optional document source
            metadata: Optional metadata dictionary
            
        Returns:
            Dictionary with document information
        """
        return self.document_manager.process_document(
            content=content,
            dbidL=dbidL,
            dbidS=dbidS,
            title=title,
            source=source,
            metadata=metadata
        )
    
    def get_document(self, doc_id: str) -> Optional[Dict[str, Any]]:
        """
        Get document information.
        
        Args:
            doc_id: Document identifier (UUID, or L:S formatted as "L:S")
            
        Returns:
            Dictionary with document information or None if not found
        """
        if ":" in doc_id:
            # L:S format
            dbidL, dbidS = doc_id.split(":", 1)
            return self.document_manager.get_document(dbidL=dbidL, dbidS=dbidS)
        else:
            # UUID format
            return self.document_manager.get_document(doc_uuid=doc_id)
    
    def get_all_documents(self) -> List[Dict[str, Any]]:
        """
        Get all documents in the system.
        
        Returns:
            List of document information
        """
        return self.document_manager.get_all_documents()
    
    def get_sentence(self, sentence_uuid: str) -> Optional[Dict[str, Any]]:
        """
        Get sentence information.
        
        Args:
            sentence_uuid: Sentence UUID
            
        Returns:
            Dictionary with sentence information or None if not found
        """
        return self.document_manager.get_sentence(sentence_uuid)
    
    def get_all_sentences(self) -> List[Dict[str, Any]]:
        """
        Get all sentences in the system.
        
        Returns:
            List of sentence information
        """
        return self.document_manager.get_all_sentences()
    
    def find_documents_with_sentence(self, sentence_text: str) -> List[Dict[str, Any]]:
        """
        Find all documents containing a specific sentence.
        
        Args:
            sentence_text: Sentence text to search for
            
        Returns:
            List of documents containing the sentence
        """
        return self.document_manager.find_documents_with_sentence(sentence_text)
    
    def search_by_token(self, token_text: str) -> List[Dict[str, Any]]:
        """
        Search for sentences and documents containing a specific token.
        
        Args:
            token_text: Token text to search for
            
        Returns:
            List of matches with sentence and document information
        """
        return self.document_manager.search_by_token(token_text)
    
    def create_address(self, levels: List[str], attributes: Optional[List[str]] = None,
                      uuid_value: Optional[str] = None, addr_type: Optional[str] = None) -> str:
        """
        Create a hierarchical address.
        
        Args:
            levels: List of hierarchical levels
            attributes: Optional list of attributes
            uuid_value: Optional UUID to associate with the address
            addr_type: Optional type of address
            
        Returns:
            The full address string
        """
        # Combine levels and attributes
        base_addr = "-".join(levels)
        if attributes and len(attributes) > 0:
            full_addr = f"{base_addr}_{'_'.join(attributes)}"
        else:
            full_addr = base_addr
        
        # Create address with zero index (always 0 for the base level)
        return self.address_manager.create_address(full_addr, uuid_value, addr_type, 0)
    
    def resolve_address(self, addr: str, relative_to: Optional[str] = None) -> Dict[str, Any]:
        """
        Resolve an address, optionally relative to another address.
        
        Args:
            addr: The address to resolve
            relative_to: Optional reference address for relative resolution
            
        Returns:
            Dictionary with resolved address information
        """
        return self.address_manager.resolve_address(addr, relative_to)
    
    def set_relation(self, source_addr: str, target_addr: str, relation_type: str) -> bool:
        """
        Create a relationship between two addresses.
        
        Args:
            source_addr: Source address
            target_addr: Target address
            relation_type: Type of relationship
            
        Returns:
            True if successful, False otherwise
        """
        return self.address_manager.set_relation(source_addr, target_addr, relation_type)
    
    def evaluate_function(self, formula: str, context_cell: Optional[Tuple[int, int]] = None) -> Any:
        """
        Evaluate a function formula.
        
        Args:
            formula: Formula string (e.g., "rel(A1, 'synonym')")
            context_cell: Optional tuple with (row, col) of the current cell
            
        Returns:
            Function result
        """
        return self.function_service.evaluate(formula, context_cell)
    
    def add_embedding(self, uuid_value: str, embedding: np.ndarray, entity_type: str = "sentence",
                    token_position: Optional[int] = None) -> bool:
        """
        Add embedding for a sentence or token.
        
        Args:
            uuid_value: UUID of the entity
            embedding: Numpy array containing the embedding
            entity_type: Type of entity ("sentence" or "token")
            token_position: Position of token within sentence (required if entity_type is "token")
            
        Returns:
            True if successful, False otherwise
        """
        return self.document_manager.add_embedding(uuid_value, embedding, entity_type, token_position)
    
    def get_embedding(self, uuid_value: str, entity_type: str = "sentence",
                     token_position: Optional[int] = None) -> Optional[np.ndarray]:
        """
        Get embedding for a sentence or token.
        
        Args:
            uuid_value: UUID of the entity
            entity_type: Type of entity ("sentence" or "token")
            token_position: Position of token within sentence (required if entity_type is "token")
            
        Returns:
            Numpy array containing the embedding or None if not found
        """
        return self.document_manager.get_embedding(uuid_value, entity_type, token_position)