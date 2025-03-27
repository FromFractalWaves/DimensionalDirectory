import sqlite3
import h5py
import os
from typing import Dict, List, Optional, Tuple, Any

class ZeroIndexMapper:
    """
    Manages the mapping between zero-indexed addresses and UUIDs.
    
    This is a central component of the refactored system, implementing 
    the many-to-many sentence mapping with UUID deduplication while 
    maintaining zero-indexed addressing in the interface.
    """
    
    def __init__(self, db_path: str, hdf5_path: str):
        """
        Initialize the zero-index mapper.
        
        Args:
            db_path: Path to the SQLite database
            hdf5_path: Path to the HDF5 storage
        """
        self.db_path = db_path
        self.hdf5_path = hdf5_path
        
        # Ensure the parent directories exist
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
        os.makedirs(os.path.dirname(self.hdf5_path), exist_ok=True)
    
    def map_addr_to_uuid(self, address: str) -> Optional[str]:
        """
        Convert an address (e.g., 'doc:123-0') to a UUID.
        
        Args:
            address: Address in the format 'doc:id-position'
            
        Returns:
            UUID string or None if not found
        """
        parts = address.split('-')
        if len(parts) < 2:
            return None
        
        doc_id = parts[0].split(':')[1] if ':' in parts[0] else parts[0]
        position = int(parts[1])
        
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute(
                """
                SELECT s.uuid 
                FROM sentences s
                JOIN doc_sentence_map m ON s.uuid = m.sentence_uuid
                WHERE m.doc_uuid = ? AND m.position = ?
                """,
                (doc_id, position)
            )
            
            result = cursor.fetchone()
            return result[0] if result else None
    
    def map_uuid_to_addr(self, uuid: str, doc_id: Optional[str] = None) -> List[str]:
        """
        Convert a UUID to a list of addresses where it appears.
        
        Args:
            uuid: Sentence UUID
            doc_id: Optional document ID to filter by
            
        Returns:
            List of address strings
        """
        with sqlite3.connect(self.db_path) as conn:
            if doc_id:
                cursor = conn.execute(
                    """
                    SELECT m.doc_uuid, m.position 
                    FROM doc_sentence_map m
                    WHERE m.sentence_uuid = ? AND m.doc_uuid = ?
                    ORDER BY m.position
                    """,
                    (uuid, doc_id)
                )
            else:
                cursor = conn.execute(
                    """
                    SELECT m.doc_uuid, m.position 
                    FROM doc_sentence_map m
                    WHERE m.sentence_uuid = ?
                    ORDER BY m.doc_uuid, m.position
                    """,
                    (uuid,)
                )
            
            return [f"doc:{row[0]}-{row[1]}" for row in cursor.fetchall()]
    
    def get_sentence_info(self, uuid: str) -> Optional[Dict[str, Any]]:
        """
        Get detailed information about a sentence by UUID.
        
        Args:
            uuid: Sentence UUID
            
        Returns:
            Dictionary with sentence information
        """
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute(
                "SELECT text, hash, created_at FROM sentences WHERE uuid = ?",
                (uuid,)
            )
            
            row = cursor.fetchone()
            if not row:
                return None
            
            # Get all document occurrences
            addresses = self.map_uuid_to_addr(uuid)
            
            # Get tokens if they exist
            cursor = conn.execute(
                """
                SELECT t.text, m.position 
                FROM tokens t
                JOIN sentence_token_map m ON t.id = m.token_id
                WHERE m.sentence_uuid = ?
                ORDER BY m.position
                """,
                (uuid,)
            )
            
            tokens = [(row[0], row[1]) for row in cursor.fetchall()]
            
            return {
                "uuid": uuid,
                "text": row[0],
                "hash": row[1],
                "created_at": row[2],
                "addresses": addresses,
                "tokens": tokens,
                "zero_index": 0  # Always 0 for sentences
            }
    
    def get_token_addr(self, sentence_uuid: str, token_position: int) -> str:
        """
        Get the token address in UUID-index format.
        
        Args:
            sentence_uuid: UUID of the sentence
            token_position: Position within the sentence (0-based)
            
        Returns:
            Token address as "{uuid}-{position}"
        """
        return f"{sentence_uuid}-{token_position}"
    
    def resolve_token_addr(self, token_addr: str) -> Optional[Tuple[str, int]]:
        """
        Resolve a token address to sentence UUID and position.
        
        Args:
            token_addr: Token address in the format "uuid-position"
            
        Returns:
            Tuple of (sentence_uuid, position) or None if invalid
        """
        parts = token_addr.split('-')
        if len(parts) < 2:
            return None
        
        # Reconstruct UUID from all parts except the last one
        sentence_uuid = '-'.join(parts[:-1])
        try:
            position = int(parts[-1])
            return sentence_uuid, position
        except ValueError:
            return None
    
    def get_token_info(self, token_addr: str) -> Optional[Dict[str, Any]]:
        """
        Get detailed information about a token by its address.
        
        Args:
            token_addr: Token address in the format "uuid-position"
            
        Returns:
            Dictionary with token information
        """
        resolved = self.resolve_token_addr(token_addr)
        if not resolved:
            return None
        
        sentence_uuid, position = resolved
        
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute(
                """
                SELECT t.id, t.text, t.hash 
                FROM tokens t
                JOIN sentence_token_map m ON t.id = m.token_id
                WHERE m.sentence_uuid = ? AND m.position = ?
                """,
                (sentence_uuid, position)
            )
            
            row = cursor.fetchone()
            if not row:
                return None
            
            # Get sentence information
            cursor = conn.execute(
                "SELECT text FROM sentences WHERE uuid = ?",
                (sentence_uuid,)
            )
            
            sentence_row = cursor.fetchone()
            if not sentence_row:
                return None
            
            return {
                "token_addr": token_addr,
                "token_id": row[0],
                "text": row[1],
                "hash": row[2],
                "position": position,
                "sentence_uuid": sentence_uuid,
                "sentence_text": sentence_row[0]
            }
    
    def get_embedding(self, uuid: str, is_token: bool = False, token_position: Optional[int] = None) -> Optional[bytes]:
        """
        Get embedding for a sentence or token.
        
        Args:
            uuid: Sentence UUID
            is_token: Whether this is a token embedding
            token_position: If is_token is True, the token position
            
        Returns:
            Embedding as bytes or None if not found
        """
        try:
            with h5py.File(self.hdf5_path, 'r') as f:
                if is_token and token_position is not None:
                    token_addr = self.get_token_addr(uuid, token_position)
                    if "tokens" in f and token_addr in f["tokens"]:
                        if "embedding" in f["tokens"][token_addr]:
                            return f["tokens"][token_addr]["embedding"][()]
                else:
                    if "sentences" in f and uuid in f["sentences"]:
                        if "embedding" in f["sentences"][uuid]:
                            return f["sentences"][uuid]["embedding"][()]
                            
            return None
        except Exception as e:
            print(f"Error getting embedding: {e}")
            return None