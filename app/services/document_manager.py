import os
import sqlite3
import uuid
import hashlib
import json
import h5py
import re
import numpy as np
from typing import Dict, List, Optional, Tuple, Any, Union
from app.services.address_manager import AddressManager
from app.core.addressing.zero_index_mapper import ZeroIndexMapper

class DocumentManager:
    """
    Enhanced DocumentManager for the Dimensional Directory system.
    
    This class handles document processing, sentence mapping, and token management
    using the zero-indexed UUID approach for many-to-many mapping.
    """
    
    def __init__(self, db_path: str, hdf5_path: str, address_manager: Optional[AddressManager] = None):
        """
        Initialize the Document Manager.
        
        Args:
            db_path: Path to the SQLite database
            hdf5_path: Path to the HDF5 storage
            address_manager: Optional AddressManager instance
        """
        self.db_path = db_path
        self.hdf5_path = hdf5_path
        
        # Create directories if they don't exist
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        os.makedirs(os.path.dirname(hdf5_path), exist_ok=True)
        
        self.address_manager = address_manager or AddressManager(db_path, hdf5_path)
        self.zero_index_mapper = ZeroIndexMapper(db_path, hdf5_path)
        self._init_database()
    
    def _init_database(self):
        """Initialize the database tables for document management"""
        with sqlite3.connect(self.db_path) as conn:
            # Documents table with L-S identifiers
            conn.execute("""
                CREATE TABLE IF NOT EXISTS documents (
                    uuid TEXT PRIMARY KEY,
                    dbidL TEXT NOT NULL,
                    dbidS TEXT NOT NULL,
                    title TEXT,
                    source TEXT,
                    metadata TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(dbidL, dbidS)
                )
            """)
            
            # Sentences table (central store for deduplication)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS sentences (
                    uuid TEXT PRIMARY KEY,
                    text TEXT UNIQUE,
                    hash TEXT UNIQUE,
                    embedding BLOB,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Document-Sentence mapping (many-to-many relationship)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS doc_sentence_map (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    doc_uuid TEXT,
                    sentence_uuid TEXT,
                    position INTEGER,
                    FOREIGN KEY (doc_uuid) REFERENCES documents(uuid),
                    FOREIGN KEY (sentence_uuid) REFERENCES sentences(uuid),
                    UNIQUE(doc_uuid, sentence_uuid, position)
                )
            """)
            
            # Token table (for individual tokens within sentences)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS tokens (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    text TEXT NOT NULL,
                    hash TEXT NOT NULL,
                    embedding BLOB,
                    UNIQUE(hash)
                )
            """)
            
            # Token mapping to sentences
            conn.execute("""
                CREATE TABLE IF NOT EXISTS sentence_token_map (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    sentence_uuid TEXT,
                    token_id INTEGER,
                    position INTEGER,
                    FOREIGN KEY (sentence_uuid) REFERENCES sentences(uuid),
                    FOREIGN KEY (token_id) REFERENCES tokens(id),
                    UNIQUE(sentence_uuid, position)
                )
            """)
            
            # Create indexes for performance
            conn.execute("CREATE INDEX IF NOT EXISTS idx_doc_sentence_map_doc_uuid ON doc_sentence_map(doc_uuid)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_doc_sentence_map_sentence_uuid ON doc_sentence_map(sentence_uuid)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_sentences_hash ON sentences(hash)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_tokens_hash ON tokens(hash)")
            
            conn.commit()
    
    def process_document(self, content: str, dbidL: str, dbidS: Optional[str] = None,
                         title: Optional[str] = None, source: Optional[str] = None,
                         metadata: Optional[Dict] = None) -> Dict[str, Any]:
        """
        Process a document and map its sentences with UUID deduplication.
        
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
        # Generate UUID and handle dbidS if not provided
        doc_uuid = str(uuid.uuid4())
        if not dbidS:
            dbidS = doc_uuid[:8]  # Use first 8 chars of UUID as dbidS
        
        # Store document metadata
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """
                INSERT INTO documents 
                (uuid, dbidL, dbidS, title, source, metadata) 
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (doc_uuid, dbidL, dbidS, title, source, 
                 json.dumps(metadata) if metadata else None)
            )
            conn.commit()
        
        # Process document content into sentences
        sentences = self._split_into_sentences(content)
        processed_sentences = []
        
        for position, sentence_text in enumerate(sentences):
            # Get or create sentence (returns existing UUID if duplicate)
            sentence_uuid, is_new = self._get_or_create_sentence(sentence_text)
            
            # Map the sentence to this document at the current position
            self._map_sentence_to_document(sentence_uuid, doc_uuid, position)
            
            # Create address mapping
            addr = f"doc:{doc_uuid}-{position}"
            self.address_manager.create_address(addr, sentence_uuid, "sentence", 0)
            
            # Process tokens for new sentences
            tokens = []
            if is_new:
                tokens = self._process_sentence_tokens(sentence_text, sentence_uuid)
            
            processed_sentences.append({
                "uuid": sentence_uuid,
                "text": sentence_text,
                "position": position,
                "address": addr,
                "is_new": is_new,
                "tokens": tokens
            })
        
        # Store document content in HDF5
        with h5py.File(self.hdf5_path, "a") as f:
            if "documents" not in f:
                f.create_group("documents")
            
            if doc_uuid not in f["documents"]:
                doc_group = f["documents"].create_group(doc_uuid)
                doc_group.attrs["dbidL"] = dbidL
                doc_group.attrs["dbidS"] = dbidS
                doc_group.attrs["title"] = title or ""
                doc_group.attrs["source"] = source or ""
                doc_group.create_dataset("content", data=content.encode('utf-8'))
                
                # Store sentence references
                sentence_refs = doc_group.create_group("sentences")
                for sentence in processed_sentences:
                    sentence_refs.attrs[str(sentence["position"])] = sentence["uuid"]
        
        return {
            "uuid": doc_uuid,
            "dbidL": dbidL,
            "dbidS": dbidS,
            "title": title,
            "source": source,
            "sentence_count": len(processed_sentences),
            "sentences": processed_sentences
        }
    
    def _split_into_sentences(self, text: str) -> List[str]:
        """
        Split text into sentences using basic rules.
        
        Args:
            text: Input text
            
        Returns:
            List of sentences
        """
        # Simple sentence splitting logic
        # For production, consider using a proper NLP library
        sentences = re.split(r'(?<!\w\.\w.)(?<![A-Z][a-z]\.)(?<=\.|\?|\!)\s', text)
        return [s.strip() for s in sentences if s.strip()]
    
    def _get_or_create_sentence(self, sentence_text: str) -> Tuple[str, bool]:
        """
        Get existing sentence UUID or create a new one.
        
        This is a key component for sentence deduplication - if the sentence
        already exists, we return its UUID rather than creating a duplicate.
        
        Args:
            sentence_text: Sentence text
            
        Returns:
            Tuple of (sentence_uuid, is_new)
        """
        # Calculate hash for sentence
        sentence_hash = hashlib.sha256(sentence_text.encode('utf-8')).hexdigest()
        
        with sqlite3.connect(self.db_path) as conn:
            # Check if sentence already exists
            cursor = conn.execute(
                "SELECT uuid FROM sentences WHERE hash = ?",
                (sentence_hash,)
            )
            
            result = cursor.fetchone()
            if result:
                # Sentence already exists - return existing UUID
                return result[0], False
            
            # Create new sentence
            sentence_uuid = str(uuid.uuid4())
            conn.execute(
                "INSERT INTO sentences (uuid, text, hash) VALUES (?, ?, ?)",
                (sentence_uuid, sentence_text, sentence_hash)
            )
            conn.commit()
            
            # Store sentence in HDF5
            with h5py.File(self.hdf5_path, "a") as f:
                if "sentences" not in f:
                    f.create_group("sentences")
                
                if sentence_uuid not in f["sentences"]:
                    sentence_group = f["sentences"].create_group(sentence_uuid)
                    sentence_group.attrs["hash"] = sentence_hash
                    sentence_group.create_dataset("text", data=sentence_text.encode('utf-8'))
            
            return sentence_uuid, True
    
    def _map_sentence_to_document(self, sentence_uuid: str, doc_uuid: str, position: int):
        """
        Create mapping between sentence and document.
        
        Args:
            sentence_uuid: Sentence UUID
            doc_uuid: Document UUID
            position: Position of sentence in document
        """
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """
                INSERT OR IGNORE INTO doc_sentence_map (doc_uuid, sentence_uuid, position) 
                VALUES (?, ?, ?)
                """,
                (doc_uuid, sentence_uuid, position)
            )
            conn.commit()
    
    def _process_sentence_tokens(self, sentence_text: str, sentence_uuid: str) -> List[Dict[str, Any]]:
        """
        Process and store tokens for a sentence.
        
        Args:
            sentence_text: Sentence text
            sentence_uuid: Sentence UUID
            
        Returns:
            List of token information dictionaries
        """
        tokens = self._tokenize_sentence(sentence_text)
        token_info = []
        
        for position, token_text in enumerate(tokens):
            # Get or create token
            token_id = self._get_or_create_token(token_text)
            
            # Map token to sentence
            self._map_token_to_sentence(token_id, sentence_uuid, position)
            
            # Create token address in the format "{sentence_uuid}-{position}"
            token_addr = self.zero_index_mapper.get_token_addr(sentence_uuid, position)
            
            # Create address entry
            self.address_manager.create_address(
                f"{sentence_uuid}.{position}", 
                token_addr, 
                "token", 
                position
            )
            
            token_info.append({
                "id": token_id,
                "text": token_text,
                "position": position,
                "address": token_addr
            })
            
            # Store token in HDF5
            with h5py.File(self.hdf5_path, "a") as f:
                if "tokens" not in f:
                    f.create_group("tokens")
                
                if token_addr not in f["tokens"]:
                    token_group = f["tokens"].create_group(token_addr)
                    token_group.attrs["sentence_uuid"] = sentence_uuid
                    token_group.attrs["position"] = position
                    token_group.create_dataset("text", data=token_text.encode('utf-8'))
        
        return token_info
    
    def _tokenize_sentence(self, sentence: str) -> List[str]:
        """
        Tokenize a sentence into words/tokens.
        
        Args:
            sentence: Input sentence
            
        Returns:
            List of tokens
        """
        # Simple tokenization by whitespace and punctuation
        # For production, consider using a proper NLP tokenizer
        tokens = re.findall(r'\b\w+\b', sentence.lower())
        return tokens
    
    def _get_or_create_token(self, token_text: str) -> int:
        """
        Get existing token ID or create a new one.
        
        Args:
            token_text: Token text
            
        Returns:
            Token ID
        """
        # Calculate hash for token
        token_hash = hashlib.sha256(token_text.encode('utf-8')).hexdigest()
        
        with sqlite3.connect(self.db_path) as conn:
            # Check if token already exists
            cursor = conn.execute(
                "SELECT id FROM tokens WHERE hash = ?",
                (token_hash,)
            )
            
            result = cursor.fetchone()
            if result:
                # Token already exists
                return result[0]
            
            # Create new token
            conn.execute(
                "INSERT INTO tokens (text, hash) VALUES (?, ?)",
                (token_text, token_hash)
            )
            conn.commit()
            
            # Get the ID of the inserted token
            cursor = conn.execute(
                "SELECT id FROM tokens WHERE hash = ?",
                (token_hash,)
            )
            
            return cursor.fetchone()[0]
    
    def _map_token_to_sentence(self, token_id: int, sentence_uuid: str, position: int):
        """
        Create mapping between token and sentence.
        
        Args:
            token_id: Token ID
            sentence_uuid: Sentence UUID
            position: Position of token in sentence
        """
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """
                INSERT OR IGNORE INTO sentence_token_map (sentence_uuid, token_id, position) 
                VALUES (?, ?, ?)
                """,
                (sentence_uuid, token_id, position)
            )
            conn.commit()
    
    def get_document(self, doc_uuid: Optional[str] = None, 
                     dbidL: Optional[str] = None, 
                     dbidS: Optional[str] = None) -> Optional[Dict[str, Any]]:
        """
        Get document information.
        
        Args:
            doc_uuid: Optional document UUID
            dbidL: Optional long identifier
            dbidS: Optional short identifier
            
        Returns:
            Dictionary with document information or None if not found
        """
        if not doc_uuid and (not dbidL or not dbidS):
            raise ValueError("Either doc_uuid or both dbidL and dbidS must be provided")
        
        with sqlite3.connect(self.db_path) as conn:
            if doc_uuid:
                cursor = conn.execute(
                    """
                    SELECT uuid, dbidL, dbidS, title, source, metadata, created_at 
                    FROM documents WHERE uuid = ?
                    """,
                    (doc_uuid,)
                )
            else:
                cursor = conn.execute(
                    """
                    SELECT uuid, dbidL, dbidS, title, source, metadata, created_at 
                    FROM documents WHERE dbidL = ? AND dbidS = ?
                    """,
                    (dbidL, dbidS)
                )
            
            result = cursor.fetchone()
            if not result:
                return None
            
            doc_info = {
                "uuid": result[0],
                "dbidL": result[1],
                "dbidS": result[2],
                "title": result[3],
                "source": result[4],
                "metadata": json.loads(result[5]) if result[5] else None,
                "created_at": result[6]
            }
            
            # Get sentences with zero-indexed UUIDs
            cursor = conn.execute(
                """
                SELECT s.uuid, s.text, s.hash, m.position 
                FROM sentences s 
                JOIN doc_sentence_map m ON s.uuid = m.sentence_uuid 
                WHERE m.doc_uuid = ? 
                ORDER BY m.position
                """,
                (doc_info["uuid"],)
            )
            
            sentences = []
            for row in cursor.fetchall():
                sent_uuid, sent_text, sent_hash, position = row
                
                # Create the address
                addr = f"doc:{doc_info['uuid']}-{position}"
                
                sentences.append({
                    "uuid": sent_uuid,
                    "text": sent_text,
                    "hash": sent_hash,
                    "position": position,
                    "address": addr,
                    "zero_index": 0  # Always 0 for sentences
                })
            
            return results
    
    def get_sentence(self, sentence_uuid: str) -> Optional[Dict[str, Any]]:
        """
        Get detailed information about a sentence by UUID.
        
        Args:
            sentence_uuid: Sentence UUID
            
        Returns:
            Dictionary with sentence information or None if not found
        """
        return self.zero_index_mapper.get_sentence_info(sentence_uuid)
    
    def get_all_sentences(self) -> List[Dict[str, Any]]:
        """
        Get all sentences in the system.
        
        Returns:
            List of sentence information
        """
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute(
                "SELECT uuid, text, hash, created_at FROM sentences ORDER BY created_at"
            )
            
            sentences = []
            for row in cursor.fetchall():
                uuid_val, text, hash_val, created_at = row
                
                # For each sentence, get its occurrences in documents
                occurrences = self.zero_index_mapper.map_uuid_to_addr(uuid_val)
                
                sentences.append({
                    "uuid": uuid_val,
                    "text": text,
                    "hash": hash_val,
                    "created_at": created_at,
                    "occurrences": occurrences,
                    "zero_index": 0  # Always 0 for sentences
                })
            
            return sentences
    
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
        try:
            # Store embedding in database
            with sqlite3.connect(self.db_path) as conn:
                if entity_type == "sentence":
                    conn.execute(
                        "UPDATE sentences SET embedding = ? WHERE uuid = ?",
                        (embedding.tobytes(), uuid_value)
                    )
                elif entity_type == "token" and token_position is not None:
                    # For tokens, we need to get the token ID first
                    cursor = conn.execute(
                        """
                        SELECT t.id FROM tokens t
                        JOIN sentence_token_map m ON t.id = m.token_id
                        WHERE m.sentence_uuid = ? AND m.position = ?
                        """,
                        (uuid_value, token_position)
                    )
                    token_result = cursor.fetchone()
                    if token_result:
                        conn.execute(
                            "UPDATE tokens SET embedding = ? WHERE id = ?",
                            (embedding.tobytes(), token_result[0])
                        )
                    else:
                        return False
                else:
                    return False
                
                conn.commit()
            
            # Store embedding in HDF5
            with h5py.File(self.hdf5_path, "a") as f:
                if entity_type == "sentence":
                    if "sentences" not in f:
                        f.create_group("sentences")
                    
                    if uuid_value not in f["sentences"]:
                        f["sentences"].create_group(uuid_value)
                    
                    sentence_group = f["sentences"][uuid_value]
                    if "embedding" in sentence_group:
                        del sentence_group["embedding"]
                    
                    sentence_group.create_dataset("embedding", data=embedding)
                elif entity_type == "token" and token_position is not None:
                    if "tokens" not in f:
                        f.create_group("tokens")
                    
                    token_addr = self.zero_index_mapper.get_token_addr(uuid_value, token_position)
                    if token_addr not in f["tokens"]:
                        f["tokens"].create_group(token_addr)
                    
                    token_group = f["tokens"][token_addr]
                    if "embedding" in token_group:
                        del token_group["embedding"]
                    
                    token_group.create_dataset("embedding", data=embedding)
                else:
                    return False
            
            return True
        except Exception as e:
            print(f"Error adding embedding: {e}")
            return False
    
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
        try:
            # First check HDF5 (faster access for large embeddings)
            with h5py.File(self.hdf5_path, "r") as f:
                if entity_type == "sentence":
                    if "sentences" in f and uuid_value in f["sentences"]:
                        if "embedding" in f["sentences"][uuid_value]:
                            return f["sentences"][uuid_value]["embedding"][()]
                elif entity_type == "token" and token_position is not None:
                    token_addr = self.zero_index_mapper.get_token_addr(uuid_value, token_position)
                    if "tokens" in f and token_addr in f["tokens"]:
                        if "embedding" in f["tokens"][token_addr]:
                            return f["tokens"][token_addr]["embedding"][()]
            
            # If not found in HDF5, check SQLite
            with sqlite3.connect(self.db_path) as conn:
                if entity_type == "sentence":
                    cursor = conn.execute(
                        "SELECT embedding FROM sentences WHERE uuid = ?",
                        (uuid_value,)
                    )
                    result = cursor.fetchone()
                    if result and result[0]:
                        return np.frombuffer(result[0])
                elif entity_type == "token" and token_position is not None:
                    cursor = conn.execute(
                        """
                        SELECT t.embedding FROM tokens t
                        JOIN sentence_token_map m ON t.id = m.token_id
                        WHERE m.sentence_uuid = ? AND m.position = ?
                        """,
                        (uuid_value, token_position)
                    )
                    result = cursor.fetchone()
                    if result and result[0]:
                        return np.frombuffer(result[0])
            
            return None
        except Exception as e:
            print(f"Error getting embedding: {e}")
            return None
            
            doc_info["sentences"] = sentences
            
            # Get document content from HDF5
            try:
                with h5py.File(self.hdf5_path, "r") as f:
                    if "documents" in f and doc_info["uuid"] in f["documents"]:
                        doc_group = f["documents"][doc_info["uuid"]]
                        if "content" in doc_group:
                            content = doc_group["content"][()]
                            doc_info["content"] = content.decode('utf-8')
            except Exception as e:
                # Continue without content if HDF5 access fails
                print(f"Error accessing HDF5: {e}")
            
            return doc_info
    
    def get_all_documents(self) -> List[Dict[str, Any]]:
        """
        Get all documents in the system.
        
        Returns:
            List of document information
        """
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute(
                """
                SELECT uuid, dbidL, dbidS, title, source, created_at,
                (SELECT COUNT(*) FROM doc_sentence_map WHERE doc_uuid = documents.uuid) AS sentence_count
                FROM documents ORDER BY created_at
                """
            )
            
            documents = []
            for row in cursor.fetchall():
                documents.append({
                    "uuid": row[0],
                    "dbidL": row[1],
                    "dbidS": row[2],
                    "title": row[3],
                    "source": row[4],
                    "created_at": row[5],
                    "sentence_count": row[6]
                })
            
            return documents
    
    def find_documents_with_sentence(self, sentence_text: str) -> List[Dict[str, Any]]:
        """
        Find all documents containing a specific sentence.
        
        Args:
            sentence_text: Sentence text to search for
            
        Returns:
            List of documents containing the sentence
        """
        # Calculate hash for sentence
        sentence_hash = hashlib.sha256(sentence_text.encode('utf-8')).hexdigest()
        
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute(
                """
                SELECT d.uuid, d.dbidL, d.dbidS, d.title, d.source, m.position, s.uuid
                FROM documents d 
                JOIN doc_sentence_map m ON d.uuid = m.doc_uuid 
                JOIN sentences s ON m.sentence_uuid = s.uuid 
                WHERE s.hash = ? 
                ORDER BY d.created_at, m.position
                """,
                (sentence_hash,)
            )
            
            results = []
            for row in cursor.fetchall():
                doc_uuid, dbidL, dbidS, title, source, position, sent_uuid = row
                
                # Create address
                addr = f"doc:{doc_uuid}-{position}"
                
                results.append({
                    "uuid": doc_uuid,
                    "dbidL": dbidL,
                    "dbidS": dbidS,
                    "title": title,
                    "source": source,
                    "position": position,
                    "sentence_uuid": sent_uuid,
                    "address": addr
                })
            
            return results
    
    def search_by_token(self, token_text: str) -> List[Dict[str, Any]]:
        """
        Search for sentences and documents containing a specific token.
        
        Args:
            token_text: Token text to search for
            
        Returns:
            List of matches with sentence and document information
        """
        token_text = token_text.lower()
        
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute(
                """
                SELECT t.id, t.text, s.uuid, s.text, d.uuid, d.title, d.dbidL, d.dbidS, 
                       m1.position AS token_pos, m2.position AS sent_pos
                FROM tokens t
                JOIN sentence_token_map m1 ON t.id = m1.token_id
                JOIN sentences s ON m1.sentence_uuid = s.uuid
                JOIN doc_sentence_map m2 ON s.uuid = m2.sentence_uuid
                JOIN documents d ON m2.doc_uuid = d.uuid
                WHERE t.text = ?
                ORDER BY d.created_at, m2.position, m1.position
                """,
                (token_text,)
            )
            
            results = []
            for row in cursor.fetchall():
                token_id, token_text, sent_uuid, sent_text, doc_uuid, doc_title, dbidL, dbidS, token_pos, sent_pos = row
                
                # Create addresses
                sent_addr = f"doc:{doc_uuid}-{sent_pos}"
                token_addr = f"{sent_uuid}-{token_pos}"
                
                results.append({
                    "token": {
                        "id": token_id,
                        "text": token_text,
                        "position": token_pos,
                        "address": token_addr
                    },
                    "sentence": {
                        "uuid": sent_uuid,
                        "text": sent_text,
                        "position": sent_pos,
                        "address": sent_addr
                    },
                    "document": {
                        "uuid": doc_uuid,
                        "title": doc_title,
                        "dbidL": dbidL,
                        "dbidS": dbidS
                    }
                })