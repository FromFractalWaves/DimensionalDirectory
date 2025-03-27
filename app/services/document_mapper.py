import sqlite3
import hashlib
import uuid
import h5py
import re
import os
import numpy as np
from typing import Dict, List, Optional, Tuple, Any

class DocumentMapper:
    """
    Handles mapping between documents, sentences, and tokens in the Dimensional Directory system.
    Implements many-to-many sentence mapping and deduplication as shown in the diagrams.
    """
    
    def __init__(self, db_path: str, hdf5_path: str, address_manager=None, lstable_manager=None):
        """
        Initialize the Document Mapper.
        
        Args:
            db_path: Path to the SQLite database
            hdf5_path: Path to the HDF5 file for storing embeddings and content
            address_manager: Optional AddressManager instance
            lstable_manager: Optional LStableManager instance
        """
        self.db_path = db_path
        self.hdf5_path = hdf5_path
        self.address_manager = address_manager
        self.lstable_manager = lstable_manager
        
        # Create directory if it doesn't exist
        os.makedirs(os.path.dirname(hdf5_path), exist_ok=True)
        
        self._init_database()
    
    def _init_database(self):
        """Initialize the database tables for document mapping"""
        with sqlite3.connect(self.db_path) as conn:
            # Documents table
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
                         metadata: Optional[Dict] = None) -> Dict:
        """
        Process a document and map its sentences.
        
        Args:
            content: Document content
            dbidL: Long identifier for the document
            dbidS: Optional short identifier (will be generated if not provided)
            title: Optional document title
            source: Optional document source
            metadata: Optional metadata dictionary
            
        Returns:
            Dictionary with document information
        """
        # Register L-S mapping if lstable_manager is available
        if self.lstable_manager:
            dbidL, dbidS = self.lstable_manager.register_mapping(dbidL, dbidS)
        elif not dbidS:
            # Generate a unique short ID if not provided
            dbidS = str(uuid.uuid4().hex[:8])
        
        # Generate document UUID
        doc_uuid = str(uuid.uuid4())
        
        # Store document in database
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                "INSERT INTO documents (uuid, dbidL, dbidS, title, source, metadata) VALUES (?, ?, ?, ?, ?, ?)",
                (doc_uuid, dbidL, dbidS, title, source, str(metadata) if metadata else None)
            )
            conn.commit()
        
        # Process sentences
        sentences = self._split_into_sentences(content)
        sentence_mappings = []
        
        for position, sentence_text in enumerate(sentences):
            # Get or create sentence
            sentence_uuid, is_new = self._get_or_create_sentence(sentence_text)
            
            # Map sentence to document
            self._map_sentence_to_document(sentence_uuid, doc_uuid, position)
            
            # Create address if address_manager is available
            if self.address_manager:
                sentence_addr = self.address_manager.create_address(
                    [f"doc:{doc_uuid}", f"s:{sentence_uuid}"],
                    None,
                    sentence_uuid,
                    "sentence"
                )
            
            # Process tokens if this is a new sentence
            if is_new:
                tokens = self._tokenize_sentence(sentence_text)
                token_mappings = []
                
                for token_position, token_text in enumerate(tokens):
                    token_id = self._get_or_create_token(token_text)
                    self._map_token_to_sentence(token_id, sentence_uuid, token_position)
                    
                    # Create address for token if address_manager is available
                    if self.address_manager:
                        token_addr = self.address_manager.create_address(
                            [f"doc:{doc_uuid}", f"s:{sentence_uuid}", f"t:{token_position}"],
                            None,
                            None,
                            "token"
                        )
                    
                    token_mappings.append({
                        "position": token_position,
                        "text": token_text,
                        "id": token_id
                    })
            
            sentence_mappings.append({
                "uuid": sentence_uuid,
                "text": sentence_text,
                "position": position,
                "is_new": is_new
            })
        
        # Store document content in HDF5
        with h5py.File(self.hdf5_path, "a") as f:
            if "documents" not in f:
                f.create_group("documents")
            
            if doc_uuid not in f["documents"]:
                doc_group = f["documents"].create_group(doc_uuid)
                doc_group.attrs["dbidL"] = dbidL
                doc_group.attrs["dbidS"] = dbidS
                doc_group.attrs["title"] = title if title else ""
                doc_group.attrs["source"] = source if source else ""
                doc_group.create_dataset("content", data=content.encode('utf-8'))
                
                # Store sentence references
                sentence_refs = doc_group.create_group("sentences")
                for mapping in sentence_mappings:
                    sentence_refs.attrs[str(mapping["position"])] = mapping["uuid"]
        
        return {
            "uuid": doc_uuid,
            "dbidL": dbidL,
            "dbidS": dbidS,
            "title": title,
            "source": source,
            "sentence_count": len(sentences),
            "sentences": sentence_mappings
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
        # Could be replaced with more sophisticated NLP techniques
        sentences = re.split(r'(?<!\w\.\w.)(?<![A-Z][a-z]\.)(?<=\.|\?|\!)\s', text)
        return [s.strip() for s in sentences if s.strip()]
    
    def _get_or_create_sentence(self, sentence_text: str) -> Tuple[str, bool]:
        """
        Get existing sentence UUID or create a new one.
        
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
                # Sentence already exists
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
    
    def _tokenize_sentence(self, sentence: str) -> List[str]:
        """
        Tokenize a sentence into words/tokens.
        
        Args:
            sentence: Input sentence
            
        Returns:
            List of tokens
        """
        # Simple tokenization by whitespace and punctuation
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
                    dbidS: Optional[str] = None) -> Optional[Dict]:
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
                "metadata": result[5],
                "created_at": result[6]
            }
            
            # Get sentences
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
            
            doc_info["sentences"] = [
                {
                    "uuid": row[0],
                    "text": row[1],
                    "hash": row[2],
                    "position": row[3]
                }
                for row in cursor.fetchall()
            ]
            
            # Get document content from HDF5
            try:
                with h5py.File(self.hdf5_path, "r") as f:
                    if "documents" in f and doc_info["uuid"] in f["documents"]:
                        doc_group = f["documents"][doc_info["uuid"]]
                        if "content" in doc_group:
                            content = doc_group["content"][()]
                            doc_info["content"] = content.decode('utf-8')
            except Exception as e:
                # If HDF5 file doesn't exist or other error, continue without content
                pass
            
            return doc_info
    
    def get_sentence(self, sentence_uuid: str) -> Optional[Dict]:
        """
        Get sentence information.
        
        Args:
            sentence_uuid: Sentence UUID
            
        Returns:
            Dictionary with sentence information or None if not found
        """
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute(
                "SELECT uuid, text, hash, created_at FROM sentences WHERE uuid = ?",
                (sentence_uuid,)
            )
            
            result = cursor.fetchone()
            if not result:
                return None
            
            sentence_info = {
                "uuid": result[0],
                "text": result[1],
                "hash": result[2],
                "created_at": result[3]
            }
            
            # Get tokens
            cursor = conn.execute(
                """
                SELECT t.id, t.text, t.hash, m.position 
                FROM tokens t 
                JOIN sentence_token_map m ON t.id = m.token_id 
                WHERE m.sentence_uuid = ? 
                ORDER BY m.position
                """,
                (sentence_uuid,)
            )
            
            sentence_info["tokens"] = [
                {
                    "id": row[0],
                    "text": row[1],
                    "hash": row[2],
                    "position": row[3]
                }
                for row in cursor.fetchall()
            ]
            
            # Get documents containing this sentence
            cursor = conn.execute(
                """
                SELECT d.uuid, d.dbidL, d.dbidS, d.title, m.position 
                FROM documents d 
                JOIN doc_sentence_map m ON d.uuid = m.doc_uuid 
                WHERE m.sentence_uuid = ? 
                ORDER BY d.created_at, m.position
                """,
                (sentence_uuid,)
            )
            
            sentence_info["documents"] = [
                {
                    "uuid": row[0],
                    "dbidL": row[1],
                    "dbidS": row[2],
                    "title": row[3],
                    "position": row[4]
                }
                for row in cursor.fetchall()
            ]
            
            # Get sentence from HDF5
            try:
                with h5py.File(self.hdf5_path, "r") as f:
                    if "sentences" in f and sentence_uuid in f["sentences"]:
                        sentence_group = f["sentences"][sentence_uuid]
                        if "embedding" in sentence_group:
                            embedding = sentence_group["embedding"][()]
                            sentence_info["embedding"] = embedding
            except Exception as e:
                # If HDF5 file doesn't exist or other error, continue without embedding
                pass
            
            return sentence_info
    
    def find_documents_with_sentence(self, sentence_text: str) -> List[Dict]:
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
                SELECT d.uuid, d.dbidL, d.dbidS, d.title, d.source, m.position 
                FROM documents d 
                JOIN doc_sentence_map m ON d.uuid = m.doc_uuid 
                JOIN sentences s ON m.sentence_uuid = s.uuid 
                WHERE s.hash = ? 
                ORDER BY d.created_at, m.position
                """,
                (sentence_hash,)
            )
            
            return [
                {
                    "uuid": row[0],
                    "dbidL": row[1],
                    "dbidS": row[2],
                    "title": row[3],
                    "source": row[4],
                    "position": row[5]
                }
                for row in cursor.fetchall()
            ]
    
    def search_by_token(self, token_text: str) -> List[Dict]:
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
                SELECT t.id, t.text, s.uuid, s.text, d.uuid, d.title, d.dbidL, d.dbidS, m1.position AS token_pos, m2.position AS sent_pos
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
                
                # Create address if address_manager is available
                token_addr = None
                if self.address_manager:
                    token_addr = self.address_manager.create_address(
                        [f"doc:{doc_uuid}", f"s:{sent_uuid}", f"t:{token_pos}"],
                        None,
                        None,
                        "token"
                    )
                
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
                        "position": sent_pos
                    },
                    "document": {
                        "uuid": doc_uuid,
                        "title": doc_title,
                        "dbidL": dbidL,
                        "dbidS": dbidS
                    }
                })
            
            return results
    
    def add_embedding(self, uuid_value: str, embedding: np.ndarray, entity_type: str = "sentence"):
        """
        Add embedding for a sentence or token.
        
        Args:
            uuid_value: UUID of the entity
            embedding: Numpy array containing the embedding
            entity_type: Type of entity ("sentence" or "token")
        """
        # Store embedding in database
        with sqlite3.connect(self.db_path) as conn:
            if entity_type == "sentence":
                conn.execute(
                    "UPDATE sentences SET embedding = ? WHERE uuid = ?",
                    (embedding.tobytes(), uuid_value)
                )
            elif entity_type == "token":
                conn.execute(
                    "UPDATE tokens SET embedding = ? WHERE id = ?",
                    (embedding.tobytes(), uuid_value)
                )
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
            elif entity_type == "token":
                if "tokens" not in f:
                    f.create_group("tokens")
                
                if uuid_value not in f["tokens"]:
                    f["tokens"].create_group(uuid_value)
                
                token_group = f["tokens"][uuid_value]
                if "embedding" in token_group:
                    del token_group["embedding"]
                
                token_group.create_dataset("embedding", data=embedding)
    
    def get_all_sentences(self) -> List[Dict]:
        """
        Get all sentences in the system.
        
        Returns:
            List of sentence information
        """
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute(
                "SELECT uuid, text, hash, created_at FROM sentences ORDER BY created_at"
            )
            
            return [
                {
                    "uuid": row[0],
                    "text": row[1],
                    "hash": row[2],
                    "created_at": row[3]
                }
                for row in cursor.fetchall()
            ]
    
    def get_all_documents(self) -> List[Dict]:
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
            
            return [
                {
                    "uuid": row[0],
                    "dbidL": row[1],
                    "dbidS": row[2],
                    "title": row[3],
                    "source": row[4],
                    "created_at": row[5],
                    "sentence_count": row[6]
                }
                for row in cursor.fetchall()
            ]