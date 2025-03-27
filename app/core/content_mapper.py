# app/core/document_mapper.py
import uuid
import re
import hashlib
import sqlite3
import h5py
from typing import Dict, List, Tuple, Set, Optional
from app.core.dd_manager import DimensionalDirectory

class DocumentMapper:
    """
    Handles pre-processing and mapping of documents into the dimensional directory structure.
    Creates addressing schemes and maintains the relationships between documents, sentences, 
    and tokens.
    """
    
    def __init__(self, dd: DimensionalDirectory, db_path: str, hdf5_path: str):
        self.dd = dd
        self.db_path = db_path
        self.hdf5_path = hdf5_path
        self._initialize_db()
        
    def _initialize_db(self):
        """Initialize the database schema for document mapping"""
        with sqlite3.connect(self.db_path) as conn:
            # Document table
            conn.execute("""
                CREATE TABLE IF NOT EXISTS documents (
                    uuid TEXT PRIMARY KEY,
                    title TEXT,
                    source TEXT,
                    metadata TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Sentences table
            conn.execute("""
                CREATE TABLE IF NOT EXISTS sentences (
                    uuid TEXT PRIMARY KEY,
                    text TEXT UNIQUE,
                    hash TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Document-Sentence mapping table (many-to-many)
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
            
            # Address book table
            conn.execute("""
                CREATE TABLE IF NOT EXISTS address_book (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    addr TEXT UNIQUE,
                    uuid TEXT,
                    type TEXT,
                    parent_addr TEXT,
                    FOREIGN KEY (parent_addr) REFERENCES address_book(addr)
                )
            """)
            
            # Token mapping
            conn.execute("""
                CREATE TABLE IF NOT EXISTS token_map (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    token_text TEXT,
                    token_hash TEXT,
                    sentence_uuid TEXT,
                    position INTEGER,
                    addr TEXT,
                    FOREIGN KEY (sentence_uuid) REFERENCES sentences(uuid),
                    FOREIGN KEY (addr) REFERENCES address_book(addr),
                    UNIQUE(sentence_uuid, position)
                )
            """)
            
            conn.commit()
    
    def process_document(self, title: str, content: str, source: Optional[str] = None, 
                         metadata: Optional[Dict] = None) -> str:
        """
        Process a document into the dimensional directory structure
        
        Args:
            title: Document title
            content: Document content
            source: Optional source information
            metadata: Optional metadata dictionary
            
        Returns:
            Document UUID
        """
        # Generate document UUID
        doc_uuid = str(uuid.uuid4())
        
        # Store document metadata
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                "INSERT INTO documents (uuid, title, source, metadata) VALUES (?, ?, ?, ?)",
                (doc_uuid, title, source, str(metadata) if metadata else None)
            )
            conn.commit()
        
        # Process sentences
        sentences = self._split_into_sentences(content)
        
        # Map sentences to document
        for position, sentence_text in enumerate(sentences):
            sentence_uuid = self._get_or_create_sentence(sentence_text)
            self._map_sentence_to_document(sentence_uuid, doc_uuid, position)
            
            # Process tokens within sentence
            tokens = self._tokenize_sentence(sentence_text)
            for token_position, token in enumerate(tokens):
                self._map_token(token, sentence_uuid, token_position)
        
        # Store the full document content in HDF5
        with h5py.File(self.hdf5_path, "a") as f:
            if "documents" not in f:
                f.create_group("documents")
            if doc_uuid not in f["documents"]:
                f["documents"].create_dataset(doc_uuid, data=content.encode('utf-8'))
        
        return doc_uuid
    
    def _split_into_sentences(self, text: str) -> List[str]:
        """Split text into sentences using basic rules"""
        # This is a simplified implementation; consider using a proper NLP library
        sentences = re.split(r'(?<!\w\.\w.)(?<![A-Z][a-z]\.)(?<=\.|\?|\!)\s', text)
        return [s.strip() for s in sentences if s.strip()]
    
    def _get_or_create_sentence(self, sentence_text: str) -> str:
        """Get existing sentence UUID or create a new one"""
        sentence_hash = hashlib.md5(sentence_text.encode()).hexdigest()
        
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute(
                "SELECT uuid FROM sentences WHERE hash = ?",
                (sentence_hash,)
            )
            result = cursor.fetchone()
            
            if result:
                return result[0]
            
            # Create new sentence record
            sentence_uuid = str(uuid.uuid4())
            conn.execute(
                "INSERT INTO sentences (uuid, text, hash) VALUES (?, ?, ?)",
                (sentence_uuid, sentence_text, sentence_hash)
            )
            conn.commit()
            
            return sentence_uuid
    
    def _map_sentence_to_document(self, sentence_uuid: str, doc_uuid: str, position: int):
        """Create mapping between sentence and document"""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                "INSERT OR IGNORE INTO doc_sentence_map (doc_uuid, sentence_uuid, position) VALUES (?, ?, ?)",
                (doc_uuid, sentence_uuid, position)
            )
            conn.commit()
    
    def _tokenize_sentence(self, sentence: str) -> List[str]:
        """Tokenize a sentence into words/tokens"""
        # Simple tokenization by whitespace and punctuation
        # Consider using a proper NLP tokenizer for production
        tokens = re.findall(r'\b\w+\b', sentence.lower())
        return tokens
    
    def _map_token(self, token: str, sentence_uuid: str, position: int):
        """Map a token to the address book and token map"""
        token_hash = hashlib.md5(token.encode()).hexdigest()
        
        # Create address for this token occurrence
        addr = f"s:{sentence_uuid}:t:{position}"
        
        with sqlite3.connect(self.db_path) as conn:
            # Add to address book
            conn.execute(
                "INSERT OR IGNORE INTO address_book (addr, uuid, type, parent_addr) VALUES (?, ?, ?, ?)",
                (addr, sentence_uuid, "token", f"s:{sentence_uuid}")
            )
            
            # Add to token map
            conn.execute(
                "INSERT OR IGNORE INTO token_map (token_text, token_hash, sentence_uuid, position, addr) VALUES (?, ?, ?, ?, ?)",
                (token, token_hash, sentence_uuid, position, addr)
            )
            
            conn.commit()
    
    def get_document_sentences(self, doc_uuid: str) -> List[Dict]:
        """Get all sentences for a document in order"""
        sentences = []
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute(
                """
                SELECT s.uuid, s.text, m.position
                FROM sentences s
                JOIN doc_sentence_map m ON s.uuid = m.sentence_uuid
                WHERE m.doc_uuid = ?
                ORDER BY m.position
                """,
                (doc_uuid,)
            )
            
            for row in cursor.fetchall():
                sentences.append({
                    "uuid": row[0],
                    "text": row[1],
                    "position": row[2]
                })
        
        return sentences
    
    def find_documents_with_sentence(self, sentence_text: str) -> List[Dict]:
        """Find all documents containing a specific sentence"""
        sentence_hash = hashlib.md5(sentence_text.encode()).hexdigest()
        documents = []
        
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute(
                """
                SELECT d.uuid, d.title, d.source, m.position
                FROM documents d
                JOIN doc_sentence_map m ON d.uuid = m.doc_uuid
                JOIN sentences s ON m.sentence_uuid = s.uuid
                WHERE s.hash = ?
                ORDER BY d.title, m.position
                """,
                (sentence_hash,)
            )
            
            for row in cursor.fetchall():
                documents.append({
                    "uuid": row[0],
                    "title": row[1],
                    "source": row[2],
                    "position": row[3]
                })
        
        return documents
    
    def search_by_token(self, token: str) -> List[Dict]:
        """Search for sentences and documents containing a specific token"""
        results = []
        
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute(
                """
                SELECT t.token_text, t.addr, s.uuid, s.text, d.uuid, d.title
                FROM token_map t
                JOIN sentences s ON t.sentence_uuid = s.uuid
                JOIN doc_sentence_map m ON s.uuid = m.sentence_uuid
                JOIN documents d ON m.doc_uuid = d.uuid
                WHERE t.token_text = ?
                ORDER BY d.title, m.position
                """,
                (token.lower(),)
            )
            
            for row in cursor.fetchall():
                results.append({
                    "token": row[0],
                    "addr": row[1],
                    "sentence_uuid": row[2],
                    "sentence_text": row[3],
                    "doc_uuid": row[4],
                    "doc_title": row[5]
                })
        
        return results