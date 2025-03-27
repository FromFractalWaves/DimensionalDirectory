# app/core/db_init.py
"""
Database initialization for the Dimensional Directory System.

This module contains functions to initialize and migrate the SQLite database
to the refactored schema, including all tables and indexes needed for the
L-S identifier system, many-to-many sentence mapping, and address point system.
"""

import sqlite3
import os
from typing import Optional


def init_database(db_path: str):
    """
    Initialize the SQLite database with the refactored schema.
    
    Args:
        db_path: Path to the SQLite database file
    """
    # Create directory if it doesn't exist
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    
    with sqlite3.connect(db_path) as conn:
        # LStable for L-S identifier mappings
        conn.execute("""
            CREATE TABLE IF NOT EXISTS lstable (
                dbidL TEXT PRIMARY KEY,
                dbidS TEXT UNIQUE,
                description TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
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
        
        # Address book for hierarchical addressing
        conn.execute("""
            CREATE TABLE IF NOT EXISTS address_book (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                addr TEXT UNIQUE,
                uuid TEXT,
                type TEXT,  -- 'document', 'sentence', 'token'
                parent_addr TEXT,
                coordinate_system TEXT,  -- For control plane functionality
                is_origin INTEGER DEFAULT 0,  -- Flag for origin points in coordinate systems
                FOREIGN KEY (parent_addr) REFERENCES address_book(addr)
            )
        """)
        
        # Address attributes (for underscore-separated attributes)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS address_attributes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                addr TEXT,
                attr_key TEXT,
                attr_value TEXT,
                FOREIGN KEY (addr) REFERENCES address_book(addr),
                UNIQUE(addr, attr_key)
            )
        """)
        
        # Control planes
        conn.execute("""
            CREATE TABLE IF NOT EXISTS control_planes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT UNIQUE,
                origin_addr TEXT,
                dimension_count INTEGER DEFAULT 3,
                description TEXT,
                FOREIGN KEY (origin_addr) REFERENCES address_book(addr)
            )
        """)
        
        # Control plane dimensions
        conn.execute("""
            CREATE TABLE IF NOT EXISTS control_plane_dimensions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                control_plane_id INTEGER,
                dimension_name TEXT,
                dimension_index INTEGER,
                scale REAL DEFAULT 1.0,
                FOREIGN KEY (control_plane_id) REFERENCES control_planes(id),
                UNIQUE(control_plane_id, dimension_index)
            )
        """)
        
        # Create indexes for performance
        conn.execute("CREATE INDEX IF NOT EXISTS idx_doc_sentence_map_doc_uuid ON doc_sentence_map(doc_uuid)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_doc_sentence_map_sentence_uuid ON doc_sentence_map(sentence_uuid)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_address_book_parent_addr ON address_book(parent_addr)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_address_book_type ON address_book(type)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_address_book_uuid ON address_book(uuid)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_sentences_hash ON sentences(hash)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_tokens_hash ON tokens(hash)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_lstable_dbids ON lstable(dbidL, dbidS)")
        
        conn.commit()


def migrate_existing_data(db_path: str, new_db_path: Optional[str] = None):
    """
    Migrate existing data to the new schema.
    
    This function copies data from an existing database to the new schema.
    If new_db_path is not provided, it will create a backup and update the existing database.
    
    Args:
        db_path: Path to the existing SQLite database
        new_db_path: Optional path to new database (if None, updates existing)
    """
    if not os.path.exists(db_path):
        raise FileNotFoundError(f"Database file not found: {db_path}")
    
    # If no new path provided, create a backup and update existing
    if not new_db_path:
        backup_path = f"{db_path}.backup"
        import shutil
        shutil.copy2(db_path, backup_path)
        print(f"Backup created at {backup_path}")
        new_db_path = db_path
    
    # Initialize the new database
    init_database(new_db_path)
    
    # Create connections to both databases
    with sqlite3.connect(db_path) as old_conn, sqlite3.connect(new_db_path) as new_conn:
        # Check if old database has the necessary tables
        old_cursor = old_conn.cursor()
        old_cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = [row[0] for row in old_cursor.fetchall()]
        
        # Migrate documents if table exists
        if 'documents' in tables:
            try:
                # Check if old documents table has dbidL and dbidS columns
                old_cursor.execute("PRAGMA table_info(documents)")
                columns = [row[1] for row in old_cursor.fetchall()]
                
                if 'dbidL' not in columns or 'dbidS' not in columns:
                    # Old schema doesn't have dbidL/dbidS, need to generate them
                    old_cursor.execute("SELECT uuid, title, source, metadata, created_at FROM documents")
                    docs = old_cursor.fetchall()
                    
                    for doc in docs:
                        doc_uuid, title, source, metadata, created_at = doc
                        # Generate dbidL and dbidS
                        dbidL = f"Doc{title[:10]}" if title else f"Doc{doc_uuid[:8]}"
                        dbidS = doc_uuid[:8]
                        
                        # Insert into new database
                        new_conn.execute(
                            """
                            INSERT INTO documents (uuid, dbidL, dbidS, title, source, metadata, created_at)
                            VALUES (?, ?, ?, ?, ?, ?, ?)
                            """,
                            (doc_uuid, dbidL, dbidS, title, source, metadata, created_at)
                        )
                        
                        # Register mapping in lstable
                        new_conn.execute(
                            "INSERT OR IGNORE INTO lstable (dbidL, dbidS) VALUES (?, ?)",
                            (dbidL, dbidS)
                        )
                else:
                    # Old schema already has dbidL/dbidS, direct copy
                    old_cursor.execute(
                        "SELECT uuid, dbidL, dbidS, title, source, metadata, created_at FROM documents"
                    )
                    docs = old_cursor.fetchall()
                    
                    for doc in docs:
                        new_conn.execute(
                            """
                            INSERT INTO documents (uuid, dbidL, dbidS, title, source, metadata, created_at)
                            VALUES (?, ?, ?, ?, ?, ?, ?)
                            """,
                            doc
                        )
                        
                        # Register mapping in lstable
                        new_conn.execute(
                            "INSERT OR IGNORE INTO lstable (dbidL, dbidS) VALUES (?, ?)",
                            (doc[1], doc[2])
                        )
            except sqlite3.Error as e:
                print(f"Error migrating documents: {e}")
        
        # Migrate sentences if table exists
        if 'sentences' in tables:
            try:
                old_cursor.execute("SELECT uuid, text, hash, created_at FROM sentences")
                sentences = old_cursor.fetchall()
                
                for sentence in sentences:
                    # Check if embedding column exists in old schema
                    has_embedding = False
                    try:
                        old_cursor.execute("SELECT embedding FROM sentences WHERE uuid = ?", (sentence[0],))
                        embedding_row = old_cursor.fetchone()
                        has_embedding = embedding_row is not None
                    except sqlite3.Error:
                        pass
                    
                    if has_embedding:
                        old_cursor.execute("SELECT embedding FROM sentences WHERE uuid = ?", (sentence[0],))
                        embedding = old_cursor.fetchone()[0]
                        new_conn.execute(
                            """
                            INSERT INTO sentences (uuid, text, hash, embedding, created_at)
                            VALUES (?, ?, ?, ?, ?)
                            """,
                            (sentence[0], sentence[1], sentence[2], embedding, sentence[3])
                        )
                    else:
                        new_conn.execute(
                            """
                            INSERT INTO sentences (uuid, text, hash, created_at)
                            VALUES (?, ?, ?, ?)
                            """,
                            sentence
                        )
            except sqlite3.Error as e:
                print(f"Error migrating sentences: {e}")
        
        # Migrate document-sentence mappings if table exists
        if 'doc_sentence_map' in tables:
            try:
                old_cursor.execute("SELECT doc_uuid, sentence_uuid, position FROM doc_sentence_map")
                mappings = old_cursor.fetchall()
                
                for mapping in mappings:
                    new_conn.execute(
                        """
                        INSERT INTO doc_sentence_map (doc_uuid, sentence_uuid, position)
                        VALUES (?, ?, ?)
                        """,
                        mapping
                    )
            except sqlite3.Error as e:
                print(f"Error migrating doc_sentence_map: {e}")
        
        # Migrate tokens if table exists
        if 'tokens' in tables:
            try:
                old_cursor.execute("SELECT id, text, hash FROM tokens")
                tokens = old_cursor.fetchall()
                
                for token in tokens:
                    # Check if embedding column exists in old schema
                    has_embedding = False
                    try:
                        old_cursor.execute("SELECT embedding FROM tokens WHERE id = ?", (token[0],))
                        embedding_row = old_cursor.fetchone()
                        has_embedding = embedding_row is not None
                    except sqlite3.Error:
                        pass
                    
                    if has_embedding:
                        old_cursor.execute("SELECT embedding FROM tokens WHERE id = ?", (token[0],))
                        embedding = old_cursor.fetchone()[0]
                        new_conn.execute(
                            """
                            INSERT INTO tokens (id, text, hash, embedding)
                            VALUES (?, ?, ?, ?)
                            """,
                            (token[0], token[1], token[2], embedding)
                        )
                    else:
                        new_conn.execute(
                            """
                            INSERT INTO tokens (id, text, hash)
                            VALUES (?, ?, ?)
                            """,
                            token
                        )
            except sqlite3.Error as e:
                print(f"Error migrating tokens: {e}")
        
        # Migrate token mappings if table exists
        if 'sentence_token_map' in tables:
            try:
                old_cursor.execute("SELECT sentence_uuid, token_id, position FROM sentence_token_map")
                mappings = old_cursor.fetchall()
                
                for mapping in mappings:
                    new_conn.execute(
                        """
                        INSERT INTO sentence_token_map (sentence_uuid, token_id, position)
                        VALUES (?, ?, ?)
                        """,
                        mapping
                    )
            except sqlite3.Error as e:
                print(f"Error migrating sentence_token_map: {e}")
        
        # Migrate address book if table exists
        if 'address_book' in tables:
            try:
                # Check schema of old address_book
                old_cursor.execute("PRAGMA table_info(address_book)")
                columns = [row[1] for row in old_cursor.fetchall()]
                
                if 'coordinate_system' in columns and 'is_origin' in columns:
                    # New schema already exists
                    old_cursor.execute(
                        """
                        SELECT addr, uuid, type, parent_addr, coordinate_system, is_origin
                        FROM address_book
                        """
                    )
                    addresses = old_cursor.fetchall()
                    
                    for addr in addresses:
                        new_conn.execute(
                            """
                            INSERT INTO address_book 
                            (addr, uuid, type, parent_addr, coordinate_system, is_origin)
                            VALUES (?, ?, ?, ?, ?, ?)
                            """,
                            addr
                        )
                else:
                    # Old schema doesn't have coordinate_system and is_origin
                    old_cursor.execute("SELECT addr, uuid, type, parent_addr FROM address_book")
                    addresses = old_cursor.fetchall()
                    
                    for addr in addresses:
                        new_conn.execute(
                            """
                            INSERT INTO address_book 
                            (addr, uuid, type, parent_addr, coordinate_system, is_origin)
                            VALUES (?, ?, ?, ?, NULL, 0)
                            """,
                            addr + (None, 0)
                        )
            except sqlite3.Error as e:
                print(f"Error migrating address_book: {e}")
        
        new_conn.commit()
        print(f"Migration completed to {new_db_path}")


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Initialize or migrate Dimensional Directory database")
    parser.add_argument("--db-path", required=True, help="Path to the SQLite database file")
    parser.add_argument("--new-db-path", help="Path to new database file for migration (optional)")
    parser.add_argument("--migrate", action="store_true", help="Migrate existing data")
    
    args = parser.parse_args()
    
    if args.migrate:
        migrate_existing_data(args.db_path, args.new_db_path)
    else:
        init_database(args.db_path)
        print(f"Database initialized at {args.db_path}")