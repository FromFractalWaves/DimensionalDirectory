import os
import sqlite3
import re
from typing import Dict, List, Optional, Tuple, Union, Any
from app.core.addressing.zero_index_mapper import ZeroIndexMapper

class AddressManager:
    """
    Enhanced AddressManager for the refactored Dimensional Directory system.
    
    This class manages the hierarchical addressing system with zero-indexed UUID mapping,
    supporting the many-to-many relationships between documents and sentences.
    """
    
    def __init__(self, db_path: str, hdf5_path: str):
        """
        Initialize the Address Manager.
        
        Args:
            db_path: Path to the SQLite database
            hdf5_path: Path to the HDF5 storage
        """
        self.db_path = db_path
        self.hdf5_path = hdf5_path
        self.zero_index_mapper = ZeroIndexMapper(db_path, hdf5_path)
        self._init_database()
    
    def _init_database(self):
        """Initialize the database tables for address management"""
        with sqlite3.connect(self.db_path) as conn:
            # Address book table with zero-index support
            conn.execute("""
                CREATE TABLE IF NOT EXISTS address_book (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    addr TEXT UNIQUE,
                    uuid TEXT,
                    type TEXT,  -- 'document', 'sentence', 'token'
                    parent_addr TEXT,
                    coordinate_system TEXT,  -- For control plane functionality
                    is_origin INTEGER DEFAULT 0,  -- Flag for origin points
                    zero_index INTEGER DEFAULT 0,  -- Zero-index value
                    FOREIGN KEY (parent_addr) REFERENCES address_book(addr)
                )
            """)
            
            # Address attributes table for underscore-separated attributes
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
            
            # Create indexes for performance
            conn.execute("CREATE INDEX IF NOT EXISTS idx_address_book_parent_addr ON address_book(parent_addr)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_address_book_type ON address_book(type)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_address_book_uuid ON address_book(uuid)")
            
            conn.commit()
    
    def create_address(self, addr: str, uuid_value: Optional[str] = None, 
                      addr_type: Optional[str] = None, zero_index: int = 0) -> str:
        """
        Create or update an address in the address book.
        
        Args:
            addr: The address string (e.g., "doc:123-0")
            uuid_value: Optional UUID to associate with this address
            addr_type: Optional type of address (document, sentence, token)
            zero_index: Zero-index value (default 0)
            
        Returns:
            The created or updated address
        """
        # Parse address to determine parent if it has hierarchical structure
        parent_addr = None
        if '-' in addr:
            parts = addr.split('-')
            parent_addr = '-'.join(parts[:-1]) if len(parts) > 1 else None
        
        # Check if address already exists
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute("SELECT id FROM address_book WHERE addr = ?", (addr,))
            if cursor.fetchone():
                # Update existing address if needed
                conn.execute(
                    """
                    UPDATE address_book 
                    SET uuid = COALESCE(?, uuid),
                        type = COALESCE(?, type),
                        zero_index = ?
                    WHERE addr = ?
                    """,
                    (uuid_value, addr_type, zero_index, addr)
                )
            else:
                # Ensure parent exists if specified
                if parent_addr:
                    cursor = conn.execute("SELECT id FROM address_book WHERE addr = ?", (parent_addr,))
                    if not cursor.fetchone():
                        # Create parent address recursively
                        self.create_address(parent_addr, None, addr_type)
                
                # Create new address
                conn.execute(
                    """
                    INSERT INTO address_book (addr, uuid, type, parent_addr, zero_index)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (addr, uuid_value, addr_type, parent_addr, zero_index)
                )
            
            conn.commit()
        
        return addr
    
    def add_address_attribute(self, addr: str, key: str, value: str) -> bool:
        """
        Add an attribute to an address.
        
        Args:
            addr: The address string
            key: Attribute key
            value: Attribute value
            
        Returns:
            True if successful, False otherwise
        """
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.execute(
                    """
                    INSERT OR REPLACE INTO address_attributes (addr, attr_key, attr_value)
                    VALUES (?, ?, ?)
                    """,
                    (addr, key, value)
                )
                conn.commit()
            return True
        except Exception as e:
            print(f"Error adding address attribute: {e}")
            return False
    
    def get_address(self, addr: str) -> Optional[Dict[str, Any]]:
        """
        Get information about an address.
        
        Args:
            addr: The address string
            
        Returns:
            Dictionary with address information or None if not found
        """
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute(
                """
                SELECT addr, uuid, type, parent_addr, coordinate_system, is_origin, zero_index
                FROM address_book WHERE addr = ?
                """,
                (addr,)
            )
            
            row = cursor.fetchone()
            if not row:
                return None
            
            address_info = {
                "addr": row[0],
                "uuid": row[1],
                "type": row[2],
                "parent_addr": row[3],
                "coordinate_system": row[4],
                "is_origin": bool(row[5]),
                "zero_index": row[6]
            }
            
            # Get attributes
            cursor = conn.execute(
                "SELECT attr_key, attr_value FROM address_attributes WHERE addr = ?",
                (addr,)
            )
            
            attributes = {row[0]: row[1] for row in cursor.fetchall()}
            if attributes:
                address_info["attributes"] = attributes
            
            return address_info
    
    def resolve_address(self, addr: str, relative_to: Optional[str] = None) -> Dict[str, Any]:
        """
        Resolve an address to its content and relationships.
        
        Args:
            addr: The address to resolve
            relative_to: Optional reference address for relative resolution
            
        Returns:
            Dictionary with resolved address information
        """
        # Get basic address information
        address_info = self.get_address(addr)
        if not address_info:
            # Try to resolve through the zero-index mapper
            uuid = self.zero_index_mapper.map_addr_to_uuid(addr)
            if uuid:
                # Get sentence information from the UUID
                sentence_info = self.zero_index_mapper.get_sentence_info(uuid)
                if sentence_info:
                    return {
                        "resolved": addr,
                        "uuid": uuid,
                        "type": "sentence",
                        "content": sentence_info["text"],
                        "zero_index": 0,
                        "occurrences": sentence_info["addresses"]
                    }
            
            raise ValueError(f"Address not found: {addr}")
        
        # Handle sentence address
        if address_info["type"] == "sentence" and address_info["uuid"]:
            sentence_info = self.zero_index_mapper.get_sentence_info(address_info["uuid"])
            if sentence_info:
                address_info["content"] = sentence_info["text"]
                address_info["occurrences"] = sentence_info["addresses"]
                address_info["tokens"] = sentence_info["tokens"]
        
        # Handle token address
        elif address_info["type"] == "token" and address_info["uuid"]:
            # If address contains sentence UUID and token position
            if "." in addr:
                parts = addr.split(".")
                sentence_addr = parts[0]
                try:
                    token_position = int(parts[1])
                    # Get sentence UUID
                    sentence_uuid = self.zero_index_mapper.map_addr_to_uuid(sentence_addr)
                    if sentence_uuid:
                        token_addr = self.zero_index_mapper.get_token_addr(sentence_uuid, token_position)
                        token_info = self.zero_index_mapper.get_token_info(token_addr)
                        if token_info:
                            address_info["content"] = token_info["text"]
                            address_info["sentence_uuid"] = sentence_uuid
                            address_info["position"] = token_position
                except ValueError:
                    pass
            # If address is a direct token address
            elif "-" in address_info["uuid"]:
                token_info = self.zero_index_mapper.get_token_info(address_info["uuid"])
                if token_info:
                    address_info["content"] = token_info["text"]
                    address_info["sentence_uuid"] = token_info["sentence_uuid"]
                    address_info["position"] = token_info["position"]
        
        # Handle relative addressing if specified
        if relative_to:
            relative_info = self.resolve_address(relative_to)
            if relative_info:
                address_info["relative_to"] = relative_info
                
                # If both are sentences with UUIDs, check for relationships
                if (address_info.get("type") == "sentence" and relative_info.get("type") == "sentence"
                    and address_info.get("uuid") and relative_info.get("uuid")):
                    # Check for direct relationships in the database
                    with sqlite3.connect(self.db_path) as conn:
                        cursor = conn.execute(
                            """
                            SELECT relation_type FROM relations 
                            WHERE source_uuid = ? AND target_uuid = ?
                            """,
                            (relative_info["uuid"], address_info["uuid"])
                        )
                        forward_relations = [row[0] for row in cursor.fetchall()]
                        
                        cursor = conn.execute(
                            """
                            SELECT relation_type FROM relations 
                            WHERE source_uuid = ? AND target_uuid = ?
                            """,
                            (address_info["uuid"], relative_info["uuid"])
                        )
                        backward_relations = [row[0] for row in cursor.fetchall()]
                        
                        if forward_relations:
                            address_info["forward_relations"] = forward_relations
                        if backward_relations:
                            address_info["backward_relations"] = backward_relations
        
        return address_info
    
    def find_addresses_by_uuid(self, uuid_value: str) -> List[str]:
        """
        Find all addresses associated with a specific UUID.
        
        Args:
            uuid_value: UUID to search for
            
        Returns:
            List of addresses
        """
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute(
                "SELECT addr FROM address_book WHERE uuid = ?",
                (uuid_value,)
            )
            return [row[0] for row in cursor.fetchall()]
    
    def find_addresses_by_type(self, addr_type: str) -> List[str]:
        """
        Find all addresses of a specific type.
        
        Args:
            addr_type: Address type (e.g., 'document', 'sentence', 'token')
            
        Returns:
            List of addresses
        """
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute(
                "SELECT addr FROM address_book WHERE type = ?",
                (addr_type,)
            )
            return [row[0] for row in cursor.fetchall()]
    
    def create_coordinate_system(self, name: str, origin_addr: str, 
                                dimensions: Optional[List[Dict]] = None) -> Dict[str, Any]:
        """
        Set up a coordinate system with an origin address.
        
        Args:
            name: Name of the coordinate system
            origin_addr: Address to use as the origin
            dimensions: Optional list of dimensions
            
        Returns:
            Dictionary with coordinate system information
        """
        # Check if the origin address exists
        origin_info = self.get_address(origin_addr)
        if not origin_info:
            raise ValueError(f"Origin address not found: {origin_addr}")
        
        with sqlite3.connect(self.db_path) as conn:
            # Mark the address as an origin point
            conn.execute(
                """
                UPDATE address_book 
                SET is_origin = 1, coordinate_system = ? 
                WHERE addr = ?
                """,
                (name, origin_addr)
            )
            
            # Set up the coordinate system in the database
            # (Implementation depends on the specific requirements)
            
            conn.commit()
        
        return {
            "name": name,
            "origin_addr": origin_addr,
            "dimensions": dimensions or []
        }
    
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
        # Resolve addresses to UUIDs
        source_uuid = self.zero_index_mapper.map_addr_to_uuid(source_addr)
        target_uuid = self.zero_index_mapper.map_addr_to_uuid(target_addr)
        
        if not source_uuid or not target_uuid:
            return False
        
        try:
            with sqlite3.connect(self.db_path) as conn:
                # Ensure the relations table exists
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS relations (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        source_uuid TEXT,
                        target_uuid TEXT,
                        relation_type TEXT,
                        UNIQUE(source_uuid, target_uuid, relation_type),
                        FOREIGN KEY (source_uuid) REFERENCES sentences(uuid),
                        FOREIGN KEY (target_uuid) REFERENCES sentences(uuid)
                    )
                """)
                
                # Add the relationship
                conn.execute(
                    """
                    INSERT OR REPLACE INTO relations (source_uuid, target_uuid, relation_type)
                    VALUES (?, ?, ?)
                    """,
                    (source_uuid, target_uuid, relation_type)
                )
                conn.commit()
            return True
        except Exception as e:
            print(f"Error setting relation: {e}")
            return False