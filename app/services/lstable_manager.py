import os
import sqlite3
import uuid
from typing import Dict, List, Optional, Tuple

class LStableManager:
    """
    Manages L-S identifier mappings (dbidL and dbidS) for the Dimensional Directory system.
    Handles both in-memory SQLite operations and file-based .LStable generation.
    """
    
    def __init__(self, db_path: str, base_dir: str):
        """
        Initialize the LStable manager.
        
        Args:
            db_path: Path to the SQLite database
            base_dir: Base directory for storing .LStable files
        """
        self.db_path = db_path
        self.base_dir = base_dir
        self._init_database()
        
    def _init_database(self):
        """Initialize the database tables for L-S mappings"""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS lstable (
                    dbidL TEXT PRIMARY KEY,
                    dbidS TEXT UNIQUE,
                    description TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            conn.commit()
    
    def register_mapping(self, dbidL: str, dbidS: Optional[str] = None, 
                          description: Optional[str] = None) -> Tuple[str, str]:
        """
        Register a new L-S mapping. If dbidS is not provided, generate a unique one.
        
        Args:
            dbidL: Long identifier (human-readable name)
            dbidS: Short identifier (optional, will be generated if not provided)
            description: Optional description of the mapping
            
        Returns:
            Tuple of (dbidL, dbidS)
        """
        if not dbidS:
            # Generate a unique short ID
            dbidS = str(uuid.uuid4().hex[:8])
            
        with sqlite3.connect(self.db_path) as conn:
            # Check if dbidL already exists
            cursor = conn.execute("SELECT dbidS FROM lstable WHERE dbidL = ?", (dbidL,))
            existing = cursor.fetchone()
            
            if existing:
                return dbidL, existing[0]
            
            # Check if dbidS already exists
            cursor = conn.execute("SELECT dbidL FROM lstable WHERE dbidS = ?", (dbidS,))
            if cursor.fetchone():
                # Generate a new unique dbidS
                while True:
                    new_dbidS = str(uuid.uuid4().hex[:8])
                    cursor = conn.execute("SELECT dbidL FROM lstable WHERE dbidS = ?", (new_dbidS,))
                    if not cursor.fetchone():
                        dbidS = new_dbidS
                        break
            
            # Insert the new mapping
            conn.execute(
                "INSERT INTO lstable (dbidL, dbidS, description) VALUES (?, ?, ?)",
                (dbidL, dbidS, description)
            )
            conn.commit()
            
            # Generate or update .LStable file
            self._update_lstable_file(dbidL, dbidS)
            
            return dbidL, dbidS
    
    def get_mapping(self, dbidL: Optional[str] = None, dbidS: Optional[str] = None) -> Optional[Dict]:
        """
        Get mapping information by either dbidL or dbidS.
        
        Args:
            dbidL: Long identifier
            dbidS: Short identifier
            
        Returns:
            Dictionary with mapping information or None if not found
        """
        if not dbidL and not dbidS:
            raise ValueError("Either dbidL or dbidS must be provided")
            
        with sqlite3.connect(self.db_path) as conn:
            if dbidL:
                cursor = conn.execute(
                    "SELECT dbidL, dbidS, description, created_at FROM lstable WHERE dbidL = ?", 
                    (dbidL,)
                )
            else:
                cursor = conn.execute(
                    "SELECT dbidL, dbidS, description, created_at FROM lstable WHERE dbidS = ?", 
                    (dbidS,)
                )
                
            result = cursor.fetchone()
            if not result:
                return None
                
            return {
                "dbidL": result[0],
                "dbidS": result[1],
                "description": result[2],
                "created_at": result[3]
            }
    
    def list_mappings(self) -> List[Dict]:
        """
        List all L-S mappings.
        
        Returns:
            List of dictionaries containing mapping information
        """
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute(
                "SELECT dbidL, dbidS, description, created_at FROM lstable ORDER BY created_at"
            )
            
            return [
                {
                    "dbidL": row[0],
                    "dbidS": row[1],
                    "description": row[2],
                    "created_at": row[3]
                }
                for row in cursor.fetchall()
            ]
    
    def delete_mapping(self, dbidL: Optional[str] = None, dbidS: Optional[str] = None) -> bool:
        """
        Delete an L-S mapping by either dbidL or dbidS.
        
        Args:
            dbidL: Long identifier
            dbidS: Short identifier
            
        Returns:
            True if deleted, False if not found
        """
        if not dbidL and not dbidS:
            raise ValueError("Either dbidL or dbidS must be provided")
            
        mapping = self.get_mapping(dbidL=dbidL, dbidS=dbidS)
        if not mapping:
            return False
            
        with sqlite3.connect(self.db_path) as conn:
            if dbidL:
                conn.execute("DELETE FROM lstable WHERE dbidL = ?", (dbidL,))
            else:
                conn.execute("DELETE FROM lstable WHERE dbidS = ?", (dbidS,))
            conn.commit()
            
            # Update the .LStable file by removing the entry
            self._remove_from_lstable_file(mapping["dbidL"], mapping["dbidS"])
            
            return True
    
    def _update_lstable_file(self, dbidL: str, dbidS: str):
        """
        Update or create the .LStable file for a given dbidL.
        
        Args:
            dbidL: Long identifier
            dbidS: Short identifier
        """
        # Create directory for dbidL if it doesn't exist
        directory = os.path.join(self.base_dir, dbidL)
        os.makedirs(directory, exist_ok=True)
        
        # Path to the .LStable file
        lstable_path = os.path.join(directory, ".LStable")
        
        # Check if file exists and has the mapping
        mapping_exists = False
        lines = []
        
        if os.path.exists(lstable_path):
            with open(lstable_path, "r") as f:
                lines = f.readlines()
                
            for i, line in enumerate(lines):
                if line.strip().startswith(f"{dbidL}="):
                    lines[i] = f"{dbidL}={dbidS}\n"
                    mapping_exists = True
                    break
        
        # If mapping doesn't exist, add it
        if not mapping_exists:
            lines.append(f"{dbidL}={dbidS}\n")
            
        # Write the updated file
        with open(lstable_path, "w") as f:
            f.writelines(lines)
            
        # Update global .LStable file
        global_lstable_path = os.path.join(self.base_dir, ".GlobalLStable")
        global_lines = []
        global_mapping_exists = False
        
        if os.path.exists(global_lstable_path):
            with open(global_lstable_path, "r") as f:
                global_lines = f.readlines()
                
            for i, line in enumerate(global_lines):
                if line.strip().startswith(f"{dbidL}="):
                    global_lines[i] = f"{dbidL}={dbidS}\n"
                    global_mapping_exists = True
                    break
        
        if not global_mapping_exists:
            global_lines.append(f"{dbidL}={dbidS}\n")
            
        with open(global_lstable_path, "w") as f:
            f.writelines(global_lines)
    
    def _remove_from_lstable_file(self, dbidL: str, dbidS: str):
        """
        Remove a mapping from the .LStable files.
        
        Args:
            dbidL: Long identifier
            dbidS: Short identifier
        """
        # Path to the .LStable file
        directory = os.path.join(self.base_dir, dbidL)
        lstable_path = os.path.join(directory, ".LStable")
        
        if os.path.exists(lstable_path):
            with open(lstable_path, "r") as f:
                lines = f.readlines()
                
            lines = [line for line in lines if not line.strip().startswith(f"{dbidL}=")]
            
            with open(lstable_path, "w") as f:
                f.writelines(lines)
        
        # Update global .LStable file
        global_lstable_path = os.path.join(self.base_dir, ".GlobalLStable")
        
        if os.path.exists(global_lstable_path):
            with open(global_lstable_path, "r") as f:
                global_lines = f.readlines()
                
            global_lines = [line for line in global_lines if not line.strip().startswith(f"{dbidL}=")]
            
            with open(global_lstable_path, "w") as f:
                f.writelines(global_lines)