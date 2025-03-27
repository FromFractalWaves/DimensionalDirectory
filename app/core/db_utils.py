# app/core/db_utils.py
"""
Database utility functions for the Dimensional Directory System.

This module provides helper functions for managing database connections,
transactions, and common operations across the system.
"""

import sqlite3
import contextlib
import os
from typing import Dict, List, Any, Optional, Generator, Union, Tuple
import h5py
import numpy as np


class DatabaseError(Exception):
    """Exception raised for database errors in the Dimensional Directory System."""
    pass


@contextlib.contextmanager
def get_connection(db_path: str) -> Generator[sqlite3.Connection, None, None]:
    """
    Context manager to get a database connection with proper setup.
    
    Args:
        db_path: Path to the SQLite database file
        
    Yields:
        sqlite3.Connection: Database connection with foreign keys enabled
        
    Raises:
        DatabaseError: If there's an error connecting to the database
    """
    if not os.path.exists(os.path.dirname(db_path)):
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        
    try:
        conn = sqlite3.connect(db_path)
        conn.execute("PRAGMA foreign_keys = ON")
        conn.row_factory = sqlite3.Row  # Enable dictionary-like access to rows
        yield conn
        conn.commit()
    except sqlite3.Error as e:
        raise DatabaseError(f"Database error: {e}")
    finally:
        conn.close()


@contextlib.contextmanager
def get_hdf5(hdf5_path: str, mode: str = "a") -> Generator[h5py.File, None, None]:
    """
    Context manager to get an HDF5 file handle.
    
    Args:
        hdf5_path: Path to the HDF5 file
        mode: File access mode ('r', 'r+', 'w', 'w-', 'a')
        
    Yields:
        h5py.File: HDF5 file handle
        
    Raises:
        DatabaseError: If there's an error accessing the HDF5 file
    """
    if not os.path.exists(os.path.dirname(hdf5_path)):
        os.makedirs(os.path.dirname(hdf5_path), exist_ok=True)
        
    try:
        f = h5py.File(hdf5_path, mode)
        yield f
    except (IOError, OSError) as e:
        raise DatabaseError(f"HDF5 error: {e}")
    finally:
        f.close()


def execute_query(db_path: str, query: str, params: Tuple = (), fetch_all: bool = True) -> List[Dict[str, Any]]:
    """
    Execute a query and return the results as a list of dictionaries.
    
    Args:
        db_path: Path to the SQLite database file
        query: SQL query to execute
        params: Parameters for the query
        fetch_all: Whether to fetch all results or just one
        
    Returns:
        List of dictionaries with query results
        
    Raises:
        DatabaseError: If there's an error executing the query
    """
    with get_connection(db_path) as conn:
        try:
            cursor = conn.execute(query, params)
            if fetch_all:
                result = [dict(row) for row in cursor.fetchall()]
            else:
                row = cursor.fetchone()
                result = [dict(row)] if row else []
            return result
        except sqlite3.Error as e:
            raise DatabaseError(f"Query error: {e}")


def execute_transaction(db_path: str, queries: List[Tuple[str, Tuple]]) -> bool:
    """
    Execute multiple queries as a single transaction.
    
    Args:
        db_path: Path to the SQLite database file
        queries: List of (query, params) tuples
        
    Returns:
        True if the transaction succeeded
        
    Raises:
        DatabaseError: If there's an error during the transaction
    """
    with get_connection(db_path) as conn:
        try:
            for query, params in queries:
                conn.execute(query, params)
            return True
        except sqlite3.Error as e:
            conn.rollback()
            raise DatabaseError(f"Transaction error: {e}")


def store_embedding(hdf5_path: str, group_path: str, dataset_name: str, embedding: np.ndarray) -> bool:
    """
    Store an embedding in the HDF5 file.
    
    Args:
        hdf5_path: Path to the HDF5 file
        group_path: Path to the group in HDF5 file
        dataset_name: Name of the dataset
        embedding: NumPy array containing the embedding
        
    Returns:
        True if the operation succeeded
        
    Raises:
        DatabaseError: If there's an error storing the embedding
    """
    with get_hdf5(hdf5_path) as f:
        try:
            # Create group if it doesn't exist
            group = f
            for part in group_path.strip('/').split('/'):
                if part:
                    if part not in group:
                        group = group.create_group(part)
                    else:
                        group = group[part]
            
            # Delete dataset if it exists
            if dataset_name in group:
                del group[dataset_name]
            
            # Create dataset
            group.create_dataset(dataset_name, data=embedding)
            return True
        except Exception as e:
            raise DatabaseError(f"HDF5 error: {e}")


def get_embedding(hdf5_path: str, group_path: str, dataset_name: str) -> Optional[np.ndarray]:
    """
    Get an embedding from the HDF5 file.
    
    Args:
        hdf5_path: Path to the HDF5 file
        group_path: Path to the group in HDF5 file
        dataset_name: Name of the dataset
        
    Returns:
        NumPy array containing the embedding or None if not found
        
    Raises:
        DatabaseError: If there's an error accessing the embedding
    """
    try:
        with get_hdf5(hdf5_path, 'r') as f:
            # Navigate to the group
            group = f
            for part in group_path.strip('/').split('/'):
                if part and part in group:
                    group = group[part]
                else:
                    return None
            
            # Get dataset
            if dataset_name in group:
                return group[dataset_name][()]
            return None
    except Exception as e:
        raise DatabaseError(f"HDF5 error while getting embedding: {e}")


def check_lstable_file(base_dir: str, dbidL: str, dbidS: str) -> bool:
    """
    Check if a mapping exists in the .LStable file and create/update if necessary.
    
    Args:
        base_dir: Base directory for storing .LStable files
        dbidL: Long identifier
        dbidS: Short identifier
        
    Returns:
        True if the operation succeeded
        
    Raises:
        DatabaseError: If there's an error with the file operations
    """
    try:
        # Create directory for dbidL if it doesn't exist
        directory = os.path.join(base_dir, dbidL)
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
        global_lstable_path = os.path.join(base_dir, ".GlobalLStable")
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
            
        return True
    except IOError as e:
        raise DatabaseError(f"LStable file error: {e}")