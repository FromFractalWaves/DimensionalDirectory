import re
import sqlite3
from typing import Dict, List, Optional, Tuple, Any, Union, Callable
from app.core.addressing.zero_index_mapper import ZeroIndexMapper

class FunctionService:
    """
    Function Service for formula evaluation in the Dimensional Directory system.
    
    This service handles the evaluation of formulas like rel(A1, "synonym")
    using the zero-indexed UUID approach for address resolution.
    """
    
    def __init__(self, db_path: str, zero_index_mapper: ZeroIndexMapper):
        """
        Initialize the Function Service.
        
        Args:
            db_path: Path to the SQLite database
            zero_index_mapper: ZeroIndexMapper instance for address resolution
        """
        self.db_path = db_path
        self.zero_index_mapper = zero_index_mapper
        
        # Register available functions
        self.function_registry = {
            "rel": self._rel_function,
            "rel.all": self._rel_all_function,
            "rel.count": self._rel_count_function,
            "addr": self._addr_function,
            "uuid": self._uuid_function
        }
    
    def evaluate(self, formula: str, context_cell: Optional[Tuple[int, int]] = None) -> Any:
        """
        Evaluate a formula string.
        
        Args:
            formula: Formula string like 'rel(A1, "synonym")'
            context_cell: Optional tuple with (row, col) of the current cell
            
        Returns:
            Evaluation result
        """
        # Parse formula to extract function name and arguments
        func_name, args = self._parse_formula(formula)
        
        if func_name not in self.function_registry:
            raise ValueError(f"Unknown function: {func_name}")
        
        # Resolve cell references in arguments
        resolved_args = self._resolve_args(args, context_cell)
        
        # Call the appropriate function
        return self.function_registry[func_name](*resolved_args)
    
    def _parse_formula(self, formula: str) -> Tuple[str, List[str]]:
        """
        Parse a formula into function name and arguments.
        
        Args:
            formula: Formula string like 'rel(A1, "synonym")'
            
        Returns:
            Tuple of (function_name, argument_list)
        """
        # Remove '=' prefix if present
        if formula.startswith('='):
            formula = formula[1:].strip()
        
        # Extract function name and argument string
        match = re.match(r'^(\w+(?:\.\w+)*)\((.*)\)$', formula)
        if not match:
            raise ValueError(f"Invalid formula format: {formula}")
        
        func_name, args_str = match.groups()
        
        # Parse arguments
        args = []
        current_arg = ''
        in_quotes = False
        bracket_level = 0
        
        for char in args_str:
            if char == '"' and not in_quotes:
                in_quotes = True
                current_arg += char
            elif char == '"' and in_quotes:
                in_quotes = False
                current_arg += char
            elif char == '(' and not in_quotes:
                bracket_level += 1
                current_arg += char
            elif char == ')' and not in_quotes:
                bracket_level -= 1
                current_arg += char
            elif char == ',' and not in_quotes and bracket_level == 0:
                args.append(current_arg.strip())
                current_arg = ''
            else:
                current_arg += char
        
        if current_arg:
            args.append(current_arg.strip())
        
        return func_name, args
    
    def _resolve_args(self, args: List[str], context_cell: Optional[Tuple[int, int]] = None) -> List[Any]:
        """
        Resolve cell references and literals in argument list.
        
        Args:
            args: List of argument strings
            context_cell: Optional tuple with (row, col) of the current cell
            
        Returns:
            List of resolved argument values
        """
        resolved = []
        
        for arg in args:
            # Handle string literals
            if arg.startswith('"') and arg.endswith('"'):
                resolved.append(arg[1:-1])  # Remove quotes
            # Handle cell references (e.g., A1, B2)
            elif re.match(r'^[A-Za-z]+[0-9]+$', arg):
                # Convert to address
                cell_addr = self._cell_to_addr(arg, context_cell)
                resolved.append(cell_addr)
            # Handle direct addresses (e.g., doc:123-0)
            elif re.match(r'^doc:[^-]+-\d+$', arg):
                resolved.append(arg)
            # Handle numbers
            elif re.match(r'^-?\d+(\.\d+)?$', arg):
                if '.' in arg:
                    resolved.append(float(arg))
                else:
                    resolved.append(int(arg))
            # Handle boolean literals
            elif arg.lower() == 'true':
                resolved.append(True)
            elif arg.lower() == 'false':
                resolved.append(False)
            # Handle nested functions
            elif '(' in arg and ')' in arg:
                # This is a simplification - proper parsing of nested functions is more complex
                nested_result = self.evaluate(arg, context_cell)
                resolved.append(nested_result)
            # Pass through as is
            else:
                resolved.append(arg)
        
        return resolved
    
    def _cell_to_addr(self, cell_ref: str, context_cell: Optional[Tuple[int, int]] = None) -> str:
        """
        Convert a cell reference (e.g., A1) to a document address.
        
        This implementation is a placeholder. In a real system, you would 
        need to map the spreadsheet coordinates to document addresses.
        
        Args:
            cell_ref: Cell reference (e.g., A1)
            context_cell: Optional tuple with (row, col) of the current cell
            
        Returns:
            Document address string
        """
        # Parse column and row from cell reference
        match = re.match(r'^([A-Za-z]+)([0-9]+)$', cell_ref)
        if not match:
            raise ValueError(f"Invalid cell reference: {cell_ref}")
        
        col_str, row_str = match.groups()
        
        # Convert column string to 0-based index
        col_idx = 0
        for char in col_str:
            col_idx = col_idx * 26 + (ord(char.upper()) - ord('A') + 1)
        col_idx -= 1  # Adjust to 0-based
        
        # Convert row string to 0-based index
        row_idx = int(row_str) - 1
        
        # In a real implementation, this would look up the document and position
        # based on the spreadsheet coordinates
        
        # Placeholder implementation - you would replace this with actual mapping logic
        doc_uuid = "current_document"  # This would come from the current document context
        position = row_idx  # Simple mapping where row index corresponds to sentence position
        
        return f"doc:{doc_uuid}-{position}"
    
    def _get_cell_content(self, cell_addr: str) -> Optional[str]:
        """
        Get the content at a cell address.
        
        Args:
            cell_addr: Cell address string
            
        Returns:
            Cell content or None if not found
        """
        # Resolve address to UUID
        uuid = self.zero_index_mapper.map_addr_to_uuid(cell_addr)
        if not uuid:
            return None
        
        # Get sentence text
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute(
                "SELECT text FROM sentences WHERE uuid = ?",
                (uuid,)
            )
            result = cursor.fetchone()
            return result[0] if result else None
    
    def _rel_function(self, source_addr: str, rel_type: str) -> Optional[str]:
        """
        Implementation of rel() function.
        Returns first related content with the specified relationship type.
        
        Args:
            source_addr: Source address
            rel_type: Relationship type
            
        Returns:
            Related content or None if not found
        """
        # Resolve source address to UUID
        source_uuid = self.zero_index_mapper.map_addr_to_uuid(source_addr)
        if not source_uuid:
            return None
        
        # Find first related sentence with this relationship type
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute(
                """
                SELECT t.uuid, t.text 
                FROM sentences t
                JOIN relations r ON t.uuid = r.target_uuid
                WHERE r.source_uuid = ? AND r.relation_type = ?
                LIMIT 1
                """,
                (source_uuid, rel_type)
            )
            
            result = cursor.fetchone()
            return result[1] if result else None
    
    def _rel_all_function(self, source_addr: str, rel_type: str) -> List[str]:
        """
        Implementation of rel.all() function.
        Returns all related content with the specified relationship type.
        
        Args:
            source_addr: Source address
            rel_type: Relationship type
            
        Returns:
            List of related content
        """
        # Resolve source address to UUID
        source_uuid = self.zero_index_mapper.map_addr_to_uuid(source_addr)
        if not source_uuid:
            return []
        
        # Find all related sentences with this relationship type
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute(
                """
                SELECT t.uuid, t.text 
                FROM sentences t
                JOIN relations r ON t.uuid = r.target_uuid
                WHERE r.source_uuid = ? AND r.relation_type = ?
                """,
                (source_uuid, rel_type)
            )
            
            return [row[1] for row in cursor.fetchall()]
    
    def _rel_count_function(self, source_addr: str, rel_type: Optional[str] = None) -> int:
        """
        Implementation of rel.count() function.
        Returns the count of related content with the specified relationship type.
        
        Args:
            source_addr: Source address
            rel_type: Optional relationship type (if None, counts all relationships)
            
        Returns:
            Count of related content
        """
        # Resolve source address to UUID
        source_uuid = self.zero_index_mapper.map_addr_to_uuid(source_addr)
        if not source_uuid:
            return 0
        
        # Count related sentences
        with sqlite3.connect(self.db_path) as conn:
            if rel_type:
                cursor = conn.execute(
                    """
                    SELECT COUNT(*) 
                    FROM relations
                    WHERE source_uuid = ? AND relation_type = ?
                    """,
                    (source_uuid, rel_type)
                )
            else:
                cursor = conn.execute(
                    """
                    SELECT COUNT(*) 
                    FROM relations
                    WHERE source_uuid = ?
                    """,
                    (source_uuid,)
                )
            
            result = cursor.fetchone()
            return result[0] if result else 0
    
    def _addr_function(self, uuid_value: str) -> List[str]:
        """
        Implementation of addr() function.
        Returns all addresses where a UUID appears.
        
        Args:
            uuid_value: UUID value
            
        Returns:
            List of addresses
        """
        return self.zero_index_mapper.map_uuid_to_addr(uuid_value)
    
    def _uuid_function(self, addr: str) -> Optional[str]:
        """
        Implementation of uuid() function.
        Returns the UUID associated with an address.
        
        Args:
            addr: Address string
            
        Returns:
            UUID or None if not found
        """
        return self.zero_index_mapper.map_addr_to_uuid(addr)