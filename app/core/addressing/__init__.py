"""
Address management for the Dimensional Directory System.

This module implements the zero-indexed UUID approach for many-to-many mappings
and provides tools for creating, resolving, and manipulating hierarchical addresses.
"""

from .zero_index_mapper import ZeroIndexMapper

__all__ = ["ZeroIndexMapper"]
