# app/core/__init__.py
"""
Core components of the Dimensional Directory System.

This package contains the fundamental classes and utilities for
document processing, addressing, and data storage.
"""

from .dd_manager import DimensionalDirectory
from .content_mapper import DocumentMapper

__all__ = ["DimensionalDirectory", "DocumentMapper"]

