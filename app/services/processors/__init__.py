# app/services/processors/__init__.py
"""
Document processors for the Dimensional Directory System.

This package contains processors for different types of input data,
implementing the common interface defined in the base processor.
"""

from .base import DocumentProcessor
from .text import TextDocumentProcessor

__all__ = ["DocumentProcessor", "TextDocumentProcessor"]