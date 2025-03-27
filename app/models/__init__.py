# app/models/__init__.py
"""
Data models for the Dimensional Directory System.

This package contains Pydantic models for request/response validation and data transfer.
"""

from pydantic import BaseModel
from typing import Optional, Dict, List, Any, Union


class InputRequest(BaseModel):
    """Request model for adding new input text."""
    content: str


class InputResponse(BaseModel):
    """Response model after processing input text."""
    uuid: str
    unit_count: str
    metadata: Dict


class TokenizeResponse(BaseModel):
    """Response model after tokenizing text."""
    uuid: str
    tokens: List[Dict]


class DocumentRequest(BaseModel):
    """Request model for creating a new document."""
    content: str
    dbidL: str
    dbidS: Optional[str] = None
    title: Optional[str] = None
    source: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None


class DocumentResponse(BaseModel):
    """Response model after creating a document."""
    uuid: str
    dbidL: str
    dbidS: str
    title: Optional[str] = None
    source: Optional[str] = None
    sentence_count: int
    sentences: List[Dict[str, Any]]


class SearchRequest(BaseModel):
    """Request model for searching content."""
    query: str
    search_type: str = "token"  # token, sentence, document


class AddressRequest(BaseModel):
    """Request model for creating a hierarchical address."""
    levels: List[str]
    attributes: Optional[List[str]] = None
    uuid_value: Optional[str] = None
    addr_type: Optional[str] = None


class AddressResolveRequest(BaseModel):
    """Request model for resolving an address."""
    addr: str
    relative_to: Optional[str] = None


class CoordinateSystemRequest(BaseModel):
    """Request model for creating a coordinate system."""
    addr: str
    name: str
    dimensions: Optional[List[Dict[str, Any]]] = None


class MappingRequest(BaseModel):
    """Request model for creating an L-S mapping."""
    dbidL: str
    dbidS: Optional[str] = None
    description: Optional[str] = None

class AddressModel(BaseModel):
    """Model representing a hierarchical address."""
    addr: str
    uuid: Optional[str] = None
    type: Optional[str] = None
    parent_addr: Optional[str] = None
    zero_index: int = 0
    attributes: Optional[Dict[str, str]] = None

class SentenceModel(BaseModel):
    """Model representing a sentence with its UUID and occurrences."""
    uuid: str
    text: str
    hash: Optional[str] = None
    occurrences: List[str] = []
    zero_index: int = 0
    created_at: Optional[str] = None

class DocumentModel(BaseModel):
    """Model representing a document with its metadata and sentences."""
    uuid: str
    dbidL: str
    dbidS: str
    title: Optional[str] = None
    source: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None
    sentence_count: int
    sentences: List[Dict[str, Any]] = []
    created_at: Optional[str] = None

class RelationModel(BaseModel):
    """Model representing a relationship between sentences."""
    source_uuid: str
    target_uuid: str
    relation_type: str

__all__ = [
    "InputRequest", "InputResponse", "TokenizeResponse",
    "DocumentRequest", "DocumentResponse", "SearchRequest",
    "AddressRequest", "AddressResolveRequest",
    "CoordinateSystemRequest", "MappingRequest",
    "AddressModel",
    "SentenceModel",
    "DocumentModel",
    "RelationModel"
]
