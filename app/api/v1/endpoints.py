from fastapi import APIRouter, HTTPException, Depends
from pydantic import BaseModel, Field
from typing import Dict, List, Optional, Any, Union
import numpy as np
import json

from app.services.dd_service import DimensionalDirectoryService

router = APIRouter()

# Dependency
def get_dd_service():
    return DimensionalDirectoryService(base_path="dd_data")

# Request/Response Models
class DocumentRequest(BaseModel):
    content: str
    dbidL: str
    dbidS: Optional[str] = None
    title: Optional[str] = None
    source: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None

class DocumentResponse(BaseModel):
    uuid: str
    dbidL: str
    dbidS: str
    title: Optional[str] = None
    source: Optional[str] = None
    sentence_count: int
    sentences: List[Dict[str, Any]]

class SearchRequest(BaseModel):
    query: str
    search_type: str = "token"  # token, sentence, document

class AddressRequest(BaseModel):
    levels: List[str]
    attributes: Optional[List[str]] = None
    uuid_value: Optional[str] = None
    addr_type: Optional[str] = None

class AddressResolveRequest(BaseModel):
    addr: str
    relative_to: Optional[str] = None

class RelationRequest(BaseModel):
    source_addr: str
    target_addr: str
    relation_type: str

class FunctionRequest(BaseModel):
    formula: str
    context_cell: Optional[List[int]] = None  # [row, col]

class EmbeddingRequest(BaseModel):
    uuid_value: str
    embedding: List[float]
    entity_type: str = "sentence"  # sentence or token
    token_position: Optional[int] = None

# Document Endpoints
@router.post("/documents/", response_model=DocumentResponse)
async def create_document(request: DocumentRequest, service: DimensionalDirectoryService = Depends(get_dd_service)):
    """Create a new document in the system."""
    try:
        return service.create_document(
            content=request.content,
            dbidL=request.dbidL,
            dbidS=request.dbidS,
            title=request.title,
            source=request.source,
            metadata=request.metadata
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/documents/{doc_id}")
async def get_document(doc_id: str, service: DimensionalDirectoryService = Depends(get_dd_service)):
    """Get document information."""
    try:
        document = service.get_document(doc_id)
        if not document:
            raise HTTPException(status_code=404, detail=f"Document not found: {doc_id}")
        return document
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/documents/")
async def list_documents(service: DimensionalDirectoryService = Depends(get_dd_service)):
    """List all documents in the system."""
    try:
        return {"documents": service.get_all_documents()}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# Sentence Endpoints
@router.get("/sentences/{sentence_uuid}")
async def get_sentence(sentence_uuid: str, service: DimensionalDirectoryService = Depends(get_dd_service)):
    """Get sentence information."""
    try:
        sentence = service.get_sentence(sentence_uuid)
        if not sentence:
            raise HTTPException(status_code=404, detail=f"Sentence not found: {sentence_uuid}")
        return sentence
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/sentences/")
async def list_sentences(service: DimensionalDirectoryService = Depends(get_dd_service)):
    """List all sentences in the system."""
    try:
        return {"sentences": service.get_all_sentences()}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# Search Endpoints
@router.post("/search/")
async def search(request: SearchRequest, service: DimensionalDirectoryService = Depends(get_dd_service)):
    """Search for content in the system."""
    try:
        if request.search_type == "token":
            results = service.search_by_token(request.query)
            return {"results": results, "count": len(results)}
        elif request.search_type == "sentence":
            documents = service.find_documents_with_sentence(request.query)
            return {"results": documents, "count": len(documents)}
        else:
            raise HTTPException(status_code=400, detail=f"Unsupported search type: {request.search_type}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# Address Endpoints
@router.post("/addresses/")
async def create_address(request: AddressRequest, service: DimensionalDirectoryService = Depends(get_dd_service)):
    """Create a hierarchical address."""
    try:
        addr = service.create_address(
            levels=request.levels,
            attributes=request.attributes,
            uuid_value=request.uuid_value,
            addr_type=request.addr_type
        )
        return {"address": addr}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/addresses/resolve/")
async def resolve_address(request: AddressResolveRequest, service: DimensionalDirectoryService = Depends(get_dd_service)):
    """Resolve an address, optionally relative to another address."""
    try:
        result = service.resolve_address(request.addr, request.relative_to)
        return result
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/addresses/{addr}")
async def get_address(addr: str, service: DimensionalDirectoryService = Depends(get_dd_service)):
    """Get address information."""
    try:
        address_info = service.resolve_address(addr)
        if not address_info:
            raise HTTPException(status_code=404, detail=f"Address not found: {addr}")
        return address_info
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# Relationship Endpoints
@router.post("/relations/")
async def create_relation(request: RelationRequest, service: DimensionalDirectoryService = Depends(get_dd_service)):
    """Create a relationship between two addresses."""
    try:
        success = service.set_relation(
            source_addr=request.source_addr,
            target_addr=request.target_addr,
            relation_type=request.relation_type
        )
        if not success:
            raise HTTPException(status_code=400, detail="Failed to create relationship")
        return {"status": "success"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# Function Endpoints
@router.post("/functions/evaluate/")
async def evaluate_function(request: FunctionRequest, service: DimensionalDirectoryService = Depends(get_dd_service)):
    """Evaluate a function formula."""
    try:
        context_cell = tuple(request.context_cell) if request.context_cell else None
        result = service.evaluate_function(request.formula, context_cell)
        return {"result": result}
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# Embedding Endpoints
@router.post("/embeddings/")
async def add_embedding(request: EmbeddingRequest, service: DimensionalDirectoryService = Depends(get_dd_service)):
    """Add embedding for a sentence or token."""
    try:
        success = service.add_embedding(
            uuid_value=request.uuid_value,
            embedding=np.array(request.embedding),
            entity_type=request.entity_type,
            token_position=request.token_position
        )
        if not success:
            raise HTTPException(status_code=400, detail="Failed to add embedding")
        return {"status": "success"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/embeddings/{uuid_value}")
async def get_embedding(
    uuid_value: str, 
    entity_type: str = "sentence", 
    token_position: Optional[int] = None,
    service: DimensionalDirectoryService = Depends(get_dd_service)
):
    """Get embedding for a sentence or token."""
    try:
        embedding = service.get_embedding(uuid_value, entity_type, token_position)
        if embedding is None:
            raise HTTPException(status_code=404, detail=f"Embedding not found for {uuid_value}")
        return {"embedding": embedding.tolist()}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))