# Dimensional Directory Project Overview


The **Dimensional Directory** project is a modular and extensible
framework designed to manage hierarchical and multidimensional data
structures. The system enables users to define, configure, and interact
with AddressPlanes and control planes, foundational components for
advanced computational frameworks like **C-Space Simulations** and the
**Subconscious System Architecture**.


more coming soon 

here is more:

# Dimensional Directory System

The Dimensional Directory is a system for indexing and interacting with text data using a hierarchical addressing scheme with zero-indexed UUID mapping for many-to-many relationships.

## Key Features

- **Zero-Indexed UUID Addressing**: Efficiently manages many-to-many relationships between documents and sentences
- **Sentence Deduplication**: Identical sentences across documents share the same UUID
- **Hierarchical Addressing**: Flexible addressing scheme with support for levels and attributes
- **Formula Evaluation**: Spreadsheet-like functions for relationship management
- **Embedding Support**: Store and retrieve embeddings for sentences and tokens

## Architecture

The system has a layered architecture:

1. **API Layer**: FastAPI endpoints for document processing, search, and management
2. **Service Layer**: Orchestrates core components and provides high-level operations
3. **Core Layer**: Implements zero-index mapping, addressing, and content handling
4. **Data Layer**: Stores data in SQLite (metadata) and HDF5 (embeddings, content)

## Getting Started

### Installation

```bash
# Clone the repository
git clone https://github.com/yourusername/dimensional-directory.git
cd dimensional-directory

# Create and activate virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

### Running the API

```bash
# Run the FastAPI server
python main.py
```

The API will be available at http://localhost:8000

### API Documentation

Once the server is running, you can access the auto-generated API documentation at:
- http://localhost:8000/docs (Swagger UI)
- http://localhost:8000/redoc (ReDoc)

## Usage Examples

### Creating a Document

```python
import requests
import json

# Create a document
response = requests.post(
    "http://localhost:8000/api/v1/documents/",
    json={
        "content": "Hello world. This is a test document. Hello world again.",
        "dbidL": "TestDocument",
        "dbidS": "001",
        "title": "Test Document"
    }
)

document_info = response.json()
```

### Creating a Relationship

```python
# Create a relationship between two sentences
requests.post(
    "http://localhost:8000/api/v1/relations/",
    json={
        "source_addr": "doc:123-0",
        "target_addr": "doc:456-1",
        "relation_type": "similar"
    }
)
```

### Using Formulas

```python
# Evaluate a formula
response = requests.post(
    "http://localhost:8000/api/v1/functions/evaluate/",
    json={
        "formula": "rel(A1, 'synonym')",
        "context_cell": [0, 2]  # Row 0, Column 2
    }
)

result = response.json()
```

## Architecture Design

The system uses a combination of SQLite and HDF5 for storage:

- **SQLite**: Stores metadata, relationships, and address mappings
- **HDF5**: Stores document content and embeddings

The zero-indexed UUID approach means:
- Every sentence has a UUID based on its content
- Identical sentences share the same UUID
- Each occurrence of a sentence has a unique address (e.g., `doc:123-0`)
- All indexing starts from 0 within each context

## License

This project is licensed under the MIT License - see the LICENSE file for details.