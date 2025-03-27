# app/api/v1/__init__.py
"""
API v1 endpoints for the Dimensional Directory System.
"""

from .endpoints import router as v1_router

__all__ = ["v1_router"]

