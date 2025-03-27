# app/api/__init__.py
"""
API package for the Dimensional Directory System.
"""

from .v1 import v1_router

# Export available routers
routers = [v1_router]
