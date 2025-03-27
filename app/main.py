"""
Main entry point for the Dimensional Directory API service.
"""

import os
from fastapi import FastAPI
from app.api import routers
import uvicorn

# Create the FastAPI application
app = FastAPI(
    title="Dimensional Directory API",
    description="API for the Dimensional Directory system with zero-indexed UUID addressing",
    version="2.0.0"
)

# Register API routers
for router in routers:
    app.include_router(router, prefix="/api/v1")

# Add health check endpoint
@app.get("/health")
async def health_check():
    """Check if the API is running."""
    return {"status": "healthy"}

if __name__ == "__main__":
    # Get port from environment variable or use default
    port = int(os.environ.get("PORT", 8000))
    
    # Run the application
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=port,
        reload=True  # Enable auto-reload for development
    )