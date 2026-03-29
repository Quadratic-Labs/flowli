"""
The Flowlet Example app.
"""
from fastapi import FastAPI
from . import flows


app = FastAPI(
    title="Flowlet Example Application",
    description="Example application demonstrating the Flowlet workflow orchestration framework",
    version="0.1.0"
)
app.include_router(flows.flowlet.router, prefix="", tags=["Flowlet"])


@app.get("/")
def root():
    """Root endpoint providing information about the application"""
    return {
        "name": "Flowlet Example",
        "version": "0.1.0",
        "description": "Example application demonstrating Flowlet framework",
        "docs": {
            "intent": "visit the API's documentation",
            "route": "/docs",
        },
        "flows": {
            "intent": "list available flows",
            "route": "/flows",
        },
    }


@app.get("/health")
def health_check():
    """Health check endpoint"""
    return {"status": "healthy"}
