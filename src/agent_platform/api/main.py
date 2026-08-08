# src/agent_platform/api/main.py
# FastAPI application entrypoint

from fastapi import FastAPI
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from src.agent_platform.api.routes import tasks, tenants, monitoring

app = FastAPI(
    title="AI Agent Platform",
    description="Multi-agent orchestration system",
    version="0.1.0"
)

# Register routes
app.include_router(tasks.router)
app.include_router(tenants.router)
app.include_router(monitoring.router)


@app.get("/health")
async def health_check():
    return JSONResponse(content={"status": "ok", "version": "0.1.0"})


@app.get("/")
async def root():
    return {"message": "AI Agent Platform is running", "docs": "/docs"}


# Serve static files for dashboard (optional)
# Uncomment if you have static files
# app.mount("/static", StaticFiles(directory="static"), name="static")