# src/agent_platform/api/main.py
# FastAPI application entrypoint

from fastapi import FastAPI
from fastapi.responses import JSONResponse

app = FastAPI(
    title="AI Agent Platform",
    description="Multi-agent orchestration system",
    version="0.1.0"
)

@app.get("/health")
async def health_check():
    return JSONResponse(content={"status": "ok", "version": "0.1.0"})

@app.get("/")
async def root():
    return {"message": "AI Agent Platform is running"}