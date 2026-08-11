# src/agent_platform/api/main.py
# FastAPI application entrypoint

from fastapi import FastAPI
from fastapi.responses import HTMLResponse, JSONResponse
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
    return HTMLResponse(
        """
        <!doctype html>
        <html lang="en">
          <head>
            <meta charset="utf-8" />
            <meta name="viewport" content="width=device-width, initial-scale=1" />
            <title>AI Agent Platform</title>
            <style>
              body { font-family: system-ui, sans-serif; margin: 3rem; line-height: 1.5; }
              a { display: inline-block; margin-right: 1rem; margin-top: 0.5rem; }
              code { background: #f4f4f4; padding: 0.15rem 0.35rem; border-radius: 4px; }
            </style>
          </head>
          <body>
            <h1>AI Agent Platform</h1>
            <p>The API is running.</p>
            <p>Open one of these pages:</p>
            <p>
              <a href="/docs">API Docs</a>
              <a href="/redoc">ReDoc</a>
              <a href="/monitoring/dashboard">Monitoring Dashboard</a>
              <a href="/health">Health Check</a>
            </p>
            <p>Base URL: <code>/</code></p>
          </body>
        </html>
        """
    )


# Serve static files for dashboard (optional)
# Uncomment if you have static files
# app.mount("/static", StaticFiles(directory="static"), name="static")
