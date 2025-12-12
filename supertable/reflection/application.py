from __future__ import annotations

import os
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from pathlib import Path

# single FastAPI app lives here only
app = FastAPI(title="SuperTable App", version="1.0.0")

# 1. Define the base directory (where application.py resides)
STATIC_DIR = str(Path(__file__).resolve().parent / "static")
app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")

# include the two modules' routers (no circular imports)
from supertable.reflection.admin_app import router as admin_router
from supertable.reflection.api_app import router as api_router

app.include_router(admin_router)
app.include_router(api_router)


if __name__ == "__main__":
    import uvicorn

    host = os.getenv("SUPERTABLE_HOST", "0.0.0.0")
    port = int(os.getenv("SUPERTABLE_PORT", "8080"))
    reload_flag = os.getenv("UVICORN_RELOAD", "0").strip().lower() in ("1", "true", "yes", "on")

    uvicorn.run(app, host=host, port=port, reload=reload_flag)
