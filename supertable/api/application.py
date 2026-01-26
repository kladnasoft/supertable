from __future__ import annotations

import os
from fastapi import FastAPI
from supertable.api.api import router as api_router

# single FastAPI app lives here only
app = FastAPI(title="SuperTable Rest API", version="1.0.0")

# include the two modules' routers (no circular imports)


app.include_router(api_router)


if __name__ == "__main__":
    import uvicorn

    host = os.getenv("SUPERTABLE_HOST", "0.0.0.0")
    port = int(os.getenv("SUPERTABLE_API_PORT", "8090"))
    reload_flag = os.getenv("UVICORN_RELOAD", "0").strip().lower() in ("1", "true", "yes", "on")

    uvicorn.run(app, host=host, port=port, reload=reload_flag)
