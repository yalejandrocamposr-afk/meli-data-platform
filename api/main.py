from fastapi import FastAPI
from api.routes import sales

app = FastAPI(
    title="MELI Data Platform API",
    description="API for analytics data",
    version="1.0"
)

# health check endpoint
@app.get("/health")
def health():
    return {"status": "ok"}

app.include_router(
    sales.router,
    prefix="/analytics",
    tags=["analytics"]
)

