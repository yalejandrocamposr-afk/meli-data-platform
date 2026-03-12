from fastapi import FastAPI
from api.routes import sales

app = FastAPI(
    title="MELI Data Platform API",
    description="API for analytics data",
    version="1.0"
)

app.include_router(sales.router)