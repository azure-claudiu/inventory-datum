from fastapi import FastAPI
from api.routes import snowflake

app = FastAPI()
app.include_router(snowflake.router, prefix="/api/snowflake")
