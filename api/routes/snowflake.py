from fastapi import APIRouter, HTTPException
from api.sqlite_utils import fetch_table_data

router = APIRouter()

@router.get("/{tablename}")
def get_table(tablename: str):
    try:
        data = fetch_table_data(tablename)
        return {"data": data}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
