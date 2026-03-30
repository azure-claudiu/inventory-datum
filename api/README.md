# FastAPI SQLite API

This API serves data from a local SQLite database (`snowflake.db`) using FastAPI. The main endpoint allows you to retrieve all rows from any table in the database.

## Requirements

- Python 3.8+
- FastAPI
- Uvicorn


Install dependencies:

- If you already installed all packages from requirements.txt, you do NOT need to run pip install again.
- Otherwise, you can install just the API dependencies with:

```bash
pip install fastapi uvicorn
```

## Starting the Web Server

From the root of the project (where `api/` is located), run:

```bash
uvicorn api.main:app --reload
```

- The `--reload` flag enables auto-reload on code changes (useful for development).
- By default, the server will start at http://127.0.0.1:8000

## API Usage

### Get All Rows from a Table

**Endpoint:**

```
GET /api/snowflake/{tablename}
```

- Replace `{tablename}` with the name of the table you want to query (as found in `snowflake.db`).

**Example:**

```
GET http://127.0.0.1:8000/api/snowflake/products
```

**Response:**

- JSON object with a `data` key containing a list of rows (each row is a dictionary of column values).

```json
{
  "data": [
    {"id": 1, "name": "Widget", "quantity": 100},
    {"id": 2, "name": "Gadget", "quantity": 50}
  ]
}
```

### Interactive API Docs

Once the server is running, visit:

- Swagger UI: [http://127.0.0.1:8000/docs](http://127.0.0.1:8000/docs)
- ReDoc: [http://127.0.0.1:8000/redoc](http://127.0.0.1:8000/redoc)

These provide interactive documentation and allow you to try the API in your browser.

## Notes

- The database file `snowflake.db` must be present in the project root.
- The API is ready for future extension (e.g., query parameters, authentication).
