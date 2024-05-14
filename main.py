import os
import pandas as pd
import random
import logging
from fastapi import FastAPI, Depends, HTTPException
from pydantic import BaseModel
from typing import Any, Dict
from functools import lru_cache
from starlette.middleware.cors import CORSMiddleware
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.util import get_remote_address
from slowapi.middleware import SlowAPIMiddleware

app = FastAPI()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Environment variable for CSV file path
CSV_FILE_PATH = os.getenv("CSV_FILE_PATH", "personas.csv")

# Rate Limiting
limiter = Limiter(key_func=get_remote_address)
app.state.limiter = limiter
app.add_exception_handler(429, _rate_limit_exceeded_handler)
app.add_middleware(SlowAPIMiddleware)

# CORS settings
origins = [
    "http://localhost",
    "http://localhost:8000",
]
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

class RandomRowResponse(BaseModel):
    row: Dict[str, Any]

@lru_cache()
def load_csv():
    try:
        df = pd.read_csv(CSV_FILE_PATH)
        if df.empty:
            raise ValueError("CSV file is empty")
        return df
    except Exception as e:
        logger.error(f"Error loading CSV file: {e}")
        raise HTTPException(status_code=500, detail="Error loading CSV file")

@app.get("/", response_model=RandomRowResponse)
@limiter.limit("5/minute")
def get_random_row(df: pd.DataFrame = Depends(load_csv)):
    try:
        random_index = random.randint(0, len(df) - 1)
        random_row = df.iloc[random_index].to_dict()
        return RandomRowResponse(row=random_row)
    except Exception as e:
        logger.error(f"Error getting random row: {e}")
        raise HTTPException(status_code=500, detail="Error getting random row")

@app.get("/health")
def health_check():
    return {"status": "ok"}

# Run the FastAPI application
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
