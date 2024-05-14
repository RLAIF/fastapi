import os
import pandas as pd
import random
import logging
from fastapi import FastAPI, Depends, HTTPException, Request
from pydantic import BaseModel
from typing import Any, Dict
from functools import lru_cache
from starlette.middleware.cors import CORSMiddleware
from dotenv import load_dotenv
import json_log_formatter

# Load environment variables from .env file
load_dotenv()

app = FastAPI()

# Configure JSON logging
formatter = json_log_formatter.JSONFormatter()
json_handler = logging.FileHandler(filename='app.log')
json_handler.setFormatter(formatter)
logger = logging.getLogger(__name__)
logger.addHandler(json_handler)
logger.setLevel(logging.INFO)

# Environment variable for CSV file path
CSV_FILE_PATH = os.getenv("CSV_FILE_PATH", "personas.csv")

# CORS settings
origins = os.getenv("CORS_ORIGINS", "").split(",")
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins if origins else ["*"],
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
        logger.error({"error": f"Error loading CSV file: {e}"})
        raise HTTPException(status_code=500, detail="Error loading CSV file")

@app.get("/", response_model=RandomRowResponse)
def get_random_row(request: Request, df: pd.DataFrame = Depends(load_csv)):
    try:
        random_index = random.randint(0, len(df) - 1)
        random_row = df.iloc[random_index].to_dict()
        return RandomRowResponse(row=random_row)
    except Exception as e:
        logger.error({"error": f"Error getting random row: {e}"})
        raise HTTPException(status_code=500, detail="Error getting random row")

@app.get("/health")
def health_check():
    return {"status": "ok"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
