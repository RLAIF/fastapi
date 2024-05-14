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
from prometheus_client import start_http_server, Summary
import sentry_sdk
from sentry_sdk.integrations.fastapi import FastApiIntegration

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

# Create a metric to track time spent and requests made.
REQUEST_TIME = Summary('request_processing_seconds', 'Time spent processing request')

@app.on_event("startup")
def startup_event():
    start_http_server(8001)

@app.get("/random-row", response_model=RandomRowResponse)
@REQUEST_TIME.time()
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

# Initialize Sentry
sentry_sdk.init(
    dsn=os.getenv("SENTRY_DSN"),
    integrations=[FastApiIntegration()]
)

# Run the FastAPI application
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
