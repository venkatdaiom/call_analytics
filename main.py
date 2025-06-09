# main.py
from fastapi import FastAPI, HTTPException, Query, Security, status # Added Security, status
from fastapi.security import APIKeyHeader # Added APIKeyHeader
from pydantic import BaseModel, field_validator, Field
from typing import List, Optional, Dict, Any
import pandas as pd
import ast
import os

# --- API Key Configuration ---
API_KEY_NAME = "X-API-Key"  # Standard header name for API keys
api_key_header_auth = APIKeyHeader(name=API_KEY_NAME, auto_error=False) # auto_error=False so we can give custom response

# Retrieve the secret API key from environment variable
# Provide a (less secure) default only for local dev if ENV var is not set.
# For Render, the ENV var MUST be set.
EXPECTED_API_KEY = os.getenv("API_SECRET_KEY")
if EXPECTED_API_KEY is None:
    print("WARNING: API_SECRET_KEY environment variable is not set. Authentication will likely fail on server.")
    # You might want to set a default for local testing, but ensure it's not used in prod
    # EXPECTED_API_KEY = "local_dev_key_only" # Example for local

async def get_api_key(api_key_header: Optional[str] = Security(api_key_header_auth)):
    if not EXPECTED_API_KEY: # If the server-side key isn't configured
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="API Key not configured on server."
        )
    if api_key_header == EXPECTED_API_KEY:
        return api_key_header
    else:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Could not validate credentials or API Key missing/invalid"
        )

app = FastAPI(
    title="GMB Call Data Retrieval API",
    description="API to retrieve pre-analyzed call data based on recording URL. Requires X-API-Key header for authentication.", # Updated description
    version="1.0.0"
    # You can add securitySchemes to your OpenAPI docs here if desired
    # openapi_tags=[{"name": "calls", "description": "Operations with call data."}],
    # components={"securitySchemes": {"ApiKeyAuth": {"type": "apiKey", "in": "header", "name": API_KEY_NAME}}}
    # security=[{"ApiKeyAuth": []}] # To apply to all routes by default, or use in individual routes
)

# --- Configuration for CSV loading (remains the same) ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CSV_FILE_PATH = os.path.join(BASE_DIR, "DFX - GMB KPIs - 1000_reviews.csv") # Or your correct CSV name
call_data_df = None

# --- Pydantic Model (remains the same) ---
class CallDetailsResponse(BaseModel):
    AudioDurationMinutes: Optional[float] = None
    UserType: Optional[str] = None
    CallObjective: Optional[str] = None
    Top3Themes: Optional[List[str]] = None
    NextAction: Optional[str] = None
    CallSentiment: Optional[str] = None
    Summary: Optional[str] = None
    AgentImprovementFeedback: Optional[str] = None
    OrderID: Optional[str] = None
    ProductType: Optional[str] = None
    City: Optional[str] = Field(None, alias="City.1")
    CallType: Optional[str] = None
    UserIntentToBuy: Optional[str] = None
    Customer_Language: Optional[str] = None
    Agent_Language: Optional[str] = None

    @field_validator('Top3Themes', mode='before')
    @classmethod
    def parse_stringified_list(cls, value):
        if pd.isna(value) or value is None:
            return None
        if isinstance(value, str):
            try:
                if value.startswith('[') and value.endswith(']'):
                    return ast.literal_eval(value)
                return None
            except (ValueError, SyntaxError):
                print(f"Warning: Could not parse Top3Themes string: {value}")
                return None
        elif isinstance(value, list):
            return value
        return None

    class Config:
        populate_by_name = True

# --- Data Loading (remains the same) ---
def load_data():
    global call_data_df
    try:
        print(f"Attempting to load CSV from: {CSV_FILE_PATH}")
        df = pd.read_csv(CSV_FILE_PATH)
        df.replace(["N/A", ""], pd.NA, inplace=True)
        if 'Recording URL' not in df.columns:
            raise ValueError("'Recording URL' column not found in CSV.")
        df.set_index('Recording URL', inplace=True)
        call_data_df = df
        print(f"Successfully loaded and indexed data from {CSV_FILE_PATH}")
    except FileNotFoundError:
        print(f"ERROR: CSV file not found at {CSV_FILE_PATH}")
        call_data_df = pd.DataFrame()
    except Exception as e:
        print(f"ERROR: Could not load data: {e}")
        import traceback
        traceback.print_exc()
        call_data_df = pd.DataFrame()

@app.on_event("startup")
async def startup_event():
    load_data()

# --- API Endpoint (MODIFIED to include authentication) ---
@app.get(
    "/get-call-details",
    response_model=CallDetailsResponse,
    # To make it appear in Swagger UI as secured:
    # security=[{"ApiKeyAuth": []}] # This is an alternative to global security
    dependencies=[Security(get_api_key)] # Apply security dependency
)
async def get_call_details_by_url(
    recording_url: str = Query(..., description="The full URL of the call recording to search for.")
    # api_key: str = Security(get_api_key) # No need to declare here if using dependencies list
):
    if call_data_df is None or call_data_df.empty:
        print("Data not loaded, startup might have failed or CSV is empty.")
        raise HTTPException(status_code=503, detail="Service Unavailable: Data not loaded or CSV error.")

    try:
        if recording_url not in call_data_df.index:
            raise KeyError
        call_info_series = call_data_df.loc[recording_url]
        call_info_dict = call_info_series.where(pd.notnull(call_info_series), None).to_dict()
        print(f"Raw data for URL {recording_url}: {call_info_dict}")
        
        response_data = CallDetailsResponse(**call_info_dict)
        return response_data

    except KeyError:
        raise HTTPException(status_code=404, detail=f"Recording URL not found: {recording_url}")
    except Exception as e:
        print(f"Error processing data for URL {recording_url}: {type(e).__name__} - {e}")
        import traceback
        traceback.print_exc()
        if "PydanticValidationError" in str(type(e)):
             raise HTTPException(status_code=500, detail=f"Data validation error for the requested URL. Check data format. Details: {e}")
        else:
             raise HTTPException(status_code=500, detail=f"Error parsing or processing data for the requested URL. Details: {e}")
