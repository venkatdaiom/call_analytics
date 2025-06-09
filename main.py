# main.py
from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel, field_validator, Field
from typing import List, Optional, Dict, Any
import pandas as pd
import ast
import os

app = FastAPI(
    title="GMB Call Data Retrieval API",
    description="API to retrieve pre-analyzed call data based on recording URL.",
    version="1.0.0"
)

# --- Configuration ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
# *** UPDATE CSV FILENAME HERE ***
CSV_FILE_PATH = os.path.join(BASE_DIR, "DFX - GMB KPIs - 1000_reviews.csv") # Make sure this is the correct name

call_data_df = None

# --- Pydantic Model for the Response (Reflecting desired columns) ---
class CallDetailsResponse(BaseModel):
    AudioDurationMinutes: Optional[float] = None # Made optional in case of parsing issues initially
    UserType: Optional[str] = None
    CallObjective: Optional[str] = None
    Top3Themes: Optional[List[str]] = None # Keep ast.literal_eval for this
    NextAction: Optional[str] = None
    CallSentiment: Optional[str] = None
    Summary: Optional[str] = None
    AgentImprovementFeedback: Optional[str] = None
    OrderID: Optional[str] = None
    ProductType: Optional[str] = None
    City: Optional[str] = Field(None, alias="City.1") # Use alias for 'City.1'
    CallType: Optional[str] = None
    UserIntentToBuy: Optional[str] = None
    Customer_Language: Optional[str] = None
    Agent_Language: Optional[str] = None
    # Optional: Add CallID if you want it in the response
    # CallID: Optional[int] = None


    @field_validator('Top3Themes', mode='before')
    @classmethod
    def parse_stringified_list(cls, value):
        if pd.isna(value) or value is None: # Handle NaN or None gracefully
            return None
        if isinstance(value, str):
            try:
                # Attempt to parse, but be ready for simple strings if they aren't list-like
                if value.startswith('[') and value.endswith(']'):
                    return ast.literal_eval(value)
                # If it's just a plain string (not representing a list), return it as a single-item list or handle as needed
                # For now, if it's not a list string, we'll assume it might be an error or a single theme.
                # Let's return None if parsing fails and it's not an empty string, to investigate data quality.
                # Or, if single themes are possible as plain strings, convert to list: return [value]
                return None # Or handle plain strings appropriately, e.g. return [value] if that's valid
            except (ValueError, SyntaxError):
                print(f"Warning: Could not parse Top3Themes string: {value}")
                return None # Return None or an empty list on parsing error
        elif isinstance(value, list): # If it's already a list (e.g., from direct DataFrame access)
            return value
        return None # Default for other unexpected types

    class Config:
        populate_by_name = True # Allows using alias like "City.1"

# --- Data Loading ---
def load_data():
    global call_data_df
    try:
        print(f"Attempting to load CSV from: {CSV_FILE_PATH}")
        df = pd.read_csv(CSV_FILE_PATH)

        # Handle "N/A" or empty strings more robustly by replacing them with None (which Pandas often does as NaN)
        df.replace(["N/A", ""], pd.NA, inplace=True) # Replace with pandas NA for better type handling

        if 'Recording URL' not in df.columns:
            raise ValueError("'Recording URL' column not found in CSV. Please check column names.")
        df.set_index('Recording URL', inplace=True)
        call_data_df = df
        print(f"Successfully loaded and indexed data from {CSV_FILE_PATH}")
        print(f"DataFrame columns: {call_data_df.columns.tolist()}") # Print columns for debugging
        print(f"DataFrame head:\n{call_data_df.head().to_string()}") # Print head for debugging
    except FileNotFoundError:
        print(f"ERROR: CSV file not found at {CSV_FILE_PATH}")
        call_data_df = pd.DataFrame()
    except Exception as e:
        print(f"ERROR: Could not load data: {e}")
        import traceback
        traceback.print_exc() # Print full traceback for loading errors
        call_data_df = pd.DataFrame()

@app.on_event("startup")
async def startup_event():
    load_data()

# --- API Endpoint ---
@app.get("/get-call-details", response_model=CallDetailsResponse)
async def get_call_details_by_url(
    recording_url: str = Query(..., description="The full URL of the call recording to search for.")
):
    if call_data_df is None or call_data_df.empty:
        print("Data not loaded, startup might have failed or CSV is empty.")
        raise HTTPException(status_code=503, detail="Service Unavailable: Data not loaded or CSV error.")

    try:
        # Ensure the recording_url exists in the index
        if recording_url not in call_data_df.index:
            raise KeyError # Will be caught below

        call_info_series = call_data_df.loc[recording_url]
        # Convert Pandas Series to dictionary. Pandas might introduce NaNs for missing values.
        call_info_dict = call_info_series.where(pd.notnull(call_info_series), None).to_dict()

        # Pydantic will use the alias "City.1" to populate the "City" field.
        # Pydantic will also handle type conversions for Optional fields.
        # The @field_validator will handle Top3Themes.
        
        # For debugging the dictionary before Pydantic
        print(f"Raw data for URL {recording_url}: {call_info_dict}")

        return CallDetailsResponse(**call_info_dict)

    except KeyError:
        raise HTTPException(status_code=404, detail=f"Recording URL not found: {recording_url}")
    except Exception as e: # Catch PydanticValidationErrors and other unexpected errors
        print(f"Error processing data for URL {recording_url}: {type(e).__name__} - {e}")
        import traceback
        traceback.print_exc() # Print full traceback for processing errors
        # Customize the error message based on whether it's a validation error or something else
        if "PydanticValidationError" in str(type(e)):
             raise HTTPException(status_code=500, detail=f"Data validation error for the requested URL. Check data format. Details: {e}")
        else:
             raise HTTPException(status_code=500, detail=f"Error parsing or processing data for the requested URL. Details: {e}")
