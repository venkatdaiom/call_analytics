# main.py
from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel, field_validator
from typing import List, Optional, Dict
import pandas as pd
import ast
import os # For constructing absolute path to CSV

app = FastAPI(
    title="GMB Call Data Retrieval API",
    description="API to retrieve pre-analyzed call data based on recording URL.",
    version="1.0.0"
)

# --- Configuration ---
# Construct the absolute path to the CSV file relative to this script's location
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CSV_FILE_PATH = os.path.join(BASE_DIR, "DFX - GMB KPIs - 1000_reviews.csv")

call_data_df = None

class CallDetailsResponse(BaseModel):
    CallID: int
    AudioDurationMinutes: float
    UserType: str
    CallObjective: str
    Top3Themes: List[str]
    NextAction: str
    Language: Dict[str, str]
    CallSentiment: str
    Summary: str
    AgentImprovementFeedback: Optional[str] = None
    OrderID: Optional[str] = None
    ProductType: Optional[str] = None
    City: Optional[str] = None

    @field_validator('Top3Themes', 'Language', mode='before')
    @classmethod
    def parse_stringified_json(cls, value):
        if isinstance(value, str):
            try:
                return ast.literal_eval(value)
            except (ValueError, SyntaxError):
                if 'Top3Themes' in str(value): return []
                if 'Language' in str(value): return {}
                return value
        return value
    class Config:
        populate_by_name = True


def load_data():
    global call_data_df
    try:
        print(f"Attempting to load CSV from: {CSV_FILE_PATH}") # Debug print
        df = pd.read_csv(CSV_FILE_PATH)
        df.replace("N/A", None, inplace=True)
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
        call_data_df = pd.DataFrame()

@app.on_event("startup")
async def startup_event():
    load_data()

@app.get("/get-call-details", response_model=CallDetailsResponse)
async def get_call_details_by_url(
    recording_url: str = Query(..., description="The full URL of the call recording to search for.")
):
    if call_data_df is None or call_data_df.empty:
        print("Data not loaded, startup might have failed or CSV is empty.") # Debug print
        raise HTTPException(status_code=503, detail="Service Unavailable: Data not loaded or CSV error.")
    try:
        call_info_series = call_data_df.loc[recording_url]
        call_info_dict = call_info_series.to_dict()
        call_info_dict['CallID'] = int(call_info_series['CallID'].iloc[0] if isinstance(call_info_series['CallID'], pd.Series) else call_info_series['CallID'])
        return CallDetailsResponse(**call_info_dict)
    except KeyError:
        raise HTTPException(status_code=404, detail=f"Recording URL not found: {recording_url}")
    except ValueError as ve:
        print(f"Data parsing error for URL {recording_url}: {ve}")
        raise HTTPException(status_code=500, detail=f"Error parsing data for the requested URL.")
    except Exception as e:
        print(f"An unexpected error occurred: {e} for URL: {recording_url}")
        raise HTTPException(status_code=500, detail=f"An internal server error occurred: {str(e)}")

# To run locally (PythonAnywhere will handle running it differently):
# if __name__ == "__main__":
#     import uvicorn
#     uvicorn.run(app, host="0.0.0.0", port=8000)