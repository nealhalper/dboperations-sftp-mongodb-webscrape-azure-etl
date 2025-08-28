from config import BASE_URL, FILES_TO_INGEST
import polars as pl
import requests
import io

def preview_remote_data(url):
    response = requests.get(url)
    response.raise_for_status()
    df = pl.read_csv(io.BytesIO(response.content))
    pl.Config.set_tbl_cols(len(df.columns))  # Show all columns
    print(f"\n--- Data Preview for {url} ---")
    print(df.head())
    print("\n--- Columns and Types ---")
    print(df.dtypes)
    print(df.columns)

if __name__ == "__main__":
    for file_name in FILES_TO_INGEST:
        url = f"{BASE_URL}/{file_name}"
        preview_remote_data(url)