import logging
from extract import fetch_and_store_data
from transform import clean_and_upload_data
from big_queries import run_bq_transformations

def setup_logging():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def main():
    setup_logging()

    api_endpoints = {
        "users": "https://dummyjson.com/users",
        "products": "https://dummyjson.com/products",
        "carts": "https://dummyjson.com/carts",
    }

    bucket_name = "savannah-api"
    dataset_id = "your_dataset"

    # Extraction
    fetch_and_store_data(api_endpoints, bucket_name)

    # Transformation & Load
    clean_and_upload_data(dataset_id)

    # BigQuery Transformations
    run_bq_transformations(dataset_id)

    logging.info("ETL process completed successfully.")

if __name__ == "__main__":
    main()

#$env:GOOGLE_APPLICATION_CREDENTIALS="C:\Users\hkyalo\google_key\savannah-451007-29107b0f3a17.json"