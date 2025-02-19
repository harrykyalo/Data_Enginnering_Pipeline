import pandas as pd
import logging
from google.cloud import bigquery
from extract import fetch_data

def clean_users_data(data):
    users_list = data.get("users", [])
    return pd.DataFrame(
        [
            {
                "user_id": u["id"],
                "first_name": u["firstName"],
                "last_name": u["lastName"],
                "gender": u["gender"],
                "age": u["age"],
                "street": u["address"].get("address", ""),
                "city": u["address"].get("city", ""),
                "postal_code": u["address"].get("postalCode", ""),
            }
            for u in users_list
        ]
    )

def clean_products_data(data):
    products_list = data.get("products", [])
    return pd.DataFrame(
        [
            {
                "product_id": p["id"],
                "name": p["title"],
                "category": p["category"],
                "brand": p["brand"],
                "price": float(p["price"]),
            }
            for p in products_list
            if float(p["price"]) > 50
        ]
    )

def clean_carts_data(data):
    carts_list = data.get("carts", [])
    return pd.DataFrame(
        [
            {
                "cart_id": c["id"],
                "user_id": c["userId"],
                "product_id": p["id"],
                "quantity": p["quantity"],
                "price": p["price"],
                "total_cart_value": sum(p["quantity"] * p["price"] for p in c["products"]),
            }
            for c in carts_list
            for p in c["products"]
        ]
    )

def upload_to_bigquery(df, dataset_id, table_id):
    try:
        client = bigquery.Client()
        table_ref = client.dataset(dataset_id).table(table_id)
        job = client.load_table_from_dataframe(df, table_ref)
        job.result()
        logging.info(f"Data uploaded to BigQuery: {dataset_id}.{table_id}")
    except Exception as e:
        logging.error(f"Failed to upload data to BigQuery: {e}")

def clean_and_upload_data(dataset_id):
    api_endpoints = {
        "users": "https://dummyjson.com/users",
        "products": "https://dummyjson.com/products",
        "carts": "https://dummyjson.com/carts",
    }

    for key, url in api_endpoints.items():
        data = fetch_data(url)
        if key == "users":
            df = clean_users_data(data)
            upload_to_bigquery(df, dataset_id, "users_table")
        elif key == "products":
            df = clean_products_data(data)
            upload_to_bigquery(df, dataset_id, "products_table")
        elif key == "carts":
            df = clean_carts_data(data)
            upload_to_bigquery(df, dataset_id, "carts_table")
