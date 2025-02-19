import logging
from google.cloud import bigquery

def run_bq_transformations(dataset_id):
    client = bigquery.Client()
    queries = {
        "user_summary": f"""
            CREATE OR REPLACE TABLE `savannah-451007.Enrich_Data.user_summary` AS
            SELECT u.user_id, u.first_name, SUM(c.total_cart_value) AS total_spent,
                   SUM(c.quantity) AS total_items, u.age, u.city
            FROM `{dataset_id}.users_table` u
            JOIN `{dataset_id}.carts_table` c
            ON u.user_id = c.user_id
            GROUP BY u.user_id, u.first_name, u.age, u.city;
        """,
        "category_summary": f"""
            CREATE OR REPLACE TABLE `savannah-451007.Enrich_Data.category_summary` AS
            SELECT p.category, SUM(c.total_cart_value) AS total_sales, SUM(c.quantity) AS total_items_sold
            FROM `{dataset_id}.products_table` p
            JOIN `{dataset_id}.carts_table` c
            ON p.product_id = c.product_id
            GROUP BY p.category;
        """,
        "cart_details": f"""
            CREATE OR REPLACE TABLE `savannah-451007.Enrich_Data.cart_details` AS
            SELECT c.cart_id, c.user_id, c.product_id, c.quantity, c.price, c.total_cart_value,
                   u.first_name, p.name AS product_name, p.category
            FROM `{dataset_id}.carts_table` c
            JOIN `{dataset_id}.users_table` u
            ON c.user_id = u.user_id
            JOIN `{dataset_id}.products_table` p
            ON c.product_id = p.product_id;
        """
    }

    for table, query in queries.items():
        job = client.query(query)
        job.result()
        logging.info(f"{table} transformation completed.")
