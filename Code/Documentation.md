# ETL Pipeline Documentation

## 1. Pipeline Design

### Overview
The ETL pipeline extracts, transforms, and loads data from an API into Google BigQuery, enriching the data with meaningful insights. The process involves:

- **Extraction**: Fetching raw JSON data from APIs.
- **Storage**: Saving raw data in a Google Cloud Storage (GCS) bucket under `raw/`.
- **Cleaning & Transformation**:
  - Raw data is cleaned and formatted.
  - Data is loaded into BigQuery (`your_dataset`).
- **BigQuery Transformations**:
  - Aggregations and joins are performed on tables.
  - Enriched tables (`user_summary`, `category_summary`, `cart_details`) are stored in `enrich_data` dataset

### DAG Structure and Task Dependencies
1. **Extract**: Fetch data from API (Users, Products, Carts).
2. **Store**: Save JSON files in GCS (`raw/` folder).
3. **Transform & Load**: Clean and upload data to BigQuery (`your_dataset` dataset).
4. **BigQuery Transformations**: Perform SQL transformations and store results in `enrich_data` dataset.
---

## 2. Codebase Overview

### Main Scripts & Modules

#### `main.py`
- The main entry point of the pipeline.
- Calls all functions in sequence.

#### `extract.py`
- Fetches data from API.
- Saves data as JSON and uploads to GCS.

#### `transform.py`
- Cleans and structures raw data.
- Uploads structured data to BigQuery.

#### `bigquery_queries.py`
- Contains SQL queries for data enrichment.
- Executes queries and stores results in BigQuery.

#### `export.py`
- Exports transformed tables from BigQuery to GCS.
---
## 3. BigQuery Queries

### User Summary Query


```sql
CREATE OR REPLACE TABLE `savannah_api.enrich_data.user_summary` AS
SELECT u.user_id, u.first_name, SUM(c.total_cart_value) AS total_spent, 
       SUM(c.quantity) AS total_items, u.age, u.city
FROM `savannah_api.your_dataset.users_table` u
JOIN `savannah_api.your_dataset.carts_table` c
ON u.user_id = c.user_id
GROUP BY u.user_id, u.first_name, u.age, u.city; 
```
Purpose: Summarizes total spending and number of purchases per user.

### Category Summary Query

```sql
CREATE OR REPLACE TABLE `savannah_api.enrich_data.category_summary` AS
SELECT p.category, SUM(c.total_cart_value) AS total_sales, SUM(c.quantity) AS total_items_sold
FROM `savannah_api.your_dataset.products_table` p
JOIN `savannah_api.your_dataset.carts_table` c
ON p.product_id = c.product_id
GROUP BY p.category;
```
Purpose: Aggregates sales by product category.

### Cart Details Query
```sql
CREATE OR REPLACE TABLE `savannah_api.enrich_data.cart_details` AS
SELECT c.cart_id, c.user_id, c.product_id, c.quantity, c.price, c.total_cart_value, 
       u.first_name, p.name AS product_name, p.category
FROM `savannah_api.your_dataset.carts_table` c
JOIN `savannah_api.your_dataset.users_table` u
ON c.user_id = u.user_id
JOIN `savannah_api.your_dataset.products_table` p
ON c.product_id = p.product_id;
```
Purpose: Provides transaction-level details enriched with user and product data.

## 4.  Assumptions and Trade-offs
### Assumptions
- API response format remains consistent.
- Only products priced above $50 are considered.
- Each cart contains multiple products, requiring data flattening.
- User details are static (do not change frequently).
- The pipeline should be daily, ensuring up-to-date transformations.
### Trade-offs
#### Storing raw JSON files in GCS:
- ✅ Allows for reprocessing in case of failures.  
- ❌ Increases storage costs.  

#### Using BigQuery for transformations:
- ✅ Reduces computation on local machines.  
- ❌ Requires additional storage for intermediate results.  

#### Exporting transformed data to GCS as CSV:
- ✅ Enables external analysis and reporting.  
- ❌ Data duplication between BigQuery and GCS.  
