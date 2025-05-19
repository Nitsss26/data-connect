import os
import requests
import pandas as pd
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from google.oauth2 import service_account
import logging
import certifi
import json
from datetime import datetime

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Shopify API settings
SHOPIFY_API_VERSION = '2024-04'

# BigQuery schema for customers table
CUSTOMERS_BQ_SCHEMA = [
    bigquery.SchemaField('customer_id', 'STRING', mode='REQUIRED'),
    bigquery.SchemaField('email', 'STRING'),
    bigquery.SchemaField('first_name', 'STRING'),
    bigquery.SchemaField('last_name', 'STRING'),
    bigquery.SchemaField('created_at', 'TIMESTAMP'),
    bigquery.SchemaField('updated_at', 'TIMESTAMP'),
    bigquery.SchemaField('orders_count', 'INTEGER'),
    bigquery.SchemaField('total_spent', 'FLOAT')
]

# BigQuery schema for products table
PRODUCTS_BQ_SCHEMA = [
    bigquery.SchemaField('product_id', 'STRING', mode='REQUIRED'),
    bigquery.SchemaField('title', 'STRING'),
    bigquery.SchemaField('vendor', 'STRING'),
    bigquery.SchemaField('product_type', 'STRING'),
    bigquery.SchemaField('created_at', 'TIMESTAMP'),
    bigquery.SchemaField('updated_at', 'TIMESTAMP'),
    bigquery.SchemaField('price', 'FLOAT')
]

def create_dataset(client, dataset_id, project_id):
    """Create a BigQuery dataset if it doesn't exist."""
    dataset_ref = f"{project_id}.{dataset_id}"
    try:
        client.get_dataset(dataset_ref)
        logger.info(f"Dataset {dataset_id} already exists.")
    except NotFound:
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = "US"  # Set dataset location (change if needed)
        client.create_dataset(dataset, timeout=30)
        logger.info(f"Created dataset {dataset_id}.")

def get_bigquery_client(credentials_path, project_id):
    """Initialize BigQuery client with service account credentials."""
    try:
        credentials = service_account.Credentials.from_service_account_file(credentials_path)
        client = bigquery.Client(credentials=credentials, project=project_id)
        logger.info("BigQuery client initialized successfully.")
        return client
    except Exception as e:
        logger.error(f"Failed to initialize BigQuery client: {e}")
        raise

def check_table_exists(client, table_id):
    """Check if a BigQuery table exists."""
    try:
        client.get_table(table_id)
        return True
    except NotFound:
        return False

def create_table(client, table_id, schema):
    """Create a BigQuery table if it doesn't exist."""
    if not check_table_exists(client, table_id):
        table = bigquery.Table(table_id, schema=schema)
        client.create_table(table)
        logger.info(f"Created table {table_id}.")

def fetch_shopify_data(endpoint, resource_type, api_token, shop_name):
    """Fetch data from Shopify with pagination."""
    headers = {
        'X-Shopify-Access-Token': api_token,
        'Content-Type': 'application/json'
    }
    params = {'limit': 250}  # Max 250 records per page
    data = []
    base_url = f'https://{shop_name}.myshopify.com/admin/api/{SHOPIFY_API_VERSION}'
    endpoint = f"{base_url}/{endpoint}"

    while endpoint:
        try:
            response = requests.get(endpoint, headers=headers, params=params, verify=certifi.where())
            response.raise_for_status()
            response_data = response.json()
            data.extend(response_data[resource_type])
            logger.info(f"Fetched {len(response_data[resource_type])} {resource_type}. Total: {len(data)}")
            
            # Handle pagination
            link_header = response.headers.get('Link')
            endpoint = None
            if link_header:
                links = link_header.split(',')
                for link in links:
                    if 'rel="next"' in link:
                        endpoint = link[link.find('<')+1:link.find('>')]
                        break
            params = {}  # Clear params for next page
        except requests.RequestException as e:
            logger.error(f"Failed to fetch Shopify {resource_type}: {e}")
            raise

    return data

def transform_customers(customers):
    """Transform Shopify customers into a DataFrame with default values for missing fields."""
    try:
        data = [{
            'customer_id': str(customer['id']),
            'email': customer.get('email', ''),
            'first_name': customer.get('first_name', ''),
            'last_name': customer.get('last_name', ''),
            'created_at': pd.to_datetime(customer.get('created_at', None)),
            'updated_at': pd.to_datetime(customer.get('updated_at', None)),
            'orders_count': customer.get('orders_count', 0),
            'total_spent': float(customer.get('total_spent', 0.0))
        } for customer in customers]
        df = pd.DataFrame(data)
        logger.info(f"Transformed {len(df)} customers into DataFrame.")
        return df
    except Exception as e:
        logger.error(f"Failed to transform customers: {e}")
        raise

def transform_products(products):
    """Transform Shopify products into a DataFrame with default values for missing fields."""
    try:
        data = [{
            'product_id': str(product['id']),
            'title': product.get('title', ''),
            'vendor': product.get('vendor', ''),
            'product_type': product.get('product_type', ''),
            'created_at': pd.to_datetime(product.get('created_at', None)),
            'updated_at': pd.to_datetime(product.get('updated_at', None)),
            'price': float(product['variants'][0]['price']) if product.get('variants') and product['variants'] else 0.0
        } for product in products]
        df = pd.DataFrame(data)
        logger.info(f"Transformed {len(df)} products into DataFrame.")
        return df
    except Exception as e:
        logger.error(f"Failed to transform products: {e}")
        raise

def load_to_staging(client, df, staging_table_id, schema):
    """Load DataFrame to BigQuery staging table."""
    try:
        job_config = bigquery.LoadJobConfig(
            schema=schema,
            write_disposition='WRITE_TRUNCATE'
        )
        job = client.load_table_from_dataframe(df, staging_table_id, job_config=job_config)
        job.result()  # Wait for job to complete
        logger.info(f"Loaded {len(df)} rows to staging table {staging_table_id}.")
    except Exception as e:
        logger.error(f"Failed to load to staging table: {e}")
        raise

def merge_upsert(client, main_table_id, staging_table_id, key_field, fields):
    """Perform UPSERT using MERGE statement."""
    update_fields = ', '.join([f'{field} = S.{field}' for field in fields])
    insert_fields = ', '.join(fields)
    insert_values = ', '.join([f'S.{field}' for field in fields])
    merge_query = f"""
    MERGE INTO `{main_table_id}` T
    USING `{staging_table_id}` S
    ON T.{key_field} = S.{key_field}
    WHEN MATCHED THEN
    UPDATE SET {update_fields}
    WHEN NOT MATCHED THEN
    INSERT ({insert_fields})
    VALUES ({insert_values})
    """
    try:
        query_job = client.query(merge_query)
        query_job.result()  # Wait for job to complete
        logger.info(f"Completed UPSERT from {staging_table_id} to {main_table_id}.")
    except Exception as e:
        logger.error(f"Failed to perform UPSERT: {e}")
        raise

def delete_staging_table(client, staging_table_id):
    """Delete the staging table."""
    try:
        client.delete_table(staging_table_id, not_found_ok=True)
        logger.info(f"Deleted staging table {staging_table_id}.")
    except Exception as e:
        logger.error(f"Failed to delete staging table: {e}")
        raise

def fetch_bigquery_data(client, table_id):
    """Fetch all data from a BigQuery table."""
    try:
        query = f"SELECT * FROM `{table_id}` LIMIT 1000"  # Limit to 1000 rows for API response
        query_job = client.query(query)
        results = query_job.result()
        
        # Convert to list of dicts and handle datetime serialization
        data = []
        for row in results:
            row_dict = dict(row)
            for key, value in row_dict.items():
                if isinstance(value, datetime):
                    row_dict[key] = value.isoformat()
            data.append(row_dict)
            
        logger.info(f"Fetched {len(data)} rows from {table_id}.")
        return data
    except Exception as e:
        logger.error(f"Failed to fetch data from {table_id}: {e}")
        raise

def run_pipeline(shopify_shop_name, shopify_api_token, gcp_project_id, credentials_path, dataset_id='shopify_data'):
    """Run the Shopify to BigQuery pipeline for customers and products."""
    try:
        # Log pipeline start
        logger.info(f"Starting Shopify to BigQuery pipeline for shop: {shopify_shop_name}")
        
        # Construct table IDs dynamically
        CUSTOMERS_MAIN_TABLE_ID = f'{gcp_project_id}.{dataset_id}.customers'
        CUSTOMERS_STAGING_TABLE_ID = f'{gcp_project_id}.{dataset_id}.customers_staging'
        PRODUCTS_MAIN_TABLE_ID = f'{gcp_project_id}.{dataset_id}.products'
        PRODUCTS_STAGING_TABLE_ID = f'{gcp_project_id}.{dataset_id}.products_staging'

        # Initialize BigQuery client
        client = get_bigquery_client(credentials_path, gcp_project_id)

        # Create dataset
        create_dataset(client, dataset_id, gcp_project_id)

        # Create main and staging tables for customers
        create_table(client, CUSTOMERS_MAIN_TABLE_ID, CUSTOMERS_BQ_SCHEMA)
        create_table(client, CUSTOMERS_STAGING_TABLE_ID, CUSTOMERS_BQ_SCHEMA)

        # Create main and staging tables for products
        create_table(client, PRODUCTS_MAIN_TABLE_ID, PRODUCTS_BQ_SCHEMA)
        create_table(client, PRODUCTS_STAGING_TABLE_ID, PRODUCTS_BQ_SCHEMA)

        # Fetch and transform customers
        logger.info("Fetching customer data from Shopify...")
        customers = fetch_shopify_data('customers.json', 'customers', shopify_api_token, shopify_shop_name)
        if customers:
            customers_df = transform_customers(customers)
            load_to_staging(client, customers_df, CUSTOMERS_STAGING_TABLE_ID, CUSTOMERS_BQ_SCHEMA)
            merge_upsert(client, CUSTOMERS_MAIN_TABLE_ID, CUSTOMERS_STAGING_TABLE_ID, 
                         'customer_id', ['customer_id', 'email', 'first_name', 'last_name', 
                                         'created_at', 'updated_at', 'orders_count', 'total_spent'])
            delete_staging_table(client, CUSTOMERS_STAGING_TABLE_ID)
        else:
            logger.info("No customers to process.")

        # Fetch and transform products
        logger.info("Fetching product data from Shopify...")
        products = fetch_shopify_data('products.json', 'products', shopify_api_token, shopify_shop_name)
        if products:
            products_df = transform_products(products)
            load_to_staging(client, products_df, PRODUCTS_STAGING_TABLE_ID, PRODUCTS_BQ_SCHEMA)
            merge_upsert(client, PRODUCTS_MAIN_TABLE_ID, PRODUCTS_STAGING_TABLE_ID, 
                         'product_id', ['product_id', 'title', 'vendor', 'product_type', 
                                        'created_at', 'updated_at', 'price'])
            delete_staging_table(client, PRODUCTS_STAGING_TABLE_ID)
        else:
            logger.info("No products to process.")

        # Fetch the final data from BigQuery to return to the frontend
        logger.info("Fetching processed data from BigQuery...")
        customers_data = fetch_bigquery_data(client, CUSTOMERS_MAIN_TABLE_ID)
        products_data = fetch_bigquery_data(client, PRODUCTS_MAIN_TABLE_ID)

        logger.info("Shopify to BigQuery pipeline completed successfully.")
        return customers_data, products_data
    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
        raise
