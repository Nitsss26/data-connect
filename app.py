from flask import Flask, request, jsonify
from flask_cors import CORS
import os
import json
from shopify_to_bigquery import run_pipeline
from gemini_integration import analyze_data
import logging
import time

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

app = Flask(__name__)
CORS(app)

@app.route('/api/connectors/shopify', methods=['POST'])
def connect_shopify():
    try:
        data = request.json
        shop_name = data.get('shopName')
        api_token = data.get('apiToken')
        if not shop_name or not api_token:
            return jsonify({"success": False, "message": "Shopify shop name and API token are required."}), 400
        
        # Set environment variables for the Shopify connector
        os.environ['SHOPIFY_SHOP_NAME'] = shop_name
        os.environ['SHOPIFY_API_TOKEN'] = api_token
        
        logger.info(f"Connected to Shopify store: {shop_name}")
        return jsonify({"success": True, "message": "Shopify connected successfully"})
    except Exception as e:
        logger.error(f"Failed to connect Shopify: {str(e)}")
        return jsonify({"success": False, "message": f"Failed to connect Shopify: {str(e)}"}), 500

@app.route('/api/connectors/bigquery', methods=['POST'])
def connect_bigquery():
    try:
        project_id = request.form.get('projectId')
        dataset_id = request.form.get('datasetId', 'shopify_data')
        credentials_file = request.files.get('credentials')
        
        if not project_id or not credentials_file:
            return jsonify({"success": False, "message": "GCP project ID and credentials file are required."}), 400
        
        # Save the credentials file
        credentials_path = os.path.join(os.path.dirname(__file__), 'google_credentials.json')
        credentials_file.save(credentials_path)
        
        # Set environment variables for BigQuery
        os.environ['GCP_PROJECT_ID'] = project_id
        os.environ['GOOGLE_CREDENTIALS_PATH'] = credentials_path
        os.environ['BIGQUERY_DATASET_ID'] = dataset_id
        
        logger.info(f"Connected to BigQuery project: {project_id}, dataset: {dataset_id}")
        return jsonify({"success": True, "message": "BigQuery connected successfully"})
    except Exception as e:
        logger.error(f"Failed to connect BigQuery: {str(e)}")
        return jsonify({"success": False, "message": f"Failed to connect BigQuery: {str(e)}"}), 500

@app.route('/api/pipelines/run', methods=['POST'])
def run_pipeline_endpoint():
    try:
        # Check if environment variables are set
        shopify_shop_name = os.getenv('SHOPIFY_SHOP_NAME')
        shopify_api_token = os.getenv('SHOPIFY_API_TOKEN')
        gcp_project_id = os.getenv('GCP_PROJECT_ID')
        credentials_path = os.getenv('GOOGLE_CREDENTIALS_PATH')
        dataset_id = os.getenv('BIGQUERY_DATASET_ID', 'shopify_data')

        if not all([shopify_shop_name, shopify_api_token, gcp_project_id, credentials_path]):
            missing = [var for var, val in [
                ('SHOPIFY_SHOP_NAME', shopify_shop_name),
                ('SHOPIFY_API_TOKEN', shopify_api_token),
                ('GCP_PROJECT_ID', gcp_project_id),
                ('GOOGLE_CREDENTIALS_PATH', credentials_path)
            ] if not val]
            return jsonify({"success": False, "message": f"Missing environment variables: {', '.join(missing)}"}), 400

        # Get pipeline ID if provided
        data = request.json
        pipeline_id = data.get('pipelineId') if data else None
        
        logger.info(f"Running pipeline {pipeline_id if pipeline_id else 'all'}")
        
        # Run the pipeline
        customers_data, products_data = run_pipeline(
            shopify_shop_name,
            shopify_api_token,
            gcp_project_id,
            credentials_path,
            dataset_id
        )
        
        return jsonify({
            "success": True,
            "message": "Pipeline executed successfully",
            "customers": customers_data,
            "products": products_data
        })
    except Exception as e:
        logger.error(f"Pipeline failed: {str(e)}")
        return jsonify({"success": False, "message": f"Pipeline failed: {str(e)}"}), 500

@app.route('/api/gemini/analyze', methods=['POST'])
def analyze_with_gemini():
    try:
        data = request.json
        prompt = data.get('prompt')
        
        if not prompt:
            return jsonify({"success": False, "message": "Prompt is required."}), 400
        
        # Check if Gemini API key is set
        gemini_api_key = os.getenv('GEMINI_API_KEY')
        if not gemini_api_key:
            return jsonify({
                "success": False, 
                "message": "Gemini API key not configured. Please set the GEMINI_API_KEY environment variable."
            }), 400
        
        # Get data context if available
        shopify_shop_name = os.getenv('SHOPIFY_SHOP_NAME')
        gcp_project_id = os.getenv('GCP_PROJECT_ID')
        credentials_path = os.getenv('GOOGLE_CREDENTIALS_PATH')
        dataset_id = os.getenv('BIGQUERY_DATASET_ID', 'shopify_data')
        
        data_context = None
        if all([shopify_shop_name, gcp_project_id, credentials_path]):
            try:
                # Try to get some data for context
                customers_data, products_data = run_pipeline(
                    shopify_shop_name,
                    os.getenv('SHOPIFY_API_TOKEN'),
                    gcp_project_id,
                    credentials_path,
                    dataset_id
                )
                data_context = {
                    "customers": customers_data[:10],  # Limit to 10 items for context
                    "products": products_data[:10]
                }
            except Exception as e:
                logger.warning(f"Could not fetch data for Gemini context: {e}")
        
        # Analyze with Gemini
        response = analyze_data(prompt, data_context)
        
        logger.info(f"Gemini analysis completed for prompt: {prompt[:50]}...")
        return jsonify({"success": True, "response": response})
    except Exception as e:
        logger.error(f"Gemini analysis failed: {str(e)}")
        return jsonify({"success": False, "message": f"Analysis failed: {str(e)}"}), 500

@app.route('/api/logs', methods=['GET'])
def get_logs():
    """Return recent logs for the frontend."""
    try:
        # In a real app, you would fetch logs from a log file or database
        # For now, we'll return mock logs
        mock_logs = [
            f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] INFO: API server started",
            f"[{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time() - 60))}] INFO: Connected to Shopify API",
            f"[{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time() - 120))}] INFO: Connected to BigQuery",
            f"[{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time() - 180))}] INFO: Pipeline execution started",
            f"[{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time() - 240))}] INFO: Fetched 250 customer records",
            f"[{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time() - 300))}] INFO: Inserted data into BigQuery",
            f"[{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time() - 360))}] INFO: Pipeline execution completed"
        ]
        
        return jsonify({"success": True, "logs": mock_logs})
    except Exception as e:
        logger.error(f"Failed to fetch logs: {str(e)}")
        return jsonify({"success": False, "message": f"Failed to fetch logs: {str(e)}"}), 500

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)
