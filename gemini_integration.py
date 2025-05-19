import os
import google.generativeai as genai
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def initialize_gemini():
    """Initialize the Gemini API client."""
    try:
        api_key = os.getenv('GEMINI_API_KEY')
        if not api_key:
            logger.error("GEMINI_API_KEY environment variable not set")
            return None
            
        genai.configure(api_key=api_key)
        logger.info("Gemini API initialized successfully")
        return True
    except Exception as e:
        logger.error(f"Failed to initialize Gemini API: {e}")
        return None

def analyze_data(prompt, data=None):
    """Analyze data using Gemini API."""
    try:
        if not initialize_gemini():
            return "Error: Gemini API not initialized. Please check your API key."
        
        # Configure the model
        model = genai.GenerativeModel('gemini-pro')
        
        # Prepare context with data if provided
        context = ""
        if data:
            if isinstance(data, dict):
                context = "Data context:\n"
                for key, value in data.items():
                    if isinstance(value, list) and len(value) > 0:
                        context += f"\n{key} (showing first 5 items):\n"
                        for i, item in enumerate(value[:5]):
                            context += f"  {i+1}. {item}\n"
                    else:
                        context += f"\n{key}: {value}\n"
            else:
                context = f"Data context:\n{str(data)[:1000]}...\n\n"
        
        # Combine context and prompt
        full_prompt = f"{context}\n\nBased on the above data, {prompt}"
        
        # Generate response
        response = model.generate_content(full_prompt)
        
        logger.info(f"Generated analysis for prompt: {prompt[:50]}...")
        return response.text
    except Exception as e:
        logger.error(f"Failed to analyze with Gemini: {e}")
        return f"Error analyzing data: {str(e)}"
