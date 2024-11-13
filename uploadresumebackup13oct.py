import json
import requests
from pymongo import MongoClient

# MongoDB connection details
mongo_uri = ""
api_key = ""

# Initialize MongoDB client
def get_mongo_client():
    return MongoClient(mongo_uri)

# Helper function to create embeddings using requests
def create_embedding(text):
    # Define the request payload
    data = {
        "input": text,
        "model": "text-embedding-3-large"
    }
    
    # Define the headers
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {api_key}"
    }
    
    # Make the request to the OpenAI API
    response = requests.post("https://api.openai.com/v1/embeddings", headers=headers, json=data)
    
    # Check if the response is successful
    if response.status_code == 200:
        response_data = response.json()
        if 'data' in response_data:
            # Extract the embedding
            embedding = response_data['data'][0]['embedding']
            return embedding
        else:
            raise ValueError("Error: 'data' field not found in response")
    else:
        raise ValueError(f"Error: {response.json()}")

# Lambda handler function
def lambda_handler(event, context):
    # Parse JSON input
    try:
        resume_data = json.loads(event['body'])
    except (json.JSONDecodeError, KeyError):
        return {
            "statusCode": 400,
            "body": json.dumps({"error": "Invalid JSON input"})
        }

    # Convert resume data to JSON string for embedding
    resume_text = json.dumps(resume_data)
    try:
        embedding = create_embedding(resume_text)
    except ValueError as e:
        return {
            "statusCode": 500,
            "body": json.dumps({"error": str(e)})
        }

    # Prepare document with embedding
    document = {
        **resume_data,
        "embedding": embedding
    }

    # Insert or update resume in MongoDB
    mongo_client = get_mongo_client()
    collection = mongo_client["resumes_database"]["resumes"]
    collection.update_one(
        {"resumeId": resume_data.get("resumeId")},
        {"$set": document},
        upsert=True
    )

    return {
        "statusCode": 200,
        "body": json.dumps({"message": "Resume stored successfully"})
    }
