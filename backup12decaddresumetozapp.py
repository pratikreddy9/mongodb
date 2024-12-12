import json
import requests
from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError


# MongoDB connection details
mongo_uri = "mongodb+srv://pratiksr:0Qg@zappresume.pcwoy.mongodb.net/?retryWrites=true&w=majority&appName=zappResume"
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

    # Check if 'resumeId' is present
    if "resumeId" not in resume_data:
        return {
            "statusCode": 400,
            "body": json.dumps({"error": "Missing required 'resumeId'"})
        }

    # List of expected keys, including optional ones
    expected_keys = ["name", "email", "contactNo", "address", "educationalQualifications", 
                     "jobExperiences", "keywords", "skills"]

    # Identify missing keys
    missing_keys = [key for key in expected_keys if key not in resume_data]

    # Extract relevant fields for embedding, defaulting to empty lists if missing
    educational_qualifications = json.dumps(resume_data.get("educationalQualifications", []))
    job_experiences = json.dumps(resume_data.get("jobExperiences", []))
    keywords = json.dumps(resume_data.get("keywords", []))
    skills = json.dumps(resume_data.get("skills", []))
    
    # Concatenate relevant fields to create a single text for embedding
    embedding_text = f"{educational_qualifications} {job_experiences} {keywords} {skills}"
    
    try:
        embedding = create_embedding(embedding_text)
    except ValueError as e:
        return {
            "statusCode": 500,
            "body": json.dumps({"error": str(e)})
        }

    # Prepare document with all original keys and embedding
    document = {
        **resume_data,  # Include all original keys (optional or not)
        "embedding": embedding  # Add generated embedding
    }

    # Insert document into MongoDB, letting MongoDB enforce unique constraint on `resumeId`
    mongo_client = get_mongo_client()
    collection = mongo_client["resumes_database"]["resumes"]

    try:
        collection.insert_one(document)
    except DuplicateKeyError:
        # Handle duplicate error when `resumeId` already exists
        return {
            "statusCode": 400,
            "body": json.dumps({"error": "Duplicate resumeId - record already exists"})
        }

    # Construct success message including missing keys
    response_message = {
        "message": "Resume stored successfully",
        "missing_keys": missing_keys
    }

    return {
        "statusCode": 200,
        "body": json.dumps(response_message)
    }
