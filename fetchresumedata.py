import json
from pymongo import MongoClient

def get_mongo_client():
    """Initialize and return MongoDB client."""
    return MongoClient(
        host="notify.pesuacademy.com",
        port=27017,
        username="admin",
        password="",
        authSource="admin"
    )

def lambda_handler(event, context):
    """Lambda function to retrieve resume details and matching jobs."""
    try:
        # Parse request body
        request_data = json.loads(event['body'])
        resume_id = request_data.get("resumeId")
        
        if not resume_id:
            return {"statusCode": 400, "body": json.dumps({"error": "Missing required 'resumeId'"})}
        
        # Connect to MongoDB
        mongo_client = get_mongo_client()
        db = mongo_client["resumes_database"]
        resume_collection = db["resumes"]
        resume_matches_collection = db["resume_matches"]
        
        # Fetch resume details excluding the "embedding" field
        resume = resume_collection.find_one({"resumeId": resume_id}, {"_id": 0, "embedding": 0})
        
        if not resume:
            return {"statusCode": 404, "body": json.dumps({"error": "Resume not found"})}
        
        # Fetch matching jobs
        matches = resume_matches_collection.find_one({"resumeId": resume_id}, {"_id": 0})
        
        response = {
            "resume": resume,
            "matches": matches.get("matches", []) if matches else []
        }
        
        return {"statusCode": 200, "body": json.dumps(response)}
    
    except Exception as e:
        return {"statusCode": 500, "body": json.dumps({"error": f"Internal server error: {str(e)}"})}
    
    finally:
        if 'mongo_client' in locals():
            mongo_client.close()
