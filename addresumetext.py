import json
import re
from pymongo import MongoClient, errors

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
    try:
        # Parse input
        body = event["body"]
        if isinstance(body, str):
            body = body.strip()
        cleaned = re.sub(r'[\x00-\x1f]+', ' ', body)
        data = json.loads(cleaned)

        resume_id = data.get("resumeId")
        resume_text = data.get("resumeText")
        update_flag = data.get("update", 0)

        if not resume_id or not resume_text:
            return {
                "statusCode": 400,
                "body": json.dumps({"error": "Missing required fields: resumeId or resumeText"})
            }

        client = get_mongo_client()
        db = client["resumes_database"]
        collection = db["resume_text"]

        # Ensure unique index
        try:
            collection.create_index("resumeId", unique=True)
        except errors.OperationFailure:
            pass

        # Check for existing document
        existing_doc = collection.find_one({"resumeId": resume_id})

        if existing_doc:
            if update_flag == 1:
                # Delete and replace
                collection.delete_many({"resumeId": resume_id})
                collection.insert_one({
                    "resumeId": resume_id,
                    "resumeText": resume_text
                })
                return {
                    "statusCode": 200,
                    "body": json.dumps({"message": f"Updated resumeId: {resume_id}"})
                }
            else:
                return {
                    "statusCode": 409,
                    "body": json.dumps({"error": f"Duplicate resumeId: {resume_id}", "hint": "Use update=1 to overwrite"})
                }
        else:
            # New insert
            collection.insert_one({
                "resumeId": resume_id,
                "resumeText": resume_text
            })
            return {
                "statusCode": 200,
                "body": json.dumps({"message": f"Inserted resumeId: {resume_id}"})
            }

    except Exception as e:
        print(f"Lambda error: {str(e)}")
        return {
            "statusCode": 500,
            "body": json.dumps({"error": f"Internal server error: {str(e)}"})
        }
    finally:
        try:
            if 'client' in locals():
                client.close()
        except:
            pass
