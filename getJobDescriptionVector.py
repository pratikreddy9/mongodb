import json
import boto3
import requests
from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError, PyMongoError

# MongoDB PESU Academy EC2 connection details
host     = "notify.pesuacademy.com"
port     = 27017
username = "admin"
password = ""                     
auth_db  = "admin"
db_name  = "resumes_database"

# ───────────────────────────────────────────────────────────────────────
# MongoDB client
def get_mongo_client():
    return MongoClient(
        host=host,
        port=port,
        username=username,
        password=password,
        authSource=auth_db
    )

client     = get_mongo_client()
db         = client[db_name]
collection = db["job_description"]

# AWS Lambda client (to trigger resume-matching Lambda)
lambda_client = boto3.client('lambda')

# OpenAI API
api_key = ""                      # ← fill in prod key
# ───────────────────────────────────────────────────────────────────────


# ── DELETE-HELPER (for JD) ─────────────────────────────────────────────
def delete_jd_data(mongo_client, job_id):
    """Delete existing JD data from the three collections."""
    db_local                   = mongo_client[db_name]
    jd_collection              = db_local["job_description"]
    matches_collection         = db_local["matches"]
    resume_matches_collection  = db_local["resume_matches"]

    jd_collection.delete_many({"jobId": job_id})
    matches_collection.delete_many({"jobId": job_id})
    resume_matches_collection.update_many(
        {"matches.jobId": job_id},
        {"$pull": {"matches": {"jobId": job_id}}}
    )
# ───────────────────────────────────────────────────────────────────────


def trigger_processing_lambda(job_id, structured_jd):
    """Trigger the processing Lambda function asynchronously."""
    payload = {
        "jobId": job_id,
        "structured_query": structured_jd
    }
    lambda_client.invoke(
        FunctionName='getResumeScoreForJD',
        InvocationType='Event',        # async
        Payload=json.dumps(payload)
    )
    print(f"Processing Lambda triggered for jobId: {job_id}")
    return True


def create_embedding(text):
    """Generate embedding with text-embedding-3-large."""
    url     = "https://api.openai.com/v1/embeddings"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {api_key}"
    }
    data = {
        "input": text,
        "model": "text-embedding-3-large"
    }
    resp = requests.post(url, headers=headers, json=data)
    resp.raise_for_status()
    payload = resp.json()
    if "data" in payload:
        return payload["data"][0]["embedding"]
    raise ValueError("Error: 'data' field not found in response")


def format_job_description(jd_text):
    """Convert a natural-language JD to structured JSON (4 keys)."""
    url     = "https://api.openai.com/v1/chat/completions"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {api_key}"
    }

    example_jd_json = """
    {
        "educationalQualifications": [
            { "degree": "", "field": "", "graduationYear": 0, "institution": "" }
        ],
        "jobExperiences": [
            { "duration": "", "title": "" }
        ],
        "keywords": [ "" ],
        "skills": [
            { "skillId": null, "skillName": "" }
        ]
    }
    """

    data = {
        "model": "gpt-4o-mini",
        "messages": [
            {
                "role": "system",
                "content": (
                    "You are a helpful assistant that outputs valid JSON for job descriptions. "
                    "Extract only educationalQualifications, jobExperiences, keywords and skills.\n\n"
                    "Job Experience Rules: title in lowercase, no special chars; company lowercase; "
                    "duration positive int years, null if invalid (round partial years).\n\n"
                    "1. Keywords: dense list of tech terms (python, aws …), lower-case, no '-' '/'.\n"
                    "2. Skills: structured skill objects.\n"
                    "3. Output must match this JSON skeleton:\n" + example_jd_json
                )
            },
            {
                "role": "user",
                "content": (
                    f"Convert this job description to the JSON structure above:\n'''{jd_text}'''"
                )
            }
        ],
        "response_format": { "type": "json_object" }
    }

    resp = requests.post(url, headers=headers, json=data)
    resp.raise_for_status()
    payload = resp.json()
    if payload.get("choices"):
        return json.loads(payload["choices"][0]["message"]["content"])
    raise ValueError("Error: Invalid response from OpenAI")


def process_job_description(job_data):
    """Insert (or update) a JD, create embedding, and start matching."""
    job_id          = job_data.get("jobId")
    job_description = job_data.get("jobDescription")

    if not job_id or not job_description:
        return {
            "statusCode": 400,
            "body": json.dumps({"error": "jobId and jobDescription are required"})
        }

    # Update-flag handling (truthy values → 1)
    raw_flag    = job_data.get("update", 0)
    update_flag = 1 if str(raw_flag).lower() in ("1", "true", "yes") else 0
    if update_flag == 1:
        print(f"[update] Deleting existing data for jobId: {job_id}")
        delete_jd_data(client, job_id)

    try:
        structured_jd       = format_job_description(job_description)
        embedding           = create_embedding(json.dumps(structured_jd))

        document = {
            **job_data,
            "structured_query": structured_jd,
            "embedding"      : embedding,
            "processingState": "pending"
        }

        collection.insert_one(document)
        trigger_processing_lambda(job_id, structured_jd)

        return {
            "statusCode": 200,
            "body": json.dumps(
                {"message": "Job description stored successfully and processing started"}
            )
        }

    except DuplicateKeyError:
        return {
            "statusCode": 400,
            "body": json.dumps({"error": "Duplicate jobId - record already exists"})
        }
    except PyMongoError as e:
        return {
            "statusCode": 500,
            "body": json.dumps({"error": f"MongoDB error: {str(e)}"})
        }
    except ValueError as e:
        return {
            "statusCode": 500,
            "body": json.dumps({"error": str(e)})
        }


# Lambda entry-point
def lambda_handler(event, context):
    try:
        req_body = json.loads(event["body"])
        return process_job_description(req_body)
    except (KeyError, json.JSONDecodeError) as exc:
        return {
            "statusCode": 400,
            "body": json.dumps(
                {"error": "Invalid input format", "message": str(exc)}
            )
        }
