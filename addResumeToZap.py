import json
import requests
from datetime import datetime
import math
from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError
from collections import Counter
import difflib

# MongoDB connection details
host = "notify.pesuacademy.com"
port = 27017
username = "admin"
password = ""
auth_db = "admin"
db_name = "resumes_database"
api_key = ""

def get_mongo_client():
    """Initialize and return MongoDB client."""
    return MongoClient(
        host=host,
        port=port,
        username=username,
        password=password,
        authSource=auth_db
    )

def create_embedding(text):
    """Create embeddings using OpenAI API."""
    data = {
        "input": text,
        "model": "text-embedding-3-large"
    }

    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {api_key}"
    }

    response = requests.post("https://api.openai.com/v1/embeddings", headers=headers, json=data)

    if response.status_code == 200:
        response_data = response.json()
        if 'data' in response_data:
            return response_data['data'][0]['embedding']
        else:
            raise ValueError("Error: 'data' field not found in response")
    else:
        raise ValueError(f"Error: {response.json()}")

def get_common_keywords(keywords1, keywords2):
    """Find common keywords between two lists."""
    return list(set(keywords1) & set(keywords2))

def calculate_cosine_similarity(vec1, vec2):
    """Calculate cosine similarity between two vectors using pure Python."""
    if len(vec1) != len(vec2):
        raise ValueError("Vectors must have the same length")

    dot_product = sum(a * b for a, b in zip(vec1, vec2))
    norm1 = math.sqrt(sum(a * a for a in vec1))
    norm2 = math.sqrt(sum(b * b for b in vec2))

    if norm1 == 0 or norm2 == 0:
        return 0.0

    return dot_product / (norm1 * norm2)

def get_common_experiences(resume_exps, jd_exps):
    result = []
    try:
        if not isinstance(resume_exps, list) or not isinstance(jd_exps, list):
            return result
        for r_exp in resume_exps:
            r_title = str(r_exp.get("title", "")).lower().strip()
            r_dur = r_exp.get("duration")
            if not r_title:
                continue
            for j_exp in jd_exps:
                j_title = str(j_exp.get("title", "")).lower().strip()
                j_dur = j_exp.get("duration")
                if not j_title:
                    continue
                score = difflib.SequenceMatcher(None, r_title, j_title).ratio()
                if score > 0.85:
                    result.append({
                        "resumeTitle": r_title,
                        "jdTitle": j_title,
                        "resumeDuration": r_dur,
                        "jdDuration": j_dur,
                        "matchScore": round(score, 2)
                    })
        return result
    except Exception as e:
        print(f"Experience match error: {str(e)}")
        return result

def process_resume_matches(mongo_client, resume_id):
    """Process matches for a single resume and correctly add to `matches` collection."""
    db = mongo_client[db_name]
    resume_collection = db["resumes"]
    jd_collection = db["job_description"]
    resume_matches_collection = db["resume_matches"]
    matches_collection = db["matches"]

    resume = resume_collection.find_one({"resumeId": resume_id})
    if not resume:
        raise ValueError(f"Resume with ID {resume_id} not found")

    resume_keywords = [skill.get("skillName") for skill in resume.get("skills", [])] + resume.get("keywords", [])
    resume_embedding = resume.get("embedding", [])
    resume_experiences = resume.get("jobExperiences", [])
    matches = []

    for jd in jd_collection.find():
        jd_id = jd["jobId"]
        jd_keywords = jd.get("structured_query", {}).get("keywords", [])
        jd_embedding = jd.get("embedding", [])
        jd_experiences = jd.get("structured_query", {}).get("jobExperiences", [])
        common_keys = get_common_keywords(jd_keywords, resume_keywords)
        common_experiences = get_common_experiences(resume_experiences, jd_experiences)

        if len(common_keys) >= 1:
            try:
                similarity_score = calculate_cosine_similarity(
                    resume_embedding,
                    jd_embedding
                )

                resume_match = {
                    "resumeId": resume_id,
                    "name": resume.get("name"),
                    "email": resume.get("email"),
                    "contactNo": resume.get("contactNo"),
                    "address": resume.get("address"),
                    "city": resume.get("city"),
                    "state": resume.get("state"),
                    "country": resume.get("country"),
                    "createdOn": resume.get("createdOn"),
                    "ownedBy": resume.get("ownedBy"),
                    "noticePeriod": resume.get("noticePeriod"),
                    "expectedCTC": resume.get("expectedCTC"),
                    "totalExperience": resume.get("totalExperience"),
                    "commonKeys": common_keys,
                    "similarityScore": similarity_score,
                    "commonExperiences": common_experiences
                }

                matches_collection.update_one(
                    {"jobId": jd_id},
                    {"$push": {"matches": resume_match}},
                    upsert=True
                )

                matches.append({
                    "jobId": jd_id,
                    "jobDescription": jd.get("jobDescription", ""),
                    "commonKeys": common_keys,
                    "similarityScore": similarity_score,
                    "commonExperiences": common_experiences
                })

            except Exception:
                continue

    resume_matches_collection.replace_one(
        {"resumeId": resume_id},
        {"resumeId": resume_id, "matches": matches},
        upsert=True
    )

    resume_collection.update_one(
        {"resumeId": resume_id},
        {"$set": {"processingState": "completed"}}
    )

    print(f"Resume {resume_id} processed. Matches created: {len(matches)}")
    return len(matches)

def delete_resume_data(mongo_client, resume_id):
    """Delete existing resume data from 3 collections."""
    db = mongo_client[db_name]
    resumes_collection = db["resumes"]
    resume_matches_collection = db["resume_matches"]
    matches_collection = db["matches"]

    resumes_collection.delete_many({"resumeId": resume_id})
    resume_matches_collection.delete_many({"resumeId": resume_id})
    matches_collection.update_many(
        {"matches.resumeId": resume_id},
        {"$pull": {"matches": {"resumeId": resume_id}}}
    )

def lambda_handler(event, context):
    """Main Lambda handler function."""
    try:
        resume_data = json.loads(event['body'])

        if "resumeId" not in resume_data:
            return {"statusCode": 400, "body": json.dumps({"error": "Missing required 'resumeId'"})}

        mongo_client = get_mongo_client()

        if resume_data.get("update") == 1:
            delete_resume_data(mongo_client, resume_data["resumeId"])
        elif resume_data.get("trigger") not in [None, 0]:
            return {"statusCode": 400, "body": json.dumps({"error": "Invalid update value. Use 0 or 1."})}

        collection = mongo_client[db_name]["resumes"]

        if collection.find_one({"resumeId": resume_data["resumeId"]}):
            return {"statusCode": 400, "body": json.dumps({"error": "Duplicate resumeId - record already exists"})}

        all_possible_keys = [
            "name", "email", "contactNo", "address", "city", "state", "country",
            "createdOn", "ownedBy", "noticePeriod", "expectedCTC", "totalExperience",
            "educationalQualifications", "jobExperiences", "keywords", "skills"
        ]
        missing_keys = [key for key in all_possible_keys if key not in resume_data]

        # Calculate totalExperience from jobExperiences
        total_experience = 0
        job_exps = resume_data.get("jobExperiences", [])
        if isinstance(job_exps, list):
            for exp in job_exps:
                dur = exp.get("duration")
                if dur is None:
                    continue
                try:
                    dur_str = str(dur).strip()
                    if dur_str == "":
                        continue
                    dur_val = float(dur_str)
                    if dur_val > 0:
                        total_experience += int(dur_val)
                except (ValueError, TypeError):
                    continue
        resume_data["totalExperience"] = total_experience

        embedding_text = f"{json.dumps(resume_data.get('educationalQualifications', []))} " \
                         f"{json.dumps(resume_data.get('jobExperiences', []))} " \
                         f"{json.dumps(resume_data.get('keywords', []))} " \
                         f"{json.dumps(resume_data.get('skills', []))}"

        try:
            embedding = create_embedding(embedding_text)
        except ValueError as e:
            return {"statusCode": 500, "body": json.dumps({"error": str(e)})}

        document = {**resume_data, "embedding": embedding, "processingState": "pending"}

        try:
            collection.insert_one(document)
        except DuplicateKeyError:
            return {"statusCode": 400, "body": json.dumps({"error": "Duplicate resumeId - record already exists"})}

        try:
            num_matches = process_resume_matches(mongo_client, resume_data["resumeId"])
        except Exception as e:
            return {"statusCode": 200, "body": json.dumps({
                "message": "Resume stored but matching failed",
                "missing_keys": missing_keys,
                "matching_error": str(e)
            })}

        db = mongo_client[db_name]
        jobs_matched = db["matches"].count_documents({"matches.resumeId": resume_data["resumeId"]})

        return {
            "statusCode": 200,
            "body": json.dumps({
                "message": "Resume processed successfully",
                "collections_status": {
                    "resume_matches": {"total_jobs_matched": num_matches},
                    "matches": {"added_to_jobs_count": jobs_matched}
                },
                "missing_keys": missing_keys
            })
        }

    except Exception as e:
        return {"statusCode": 500, "body": json.dumps({"error": f"Internal server error: {str(e)}"})}

    finally:
        if 'mongo_client' in locals():
            mongo_client.close()
