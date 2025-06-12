import json
import requests
from pymongo import MongoClient
from concurrent.futures import ThreadPoolExecutor, as_completed

# ========== CONFIGURATION ==========
TOP_N_MATCHES = 5          # legacy constant (no longer controls slicing)
BATCH_SIZE = 5             # resumes per OpenAI request

# -- NEW knobs --
CANDIDATES_TO_SCORE = 20   # number of resumes to send for aiScore
TOP_RESULTS_RETURNED = 5   # number of resumes returned to the caller
PARALLEL_WORKERS     = 4   # parallel OpenAI calls (4 × 5 = 20)

# MongoDB setup
def get_mongo_client():
    return MongoClient(
        host="notify.pesuacademy.com",
        port=27017,
        username="admin",
        password="",
        authSource="admin"
    )

# OpenAI setup
OPENAI_API_KEY = ""
OPENAI_MODEL = "gpt-4o"
OPENAI_URL = "https://api.openai.com/v1/chat/completions"

SYSTEM_PROMPT = """You are a helpful assistant skilled at evaluating resumes for a given job description. 
Your task is to evaluate each resume against the job description independently and assign an aiScore from 0 to 100, representing how well the resume aligns with the JD.

You must give a score to every resume individually, not in comparison to others. each candidate is diffrent.

Output format must be a JSON object with a key named 'result' which holds a list of resume objects, like this:

{
  "result": [
    {
      "resumeId": "RES123",
      "aiScore": 87
    }
  ]
}
"""

# ✅ Safe country normalization
def safe_normalize_country(value):
    if not isinstance(value, str):
        return ""
    return value.strip().lower()

def call_openai(jd_text, resumes_batch):
    formatted_resumes = []
    for resume in resumes_batch:
        resume_id = resume.get("resumeId")
        if not resume_id:
            continue

        if resume.get("resumeText"):
            print(f"📝 Using TEXT for resumeId: {resume_id}")
            formatted_resumes.append(f'### Resume ID: {resume_id} ###\n"""\n{resume["resumeText"]}\n"""')
        else:
            print(f"📄 Using JSON for resumeId: {resume_id}")
            formatted_resumes.append(json.dumps(resume, indent=2))

    user_prompt = f"""Here is the job description:
\"\"\"{jd_text}\"\"\"

Here are the resumes:
{chr(10).join(formatted_resumes)}

Evaluate each resume individually and return only JSON in the exact format described above.
"""

    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {OPENAI_API_KEY}"
    }

    payload = {
        "model": OPENAI_MODEL,
        "response_format": {"type": "json_object"},
        "messages": [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": user_prompt}
        ]
    }

    try:
        print("Calling OpenAI API for aiScore evaluation")
        response = requests.post(OPENAI_URL, headers=headers, json=payload)
        print("OpenAI response status code:", response.status_code)
        response.raise_for_status()

        response_json = response.json()
        print("Full OpenAI JSON response:", json.dumps(response_json, indent=2))

        content = response_json["choices"][0]["message"]["content"]
        print("OpenAI content string:", content)

        parsed = json.loads(content)

        if not isinstance(parsed, dict) or "result" not in parsed:
            print("OpenAI JSON object does not contain 'result' key")
            return None

        result_list = parsed["result"]
        if not isinstance(result_list, list):
            print("'result' key is not a list")
            return None

        scores = {
            entry["resumeId"]: entry["aiScore"]
            for entry in result_list
            if isinstance(entry, dict) and "resumeId" in entry and "aiScore" in entry
        }

        print("✅ Parsed aiScore map:", scores)
        return scores

    except Exception as e:
        print("OpenAI call failed:", str(e))
        import traceback
        traceback.print_exc()
        return None

def lambda_handler(event, context):
    mongo_client = get_mongo_client()
    try:
        print("Parsing request and extracting jobId and filters")
        request_data = json.loads(event['body'])
        jd_id = request_data.get("jobId")
        filter_keywords = request_data.get("filterKeywords", [])
        region_id = request_data.get("regionId")

        if not jd_id:
            return {"statusCode": 400, "body": json.dumps({"error": "Missing required 'jobId'"})}

        db = mongo_client["resumes_database"]
        jd_collection = db["job_description"]
        matches_collection = db["matches"]
        resumes_collection = db["resumes"]

        print("Fetching job description from DB")
        jd = jd_collection.find_one({"jobId": jd_id}, {"_id": 0, "embedding": 0})
        if not jd:
            return {"statusCode": 404, "body": json.dumps({"error": "Job description not found"})}
        jd_text = jd.get("jobDescription", "")

        print("Fetching all matches from DB")
        match_doc = matches_collection.find_one({"jobId": jd_id}, {"_id": 0})
        all_matches = match_doc.get("matches", []) if match_doc else []

        if filter_keywords:
            print("Filtering matches by keywords")
            all_matches = [
                m for m in all_matches
                if set(filter_keywords).issubset(set(m.get("commonKeys", [])))
            ]

        region_id_to_countries = {
            "0966bbc7-8d15-11ef-a224-000c29dc611c": ["Australia"],
            "2039bca5-8d14-11ef-a224-000c29dc611c": ["United Arab Emirates", "Uae"],
            "2853b6af-04af-11f0-b74e-52540e737e83": ["Hong Kong", "Hong Kong Sar"],
            "28820f8a-04af-11f0-b74e-52540e737e83": ["Japan"],
            "28cccee1-04af-11f0-b74e-52540e737e83": ["Germany"],
            "29ef8adf-8d16-11ef-a224-000c29dc611c": ["Saudi Arabia", "Ksa"],
            "3e58491b-8d13-11ef-a224-000c29dc611c": ["Philippines", "The Philippines"],
            "6f48aadb-08a0-11f0-a380-5254828ec570": ["Singapore"],
            "7c1f71d7-8d25-11ef-a224-000c29dc611c": ["Thailand"],
            "9a7be17b-8d15-11ef-a224-000c29dc611c": ["New Zealand"],
            "9e21a39d-d1a6-4aca-bfc2-4a241d4cbec8": ["India", "Ind"],
            "a227528b-8d13-11ef-a224-000c29dc611c": ["Malaysia"],
            "ba184d1f-8d14-11ef-a224-000c29dc611c": ["United States", "Usa", "Us"],
            "c7c45e99-ff53-42e1-981b-3ba1f0794b24": ["Indonesia"],
            "e573ba69-2886-11ef-b4be-000c29dc611c": ["Vietnam", "Viet Nam", "Vn", "Vietnamese"]
        }

        if region_id and region_id in region_id_to_countries:
            print("Filtering matches by regionId")
            valid_countries = set(safe_normalize_country(c) for c in region_id_to_countries[region_id])
            all_matches = [
                m for m in all_matches
                if safe_normalize_country(m.get("country")) in valid_countries
            ]

        print("Sorting and selecting top candidates")
        def has_high_match_score(exp_list):
            for exp in exp_list:
                try:
                    if float(exp.get("matchScore", 0)) >= 0.9:
                        return True
                except:
                    continue
            return False

        def calculate_valid_experience(exp_list):
            total = 0
            for exp in exp_list:
                try:
                    dur = int(exp.get("resumeDuration"))
                    if dur > 0:
                        total += dur
                except:
                    continue
            return total

        group_a = [m for m in all_matches if has_high_match_score(m.get("commonExperiences", []))]
        group_b = [m for m in all_matches if not has_high_match_score(m.get("commonExperiences", []))]

        group_a_sorted = sorted(group_a, key=lambda x: calculate_valid_experience(x.get("commonExperiences", [])), reverse=True)
        group_b_sorted = sorted(group_b, key=lambda x: x.get("similarityScore", 0), reverse=True)

        sorted_matches   = group_a_sorted + group_b_sorted
        top_candidates   = sorted_matches[:CANDIDATES_TO_SCORE]   # 20 → AI

        print("Checking which of the 20 need aiScore")
        to_score = [m for m in top_candidates if "aiScore" not in m]

        if to_score:
            print(f"{len(to_score)} resumes missing aiScore. Processing in batches of {BATCH_SIZE}")
            resume_ids_needed = [m["resumeId"] for m in to_score]

            resume_docs = list(resumes_collection.find(
                {"resumeId": {"$in": resume_ids_needed}},
                {"_id": 0, "embedding": 0}
            ))

            resume_text_collection = db["resume_text"]
            resume_text_map = {
                doc["resumeId"]: doc.get("resumeText")
                for doc in resume_text_collection.find(
                    {"resumeId": {"$in": resume_ids_needed}},
                    {"_id": 0, "resumeId": 1, "resumeText": 1}
                )
            }

            for r in resume_docs:
                r["resumeId"] = r.get("resumeId")
                r["resumeText"] = resume_text_map.get(r["resumeId"])

            batches = [
                resume_docs[i:i+BATCH_SIZE]
                for i in range(0, len(resume_docs), BATCH_SIZE)
            ]

            scores_map = {}
            with ThreadPoolExecutor(max_workers=PARALLEL_WORKERS) as pool:
                futures = {pool.submit(call_openai, jd_text, b): b for b in batches}
                for fut in as_completed(futures):
                    sc = fut.result() or {}
                    scores_map.update(sc)

            print("Updating MongoDB with aiScores")
            for resume_id, ai_score in scores_map.items():
                matches_collection.update_one(
                    {"jobId": jd_id, "matches.resumeId": resume_id},
                    {"$set": {"matches.$.aiScore": ai_score}}
                )
        else:
            print("All 20 already have aiScore")

        print("Refetching updated matches")
        updated_doc = matches_collection.find_one({"jobId": jd_id}, {"_id": 0})
        updated_matches = updated_doc.get("matches", []) if updated_doc else []

        if filter_keywords:
            updated_matches = [
                m for m in updated_matches
                if set(filter_keywords).issubset(set(m.get("commonKeys", [])))
            ]
        if region_id and region_id in region_id_to_countries:
            valid_countries = set(safe_normalize_country(c) for c in region_id_to_countries[region_id])
            updated_matches = [
                m for m in updated_matches
                if safe_normalize_country(m.get("country")) in valid_countries
            ]

        updated_sorted = sorted(
            updated_matches,
            key=lambda x: (x.get("aiScore", 0), x.get("similarityScore", 0)),
            reverse=True
        )
        final_matches = updated_sorted[:TOP_RESULTS_RETURNED]

        # ➕ Add rank field
        for idx, match in enumerate(final_matches, start=1):
            match["rank"] = idx

        print("Returning final job description and matches")
        return {
            "statusCode": 200,
            "body": json.dumps({
                "jobDescription": jd,
                "matches": final_matches
            })
        }

    except Exception as e:
        print("Error occurred:", str(e))
        import traceback
        traceback.print_exc()
        return {
            "statusCode": 500,
            "body": json.dumps({"error": f"Internal server error: {str(e)}"})
        }
    finally:
        mongo_client.close()
        print("MongoDB connection closed")
