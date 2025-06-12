#!/usr/bin/env python3
"""
fetchjddata.py - Recency-First Approach
────────────────────────────────────────────────────────────────────────────
Returns the top N resumes for a given JD, ranked PRIMARILY by recency, 
then by other criteria like keyword match count and AI score.
"""

import json
import requests
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
from pymongo import MongoClient

# ╭─── CONFIG ───────────────────────────────────────────────────────────╮
BATCH_SIZE               = 1   # resumes per OpenAI request (changed to 1)
CANDIDATES_TO_SCORE      = 20  # number of resumes sent to OpenAI
TOP_RESULTS_RETURNED     = 5   # final resumes returned
PARALLEL_WORKERS         = 4   # parallel OpenAI calls

OPENAI_API_KEY           = ""  # Add your API key
OPENAI_MODEL             = "gpt-4o"
OPENAI_URL               = "https://api.openai.com/v1/chat/completions"

SYSTEM_PROMPT = """ATS Resume Evaluation Prompt
You are an expert ATS (Applicant Tracking System) assistant skilled at evaluating resumes for a given job description. Your task is to evaluate each resume against the job description independently and assign an aiScore from 0 to 100, representing how well the resume aligns with the JD.

For each resume evaluation, assess these key parameters:

Skills match (weighted 30%)
Required skills (20%): How many required skills appear in the resume?
Preferred skills (10%): How many preferred skills appear in the resume?
Experience relevance (weighted 25%)
Relevance of previous roles to the position (15%)
Years of experience in similar roles (10%)
Compensation fit (weighted 15%)
First prioritize skills and experience match
For qualified candidates, evaluate expected CTC vs. budget using sliding scale:
Within budget: Full points
0-10% above budget: Slight reduction
10-20% above budget: Moderate reduction
20% above budget: Larger reduction

Flag highly qualified candidates (85%+ on skills/experience) as "High-Value Talent" even if above budget
Location compatibility (weighted 15%)
Exact location match (15%)
Relocation willingness if location doesn't match (7%)
Remote work compatibility if applicable (10%)
Education fit (weighted 10%)
Required degrees/certifications present
Relevant field of study
Availability & notice period (weighted 5%)
Match between candidate's availability and position's start date requirement
You must give a score to every resume individually, not in comparison to others. Each candidate is different. When calculating the final aiScore, round to the nearest whole number.

Output format must be a JSON object with a key named 'result' which holds a list of resume objects, like this:

{
  "result": [
    {
      "resumeId": "RES123",
      "aiScore": 87,
      "keyMatchPoints": ["Top 3 strongest qualification matches"],
      "compensationFit": "Within budget/Above budget by X%/Below budget by X%",
      "locationStatus": "Match/Remote possible/Relocation required",
      "availabilityMatch": "Immediate/X weeks notice period",
      "hiringRecommendation": "Strong match within budget/Exceptional talent above budget - consider negotiation/Budget match but minimum qualifications"
    }
  ]
}

If any critical information is missing from either the job description or resume, note this in the evaluation as Null and score based on available information. Do not mix up the details between resumes and keep strictly as Null for missing info."""

def get_mongo_client():
    return MongoClient(
        host="notify.pesuacademy.com",
        port=27017,
        username="admin",
        password="",
        authSource="admin"
    )
# ╰──────────────────────────────────────────────────────────────────────╯

# ─── Helpers ────────────────────────────────────────────────────────────
MIN_DT = datetime.min.replace(tzinfo=timezone.utc)

def parse_created_on(val):
    """
    FIXED: Normalise Int64 epoch-ms or ISO-8601 string → tz-aware datetime.
    Now handles epoch timestamps stored as strings!
    """
    if val is None:
        return MIN_DT
    
    # Handle numeric values (int/float) - your Int64 type
    if isinstance(val, (int, float)):
        try:
            return datetime.fromtimestamp(val / 1000, tz=timezone.utc)
        except Exception:
            return MIN_DT
    
    # Handle string values - your str type
    if isinstance(val, str):
        # First, try to parse as ISO-8601 string
        try:
            return datetime.fromisoformat(val.replace("Z", "+00:00"))
        except Exception:
            pass
        
        # If ISO parsing fails, try parsing as epoch milliseconds string
        try:
            epoch_ms = int(val)
            return datetime.fromtimestamp(epoch_ms / 1000, tz=timezone.utc)
        except (ValueError, TypeError, OSError):
            pass
    
    return MIN_DT

def safe_normalize_country(value):
    return value.strip().lower() if isinstance(value, str) else ""

def count_keywords(resume, jd_keywords):
    """Count how many of the JD keywords are present in the resume's commonKeys"""
    if not jd_keywords:
        return 0
    
    resume_keywords = set(k.lower() for k in resume.get("commonKeys", []))
    
    return len(resume_keywords)  # Just return the count of keywords

# ─── OpenAI call ────────────────────────────────────────────────────────
def call_openai(jd_text, resumes_batch):
    # Since BATCH_SIZE is 1, this will always be a single resume
    resume = resumes_batch[0]
    rid = resume.get("resumeId")
    if not rid:
        return {}
    
    if resume.get("resumeText"):
        formatted_resume = f'### Resume ID: {rid} ###\n"""\n{resume["resumeText"]}\n"""'
    else:
        formatted_resume = json.dumps(resume, indent=2)

    user_prompt = f"""Here is the job description:
\"\"\"{jd_text}\"\"\"

Here is the resume:
{formatted_resume}

Evaluate this resume individually and return only JSON in the exact format described above."""

    payload = {
        "model": OPENAI_MODEL,
        "response_format": {"type": "json_object"},
        "messages": [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": user_prompt}
        ]
    }

    try:
        resp = requests.post(
            OPENAI_URL,
            headers={
                "Content-Type": "application/json",
                "Authorization": f"Bearer {OPENAI_API_KEY}"
            },
            json=payload,
            timeout=120,
        )
        resp.raise_for_status()
        content = resp.json()["choices"][0]["message"]["content"]
        parsed = json.loads(content)
        
        result_list = parsed.get("result", [])
        if result_list and isinstance(result_list[0], dict):
            result_item = result_list[0]
            return {
                result_item.get("resumeId", rid): {
                    "aiScore": result_item.get("aiScore"),
                    "keyMatchPoints": result_item.get("keyMatchPoints"),
                    "compensationFit": result_item.get("compensationFit"),
                    "locationStatus": result_item.get("locationStatus"),
                    "availabilityMatch": result_item.get("availabilityMatch"),
                    "hiringRecommendation": result_item.get("hiringRecommendation")
                }
            }
        return {}
    except Exception as e:
        print(f"OpenAI API error: {e}")
        return {}

# ─── Lambda entry ───────────────────────────────────────────────────────
def lambda_handler(event, context):
    client = get_mongo_client()
    try:
        print("Processing request...")
        req  = json.loads(event["body"])
        jd_id  = req.get("jobId")
        kw_flt = req.get("filterKeywords", [])
        region = req.get("regionId")

        if not jd_id:
            return {"statusCode": 400,
                    "body": json.dumps({"error": "Missing 'jobId'"})}

        db   = client["resumes_database"]
        jd   = db["job_description"].find_one({"jobId": jd_id},
                                              {"_id": 0, "embedding": 0})
        if not jd:
            return {"statusCode": 404,
                    "body": json.dumps({"error": "Job description not found"})}
        jd_text = jd.get("jobDescription", "")
        jd_keywords = jd.get("structured_query", {}).get("keywords", [])

        match_doc = db["matches"].find_one({"jobId": jd_id}, {"_id": 0})
        matches_all = match_doc.get("matches", []) if match_doc else []
        print(f"Found {len(matches_all)} initial matches")

        # ── Apply filters ─────────────────────────────────────────────
        if kw_flt:
            print(f"Applying keyword filter: {kw_flt}")
            kw_set = set(kw_flt)
            matches_all = [
                m for m in matches_all
                if kw_set.issubset(set(m.get("commonKeys", [])))
            ]
            print(f"After keyword filter: {len(matches_all)} matches")

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
            "e573ba69-2886-11ef-b4be-000c29dc611c": ["Vietnam", "Viet Nam", "Vn", "Vietnamese"],
        }
        if region and region in region_id_to_countries:
            print(f"Applying region filter: {region}")
            valid = {safe_normalize_country(c) for c in region_id_to_countries[region]}
            matches_all = [
                m for m in matches_all
                if safe_normalize_country(m.get("country")) in valid
            ]
            print(f"After region filter: {len(matches_all)} matches")

        # ── Initial selection for AI scoring ───────────────────────────
        print("Initial sort by creation date (newest first)")
        for m in matches_all:
            m["_parsed_date"] = parse_created_on(m.get("createdOn"))
        
        # Sort just by creation date for initial selection
        matches_all.sort(
            key=lambda m: m["_parsed_date"],
            reverse=True  # newest first
        )
        
        # Select top candidates for AI scoring
        top_candidates = matches_all[:CANDIDATES_TO_SCORE]
        print(f"Selected {len(top_candidates)} newest candidates for AI scoring")

        # ── AI Score calculation ─────────────────────────────────────────
        to_score = [m for m in top_candidates if "aiScore" not in m]
        if to_score:
            print(f"Getting AI scores for {len(to_score)} candidates")
            need_ids = [m["resumeId"] for m in to_score]
            resumes_col = db["resumes"]
            resume_docs = list(resumes_col.find(
                {"resumeId": {"$in": need_ids}},
                {"_id": 0, "embedding": 0}
            ))

            resume_text_col = db["resume_text"]
            text_map = {
                d["resumeId"]: d.get("resumeText")
                for d in resume_text_col.find(
                    {"resumeId": {"$in": need_ids}},
                    {"_id": 0, "resumeId": 1, "resumeText": 1}
                )
            }
            for r in resume_docs:
                rid = r["resumeId"]
                r["resumeText"] = text_map.get(rid)

            batches = [resume_docs[i:i+BATCH_SIZE]
                       for i in range(0, len(resume_docs), BATCH_SIZE)]

            scores = {}
            with ThreadPoolExecutor(max_workers=PARALLEL_WORKERS) as pool:
                futures = {pool.submit(call_openai, jd_text, b): None for b in batches}
                for f in as_completed(futures):
                    scores.update(f.result())

            # Update database with all new fields
            for rid, score_data in scores.items():
                update_fields = {
                    "matches.$.aiScore": score_data.get("aiScore"),
                    "matches.$.keyMatchPoints": score_data.get("keyMatchPoints"),
                    "matches.$.compensationFit": score_data.get("compensationFit"),
                    "matches.$.locationStatus": score_data.get("locationStatus"),
                    "matches.$.availabilityMatch": score_data.get("availabilityMatch"),
                    "matches.$.hiringRecommendation": score_data.get("hiringRecommendation")
                }
                db["matches"].update_one(
                    {"jobId": jd_id, "matches.resumeId": rid},
                    {"$set": update_fields}
                )
            
            # Update our local candidates with new scores and fields
            for m in top_candidates:
                if m["resumeId"] in scores:
                    score_data = scores[m["resumeId"]]
                    m["aiScore"] = score_data.get("aiScore")
                    m["keyMatchPoints"] = score_data.get("keyMatchPoints")
                    m["compensationFit"] = score_data.get("compensationFit")
                    m["locationStatus"] = score_data.get("locationStatus")
                    m["availabilityMatch"] = score_data.get("availabilityMatch")
                    m["hiringRecommendation"] = score_data.get("hiringRecommendation")

        # ── Refetch and final ranking ──────────────────────────────────
        upd_doc = db["matches"].find_one({"jobId": jd_id}, {"_id": 0})
        upd_matches = upd_doc.get("matches", []) if upd_doc else []

        # Re-apply filters on refreshed list
        if kw_flt:
            kw_set = set(kw_flt)
            upd_matches = [
                m for m in upd_matches
                if kw_set.issubset(set(m.get("commonKeys", [])))
            ]
        if region and region in region_id_to_countries:
            valid = {safe_normalize_country(c) for c in region_id_to_countries[region]}
            upd_matches = [
                m for m in upd_matches
                if safe_normalize_country(m.get("country")) in valid
            ]
        
        # Process dates again for final sorting
        for m in upd_matches:
            m["_parsed_date"] = parse_created_on(m.get("createdOn"))
        
        # ── 3. FINAL RANKING: By date FIRST, then AI score ─────────────
        print("Final ranking: creation date first, then AI score")
        
        # First recompute dates for the updated matches
        for m in upd_matches:
            m["_parsed_date"] = parse_created_on(m.get("createdOn"))
        
        upd_sorted = sorted(
            upd_matches,
            key=lambda m: (
                m["_parsed_date"],                # newest first
                m.get("aiScore", 0)               # then highest AI score
            ),
            reverse=True
        )
        
        final = upd_sorted[:TOP_RESULTS_RETURNED]

        # Clean up and add rank
        for i, m in enumerate(final, 1):
            m["rank"] = i
            if "_parsed_date" in m:
                del m["_parsed_date"]
            if "_keyword_count" in m:
                del m["_keyword_count"]

        print(f"Returning {len(final)} final matches")
        return {
            "statusCode": 200,
            "body": json.dumps({
                "jobDescription": jd,
                "matches": final
            })
        }

    except Exception as e:
        import traceback
        traceback.print_exc()
        return {
            "statusCode": 500,
            "body": json.dumps({"error": f"Internal server error: {str(e)}"})
        }
    finally:
        client.close()
        print("MongoDB connection closed")