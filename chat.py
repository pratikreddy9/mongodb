import json
import requests
from pymongo import MongoClient
from datetime import datetime

# ✅ MongoDB Setup
def get_mongo_client():
    return MongoClient(
        host="notify.pesuacademy.com",
        port=27017,
        username="admin",
        password="",
        authSource="admin"
    )

# ✅ OpenAI Setup
OPENAI_API_KEY = ""
OPENAI_URL = "https://api.openai.com/v1/chat/completions"
OPENAI_MODEL = "gpt-4o"

MASTER_PROMPT = """
You are a powerful resume filtering assistant. Your job is to convert natural language user queries into a **structured MongoDB query**, fetch matching resumes, and respond with a JSON output that always includes:

{
    "message": "... natural language message summarizing the search ...",
    "query_parameters": {
        "country": "...",
        "min_experience_years": ...,
        "max_experience_years": ...,
        "job_titles": [...],
        "skills": [...],
        "top_k": ...
    },
    "results_count": ...,
    "results": [ ... array of matching resumes ... ],
    "completed_at": "ISO timestamp"
}

Resume collection structure:

- resumeId (str, always present)
- name (str)
- email (str)
- contactNo (str)
- address (nullable str)
- country (nullable str) → we normalize using .strip().lower()

- educationalQualifications (list of dicts)
    - degree (nullable str)
    - field (nullable str)
    - institution (nullable str)
    - graduationYear (nullable int)

- jobExperiences (list of dicts)
    - title (nullable str)
    - duration (nullable str, can be numeric or text)

- keywords (list of str)
- skills (list of dicts)
    - skillName (str)

Country Variants Knowledge:

The following country variants are normalized via .strip().lower() and must be considered equivalent:

Indonesia: ["Indonesia"]
Vietnam: ["Vietnam", "Viet Nam", "Vn", "Vietnamese"]
United States: ["United States", "Usa", "Us"]
Malaysia: ["Malaysia"]
India: ["India", "Ind"]
Singapore: ["Singapore"]
Philippines: ["Philippines", "The Philippines"]
Australia: ["Australia"]
New Zealand: ["New Zealand"]
Germany: ["Germany"]
Saudi Arabia: ["Saudi Arabia", "Ksa"]
Japan: ["Japan"]
Hong Kong: ["Hong Kong", "Hong Kong Sar"]
Thailand: ["Thailand"]
United Arab Emirates: ["United Arab Emirates", "Uae"]

Filtering Rules:

- Country: match via normalized .strip().lower() against country.
- Experience: check jobExperiences[].duration. If numeric string, convert to int; if malformed/null, skip.
- Skills: MUST check in BOTH:
    1️⃣ skills[].skillName (list of dicts)
    2️⃣ keywords[] (list of strings)
- Keywords: match against keywords list.

✅ For skills: when filtering resumes, **always match the target skill against BOTH the `skills[].skillName` and the `keywords` list.** This ensures no relevant candidates are missed.

ALWAYS return a complete JSON response in the above format, even during intermediate steps if you need to refine your query.
✅ Skill & Title Normalization Rules:

To improve search accuracy, when building the MongoDB query:

- **Skills:**
    - Always expand skill names to include common variants, synonyms, and different casings.
    - Examples:
        - "SQL" → ["SQL", "sql", "mysql", "microsoft sql server"]
        - "JavaScript" → ["JavaScript", "javascript", "js", "java script"]
        - "C#" → ["C#", "c sharp", "csharp"]
        - "HTML" → ["HTML", "html", "hypertext markup language"]

- **Job Titles:**
    - Always expand job titles to include common abbreviations and different spacings.
    - Examples:
        - "Software Developer" → ["Software Developer", "software dev", "softwaredeveloper", "software engineer"]
        - "Backend Developer" → ["Backend Developer", "backend dev", "back-end developer", "server-side developer"]
        - "Frontend Developer" → ["Frontend Developer", "frontend dev", "front-end developer"]

- **Case Insensitivity:**
    - All matches are case-insensitive (MongoDB uses `$regex` with `"i"` option).

👉 **IMPORTANT:**
- Expand these fields **directly inside the `query_parameters` JSON** (the `skills` and `job_titles` arrays).
- This ensures the backend can directly use these expanded arrays without needing additional normalization.

✅ Always include **all relevant variants** to avoid missing good matches.

"""

EVALUATOR_PROMPT = """
You are a resume scoring assistant. You receive:

1️⃣ A natural language query (the user's request),
2️⃣ A list of resumes that have already been pre-filtered by country, skills, experience, and job title.

🎯 Your task is to:

- Review the resumes based on the query.
- Select and return ONLY the top 10 best-matching `resumeId`s.

✅ Output format (JSON):

{
    "top_resume_ids": [ ... up to 10 resumeId strings ... ],
    "completed_at": "ISO timestamp"
}

👉 NOTE:
- ONLY return the `resumeId` values in the array.
- Do NOT return full resumes or any extra text.
"""

def call_openai_agent(user_query, previous_context=None):
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {OPENAI_API_KEY}"
    }
    messages = [
        {"role": "system", "content": MASTER_PROMPT},
        {"role": "user", "content": user_query}
    ]
    if previous_context:
        messages.append({"role": "assistant", "content": previous_context})

    payload = {
        "model": OPENAI_MODEL,
        "response_format": {"type": "json_object"},
        "messages": messages
    }
    response = requests.post(OPENAI_URL, headers=headers, json=payload)
    response.raise_for_status()
    full = response.json()
    return full["choices"][0]["message"]["content"]

def call_openai_evaluator(user_query, resumes):
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {OPENAI_API_KEY}"
    }
    messages = [
        {"role": "system", "content": EVALUATOR_PROMPT},
        {"role": "user", "content": f"Query: {user_query}\n\nResumes: {json.dumps(resumes)}"}
    ]

    payload = {
        "model": OPENAI_MODEL,
        "response_format": {"type": "json_object"},
        "messages": messages
    }
    response = requests.post(OPENAI_URL, headers=headers, json=payload)
    response.raise_for_status()
    full = response.json()
    return full["choices"][0]["message"]["content"]

def lambda_handler(event, context):
    mongo_client = get_mongo_client()
    try:
        if 'body' in event:
            body = json.loads(event['body'])
        else:
            body = event

        user_query = body.get("query", "")

        if not user_query:
            return {"statusCode": 400, "body": json.dumps({"error": "Missing 'query' in request"})}

        print(f"Received query: {user_query}")

        # First OpenAI call
        agent_response = call_openai_agent(user_query)
        print("Agent raw response:", agent_response)

        agent_data = json.loads(agent_response)

        filters = agent_data.get("query_parameters", {})
        country = filters.get("country")
        min_exp = filters.get("min_experience_years")
        max_exp = filters.get("max_experience_years")
        job_titles = filters.get("job_titles", [])
        skills = filters.get("skills", [])
        top_k = filters.get("top_k")

        # 🚨 Validate top_k
        if not isinstance(top_k, int) or top_k <= 0:
            top_k = 50

        # 🚨 Validate min_exp
        min_exp_val = 0
        if isinstance(min_exp, int):
            min_exp_val = min_exp
        elif isinstance(min_exp, str) and min_exp.isdigit():
            min_exp_val = int(min_exp)

        print(f"Filters: country={country}, min_exp={min_exp}, max_exp={max_exp}, job_titles={job_titles}, skills={skills}, top_k={top_k}")

        query = {}
        if country:
            query["country"] = {"$regex": f"^{country.strip().lower()}$", "$options": "i"}

        if skills:
            query["$or"] = [{"skills.skillName": {"$in": skills}}, {"keywords": {"$in": skills}}]

        job_exp_filters = []
        if job_titles:
            job_exp_filters.append({"jobExperiences.title": {"$in": job_titles}})
        if min_exp_val > 0:
            job_exp_filters.append({
                "$expr": {
                    "$gte": [
                        {
                            "$toInt": {
                                "$ifNull": [{"$first": "$jobExperiences.duration"}, "0"]
                            }
                        },
                        min_exp_val
                    ]
                }
            })
        if job_exp_filters:
            query["$and"] = job_exp_filters

        print("Final MongoDB query:", json.dumps(query))

        resumes_collection = mongo_client["resumes_database"]["resumes"]
        results = list(resumes_collection.find(query, {"_id": 0, "embedding": 0}).limit(top_k))
        print(f"Fetched {len(results)} candidates")

        # Second OpenAI call: evaluation step
        evaluator_response = call_openai_evaluator(user_query, results)
        print("Evaluator response:", evaluator_response)

        evaluator_data = json.loads(evaluator_response)

        # ✅ Fetch full resumes for the returned resume IDs
        top_resume_ids = evaluator_data.get("top_resume_ids", [])

        top_resumes = list(resumes_collection.find(
            {"resumeId": {"$in": top_resume_ids}},
            {"_id": 0, "embedding": 0}
        ))

        final_output = {
            "query": user_query,
            "initial_agent_statement": agent_response,
            "top_resumes": top_resumes,
            "completed_at": datetime.utcnow().isoformat()
        }

        return {
            "statusCode": 200,
            "body": json.dumps(final_output)
        }

    except Exception as e:
        print("Error occurred:", str(e))
        return {
            "statusCode": 500,
            "body": json.dumps({"error": str(e)})
        }
    finally:
        mongo_client.close()
        print("MongoDB connection closed")
