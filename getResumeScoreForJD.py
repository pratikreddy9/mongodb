from pymongo import MongoClient
import math
from datetime import datetime
import difflib

# ── CONFIG ─────────────────────────────────────────────────────────────
host       = "notify.pesuacademy.com"
port       = 27017
username   = "admin"
password   = ""
auth_db    = "admin"
db_name    = "resumes_database"
TOP_LIMIT  = 500               # keep best N matches per JD
TITLE_SIM_THRESHOLD = 0.85     # fuzzy title match cut-off
# ───────────────────────────────────────────────────────────────────────

def calculate_cosine_similarity(vec1, vec2):
    dot = sum(a * b for a, b in zip(vec1, vec2))
    n1  = math.sqrt(sum(a * a for a in vec1))
    n2  = math.sqrt(sum(b * b for b in vec2))
    return 0.0 if n1 == 0 or n2 == 0 else dot / (n1 * n2)

def get_common_keywords(list1, list2):
    if isinstance(list1, str):
        list1 = [list1]
    if isinstance(list2, str):
        list2 = [list2]
    return list(set(list1) & set(list2))

def get_common_experiences(resume_exps, jd_exps):
    out = []
    if not isinstance(resume_exps, list) or not isinstance(jd_exps, list):
        return out
    for r in resume_exps:
        r_title = str(r.get("title", "")).lower().strip()
        r_dur   = r.get("duration")
        if not r_title:
            continue
        for j in jd_exps:
            j_title = str(j.get("title", "")).lower().strip()
            j_dur   = j.get("duration")
            if not j_title:
                continue
            score = difflib.SequenceMatcher(None, r_title, j_title).ratio()
            if score >= TITLE_SIM_THRESHOLD:
                out.append({
                    "resumeTitle": r_title,
                    "jdTitle"    : j_title,
                    "resumeDuration": r_dur,
                    "jdDuration"    : j_dur,
                    "matchScore"    : round(score, 2)
                })
    return out

def lambda_handler(event, context):
    client = MongoClient(host=host, port=port,
                         username=username, password=password,
                         authSource=auth_db)
    try:
        db   = client[db_name]
        resumes_col        = db["resumes"]
        jd_col             = db["job_description"]
        matches_col        = db["matches"]
        resume_matches_col = db["resume_matches"]

        print("Fetching JDs with processingState = 'pending' …")
        for jd in jd_col.find({"processingState": "pending"}):
            jd_id   = jd.get("jobId")
            if not jd_id:
                print("» Skipping JD without jobId")
                continue
            print(f"▶ Processing JD {jd_id}")

            jd_keywords     = jd.get("structured_query", {}).get("keywords") or []
            jd_embedding    = jd.get("embedding") or []
            jd_experiences  = jd.get("structured_query", {}).get("jobExperiences") or []

            if not isinstance(jd_keywords, list) or not isinstance(jd_embedding, list):
                print("» Malformed JD, skipping.")
                continue

            matches = []
            print("Fetching resumes …")
            for resume in resumes_col.find():
                # --- Safe normalisation (fixes the crash) ------------------
                resume_keywords = resume.get("keywords") or []
                if not isinstance(resume_keywords, list):
                    resume_keywords = []

                raw_skills   = resume.get("skills") or []
                resume_skills = [s.get("skillName") for s in raw_skills if isinstance(s, dict) and s.get("skillName")]
                combined_tokens = resume_keywords + resume_skills
                # -----------------------------------------------------------

                if not combined_tokens:
                    continue

                common_keys = get_common_keywords(jd_keywords, combined_tokens)
                if not common_keys:
                    continue

                resume_exps        = resume.get("jobExperiences") or []
                common_experiences = get_common_experiences(resume_exps, jd_experiences)

                sim_score = 0.0
                resume_emb = resume.get("embedding") or []
                if isinstance(resume_emb, list) and len(resume_emb) == len(jd_embedding):
                    sim_score = calculate_cosine_similarity(resume_emb, jd_embedding)

                matches.append({
                    "resumeId"       : resume.get("resumeId"),
                    "name"           : resume.get("name"),
                    "email"          : resume.get("email"),
                    "contactNo"      : resume.get("contactNo"),
                    "address"        : resume.get("address"),
                    "city"           : resume.get("city"),
                    "state"          : resume.get("state"),
                    "country"        : resume.get("country"),
                    "createdOn"      : resume.get("createdOn"),
                    "ownedBy"        : resume.get("ownedBy"),
                    "noticePeriod"   : resume.get("noticePeriod"),
                    "expectedCTC"    : resume.get("expectedCTC"),
                    "totalExperience": resume.get("totalExperience"),
                    "commonKeys"     : common_keys,
                    "similarityScore": sim_score,
                    "commonExperiences": common_experiences
                })

            print(f"✓ Found {len(matches)} potential matches")
            matches = sorted(
                matches,
                key=lambda x: (len(x["commonKeys"]), x["similarityScore"]),
                reverse=True
            )[:TOP_LIMIT]

            # Store in `matches`
            matches_col.update_one(
                {"jobId": jd_id},
                {"$set": {"matches": matches}},
                upsert=True
            )
            # Mark JD processed
            jd_col.update_one({"jobId": jd_id},
                              {"$set": {"processingState": "completed"}},
                              upsert=True)

            # Update per-resume reverse index
            updated = 0
            for m in matches:
                resume_id = m["resumeId"]
                info = {
                    "jobId"           : jd_id,
                    "jobDescription"  : jd.get("jobDescription", ""),
                    "commonKeys"      : m["commonKeys"],
                    "similarityScore" : m["similarityScore"],
                    "commonExperiences": m["commonExperiences"]
                }

                already = resume_matches_col.find_one(
                    {"resumeId": resume_id, "matches.jobId": jd_id}
                )
                if not already:
                    resume_matches_col.update_one(
                        {"resumeId": resume_id},
                        {
                            "$push": {"matches": info},
                            "$set" : {"lastUpdated": datetime.utcnow().strftime("%Y-%m-%d")}
                        },
                        upsert=True
                    )
                    updated += 1
            print(f"↪  Updated resume_matches for {updated} resumes\n")

        print("All pending JDs processed successfully")
        return {"statusCode": 200,
                "body": "Job description matching completed successfully"}
    except Exception as e:
        print("Error:", e)
        return {"statusCode": 500,
                "body": str(e)}
    finally:
        client.close()
