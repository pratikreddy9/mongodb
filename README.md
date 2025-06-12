# AI-Powered Resume Matching System

An intelligent resume matching and scoring system that uses MongoDB for data storage and OpenAI's GPT models for AI-powered resume analysis and job description matching.

## Overview

This system provides a comprehensive solution for:
- **Resume Storage & Management**: Store and organize resumes in MongoDB with vector embeddings
- **Job Description Processing**: Parse and analyze job descriptions for matching
- **AI-Powered Scoring**: Use OpenAI's GPT models to score resume-job matches
- **Intelligent Search**: Natural language queries to find matching candidates
- **Vector Similarity**: Cosine similarity matching using embeddings

## Features

- 🤖 **AI-Powered Matching**: Uses OpenAI GPT-4 for intelligent resume scoring
- 📊 **Vector Embeddings**: Creates and compares vector embeddings for semantic matching
- 🔍 **Smart Search**: Natural language query processing for finding candidates
- 📈 **Scoring System**: Comprehensive scoring based on skills, experience, and job fit
- 🗄️ **MongoDB Integration**: Efficient storage and retrieval of resume data
- ⚡ **Parallel Processing**: Multi-threaded processing for improved performance

## System Components

### Core Files

- [`chat.py`](./chat.py) - Natural language query interface for resume search
- [`fetchjddata.py`](./fetchjddata.py) - Main job description processing and candidate matching
- [`fetchjddatanew.py`](./fetchjddatanew.py) - Enhanced version of job description processing
- [`addResumeToZap.py`](./addResumeToZap.py) - Resume upload and processing system
- [`getResumeScoreForJD.py`](./getResumeScoreForJD.py) - Resume scoring algorithms
- [`getJobDescriptionVector.py`](./getJobDescriptionVector.py) - Job description vectorization

### Utility Files

- [`addresumetext.py`](./addresumetext.py) - Text processing for resume data
- [`fetchresumedata.py`](./fetchresumedata.py) - Resume data retrieval utilities
- [`sampleresume.py`](./sampleresume.py) - Sample resume data for testing
- [`getAIScore`](./getAIScore) - AI scoring utilities
- [`buildinglambdadependencies`](./buildinglambdadependencies) - AWS Lambda deployment dependencies

## Prerequisites

- Python 3.7+
- MongoDB instance
- OpenAI API key
- Required Python packages:
  - `pymongo`
  - `requests`
  - `datetime`
  - `concurrent.futures`

## Configuration

Before running the system, you'll need to configure the following in each relevant file:

### MongoDB Configuration

```python
host = "your-mongodb-host"
port = 27017
username = "your-username"
password = "your-password"
auth_db = "admin"
db_name = "resumes_database"
```

### OpenAI Configuration

```python
OPENAI_API_KEY = "your-openai-api-key"
OPENAI_MODEL = "gpt-4o"
```

## Usage

### 1. Adding Resumes

Use [`addResumeToZap.py`](./addResumeToZap.py) to add new resumes to the system:

```python
# The script will process resume text and create embeddings
# Store resume data with metadata in MongoDB
```

### 2. Processing Job Descriptions

Use [`fetchjddata.py`](./fetchjddata.py) for job description matching:

```python
# Process job descriptions
# Find matching candidates
# Score and rank results
```

### 3. Natural Language Search

Use [`chat.py`](./chat.py) for conversational resume search:

```python
# Example: "Find software engineers with 3+ years Python experience"
# Returns structured MongoDB queries and results
```

### 4. Getting Resume Scores

Use [`getResumeScoreForJD.py`](./getResumeScoreForJD.py) for detailed scoring:

```python
# Calculate cosine similarity
# Compare skills and experience
# Generate comprehensive match scores
```

## System Architecture

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   Resume Input  │───▶│   AI Processing  │───▶│   MongoDB       │
│   (Text/PDF)    │    │   (OpenAI GPT)   │    │   Storage       │
└─────────────────┘    └──────────────────┘    └─────────────────┘
                                │
                                ▼
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   Job Desc.     │───▶│   Vector         │───▶│   Matching &    │
│   Input         │    │   Embeddings     │    │   Scoring       │
└─────────────────┘    └──────────────────┘    └─────────────────┘
```

## Configuration Parameters

### Performance Tuning

- `CANDIDATES_TO_SCORE`: Number of resumes to score (default: 20)
- `TOP_RESULTS_RETURNED`: Number of top results to return (default: 5)
- `PARALLEL_WORKERS`: Number of parallel OpenAI API calls (default: 4)
- `BATCH_SIZE`: Resumes per OpenAI request (default: 5)

### Matching Thresholds

- `TITLE_SIM_THRESHOLD`: Fuzzy title matching threshold (default: 0.85)
- `TOP_LIMIT`: Maximum matches per job description (default: 500)

## API Response Format

The system returns structured JSON responses:

```json
{
    "message": "Found 5 matching candidates for Software Engineer position",
    "query_parameters": {
        "country": "USA",
        "min_experience_years": 3,
        "max_experience_years": 8,
        "job_titles": ["Software Engineer", "Developer"],
        "skills": ["Python", "JavaScript", "React"]
    },
    "results": [
        {
            "resume_id": "...",
            "score": 0.95,
            "name": "John Doe",
            "experience_years": 5,
            "skills": ["Python", "React", "Node.js"]
        }
    ]
}
```

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Test thoroughly
5. Submit a pull request

## License

This project is available for educational and research purposes.

## Support

For questions or issues, please open an issue in the GitHub repository.