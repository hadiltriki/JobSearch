"""
Test script for LLM-as-judge (xai_explainer).

Run from project root:
  python test_xai_judge.py

Uses .env for Azure OpenAI. Requires EXPLAINABLE_AI_ENABLED=1 for real LLM call.
"""

import asyncio
import json
import os
import sys
from pathlib import Path

# ensure project root is on path
sys.path.insert(0, str(Path(__file__).resolve().parent))

from dotenv import load_dotenv
load_dotenv(Path(__file__).parent / ".env")

# Must set before importing xai_explainer so it reads the env
os.environ["EXPLAINABLE_AI_ENABLED"] = "1"

from xai_explainer import explain_job_match, _fallback_xai


async def main():
    print("Testing LLM-as-judge (explain_job_match)...")
    print("EXPLAINABLE_AI_ENABLED =", os.getenv("EXPLAINABLE_AI_ENABLED"))
    print()

    result = await explain_job_match(
        job_title="Senior Data Engineer",
        job_skills_req="Python, SQL, Spark, AWS, Kafka, Airflow",
        gap_matched=["Python", "SQL", "AWS"],
        gap_missing=["Spark", "Kafka", "Airflow"],
        gap_coverage=0.5,
        gap_total=6,
        cv_role="Data Engineer",
        cv_skills_summary="Python, SQL, PostgreSQL, AWS, ETL, pandas, 4 years experience",
        cosine=0.72,
        match_score=0.68,
        combined_score=0.48,
    )

    if result:
        print("[OK] LLM-as-judge response:")
        print(json.dumps(result, indent=2, ensure_ascii=True))
    else:
        print("[FAIL] explain_job_match returned None (LLM disabled or failed). Showing fallback:")
        fallback = _fallback_xai(0.72, 0.68, 0.48, 0.5, 6)
        print(json.dumps(fallback, indent=2))


if __name__ == "__main__":
    asyncio.run(main())
