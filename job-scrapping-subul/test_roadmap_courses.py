#!/usr/bin/env python3
"""
Test script for roadmap courses generation.

Usage:
  - Start the backend:  uvicorn main:app --port 8000
  - Run this script:    python test_roadmap_courses.py [user_id]
  - Default user_id=1; override with first argument.

Prints whether the /api/roadmap response includes project_ideas per skill,
and shows the first skill's tip + project_ideas if present.
"""

import json
import sys

try:
    import requests
except ImportError:
    print("Install requests: pip install requests")
    sys.exit(1)

BASE = "http://localhost:8000"
USER_ID = int(sys.argv[1]) if len(sys.argv) > 1 else 1


def main():
    # 1) Debug: is Azure/LLM configured?
    try:
        debug = requests.get(f"{BASE}/api/debug/roadmap-xai", timeout=5).json()
        ok = debug.get("azure_client_ok") and debug.get("explainable_ai_enabled")
        print("Debug (roadmap LLM):")
        print(f"  explainable_ai_enabled={debug.get('explainable_ai_enabled')}")
        print(f"  azure_endpoint_set={debug.get('azure_endpoint_set')}, azure_key_set={debug.get('azure_key_set')}")
        print(f"  azure_client_ok={debug.get('azure_client_ok')}, deployment={debug.get('deployment', '')}")
        if not ok:
            print("  -> LLM will not run; fix .env (AZURE_OPENAI_*, EXPLAINABLE_AI_ENABLED) and restart backend.")
        print()
    except Exception as e:
        print(f"Debug endpoint failed: {e}\n")

    # 2) Call roadmap API
    url = f"{BASE}/api/roadmap"
    payload = {"user_id": str(USER_ID)}
    print(f"POST {url} with {payload}")
    try:
        r = requests.post(url, json=payload, timeout=30)
        r.raise_for_status()
    except requests.exceptions.ConnectionError:
        print("ERROR: Cannot connect to backend. Is it running? (uvicorn main:app --port 8000)")
        sys.exit(1)
    except requests.exceptions.RequestException as e:
        print(f"ERROR: {e}")
        if hasattr(e, "response") and e.response is not None:
            try:
                print(e.response.json())
            except Exception:
                print(e.response.text[:500])
        sys.exit(1)

    data = r.json()
    phases = data.get("phases") or {}
    all_entries = []
    for phase_name, entries in phases.items():
        all_entries.extend(entries or [])

    if data.get("debug_error"):
        print("\n*** Backend LLM error (debug_error from API): ***")
        print(data.get("debug_error"))
        print("***\n")

    with_ideas = [e for e in all_entries if e.get("project_ideas")]
    print(f"\nRoadmap: {len(all_entries)} skills total, {len(with_ideas)} with project_ideas")
    print(f"Message: {(data.get('message') or '')[:120]}...")

    if with_ideas:
        print("\n--- First skill with project_ideas ---")
        e = with_ideas[0]
        print(f"Skill: {e.get('skill')}")
        print(f"Tip: {e.get('tip', '')[:200]}")
        print("Project ideas:")
        for i, idea in enumerate(e.get("project_ideas") or [], 1):
            print(f"  {i}. {idea}")
    else:
        print("\nNo project_ideas in response. Check backend logs for:")
        print("  - [roadmap] LLM courses applied: N skills with project_ideas")
        print("  - [xai] Roadmap courses: no Azure client ...")
        print("  - [xai] Roadmap LLM failed: ...")
        if all_entries:
            print("\nFirst skill (fallback tip only):")
            e = all_entries[0]
            print(f"  Skill: {e.get('skill')}, tip: {(e.get('tip') or '')[:100]}...")

    print()


if __name__ == "__main__":
    main()
