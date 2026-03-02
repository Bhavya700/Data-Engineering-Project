import os
import json
import time
import requests
from datetime import datetime, timezone
from typing import Iterable, Dict, Any, List, Optional

GITHUB_API = "https://api.github.com"

def _headers(token: str) -> Dict[str, str]:
    return {
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }

def fetch_pull_requests(token: str, repo_full_name: str, since_iso: Optional[str] = None) -> List[Dict[str, Any]]:
    """
    Fetch PRs via GitHub REST API. For MVP we pull recent PRs using `state=all` and paginate.
    `since_iso` is optional. GitHub pulls API doesn't support `since` directly,
    so we filter client-side using `updated_at`.
    """
    per_page = 100
    page = 1
    out: List[Dict[str, Any]] = []

    while True:
        url = f"{GITHUB_API}/repos/{repo_full_name}/pulls"
        params = {"state": "all", "per_page": per_page, "page": page, "sort": "updated", "direction": "desc"}
        resp = requests.get(url, headers=_headers(token), params=params, timeout=60)

        if resp.status_code == 403 and "rate limit" in resp.text.lower():
            # naive backoff
            time.sleep(30)
            continue

        resp.raise_for_status()
        batch = resp.json()
        if not batch:
            break

        # Client-side filter if since is provided
        if since_iso:
            filtered = []
            for pr in batch:
                updated_at = pr.get("updated_at")
                if updated_at and updated_at >= since_iso:
                    filtered.append(pr)
            batch = filtered

        out.extend(batch)

        # If since_iso provided and results are already older than since, stop early.
        if since_iso and batch:
            last_updated = batch[-1].get("updated_at")
            if last_updated and last_updated < since_iso:
                break

        if len(resp.json()) < per_page:
            break

        page += 1

    return out

def write_jsonl(records: Iterable[Dict[str, Any]], filepath: str) -> None:
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    with open(filepath, "w", encoding="utf-8") as f:
        for r in records:
            f.write(json.dumps(r))
            f.write("\n")

def utc_today_date_str() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")