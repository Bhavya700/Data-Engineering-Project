import os
import json
import time
import requests
from datetime import datetime, timezone
from typing import Iterable, Dict, Any, List, Optional
from ingestion.github.http import github_get

GITHUB_API = "https://api.github.com"

def _headers(token: str) -> Dict[str, str]:
    return {
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }

def fetch_pull_requests(token: str, repo_full_name: str, since_iso: Optional[str] = None) -> List[Dict[str, Any]]:
    """
    Fetch PRs sorted by updated desc.
    If since_iso is provided (ISO 8601 string like '2026-03-01T00:00:00Z'),
    we:
      - keep only PRs with updated_at >= since_iso
      - stop paging once the OLDEST PR on the current page is older than since_iso
    """
    per_page = 100
    page = 1
    out: List[Dict[str, Any]] = []

    while True:
        url = f"{GITHUB_API}/repos/{repo_full_name}/pulls"
        params = {
            "state": "all",
            "per_page": per_page,
            "page": page,
            "sort": "updated",
            "direction": "desc",
        }

        resp = github_get(url, headers=_headers(token), params=params, timeout=60)
        batch = resp.json()
        if not batch:
            break

        # Oldest item on this page (since sort=updated desc)
        oldest_updated = batch[-1].get("updated_at")

        if since_iso:
            # keep only new enough PRs
            for pr in batch:
                updated_at = pr.get("updated_at")
                if updated_at and updated_at >= since_iso:
                    out.append(pr)

            # If the oldest PR on this page is older than cutoff, next pages will be even older => stop
            if oldest_updated and oldest_updated < since_iso:
                break
        else:
            out.extend(batch)

        if len(batch) < per_page:
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