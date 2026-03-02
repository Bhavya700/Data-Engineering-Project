from __future__ import annotations
from typing import Dict, Any, List, Optional
from datetime import datetime, timezone

from ingestion.github.http import github_get

GITHUB_API = "https://api.github.com"

def _headers(token: str) -> Dict[str, str]:
    return {
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }

def fetch_issues(token: str, repo_full_name: str, since_ts) -> List[Dict[str, Any]]:
    """
    Uses /issues endpoint (returns issues + PRs) so we filter out PRs.
    We fetch sorted by updated desc and stop once updated_at < since_ts.
    """
    since_iso = since_ts.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    per_page = 100
    page = 1
    out: List[Dict[str, Any]] = []

    while True:
        url = f"{GITHUB_API}/repos/{repo_full_name}/issues"
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

        # Filter out PRs and apply since filter
        filtered = []
        for issue in batch:
            if "pull_request" in issue:
                continue  # it's a PR, not an issue
            updated_at = issue.get("updated_at")
            if updated_at and updated_at >= since_iso:
                filtered.append(issue)

        out.extend(filtered)

        # Early stop once list is older than since cutoff
        last_updated = batch[-1].get("updated_at")
        if last_updated and last_updated < since_iso:
            break

        if len(batch) < per_page:
            break

        page += 1

    return out