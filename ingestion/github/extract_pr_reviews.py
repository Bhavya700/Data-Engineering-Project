from __future__ import annotations
from typing import Dict, Any, List
from datetime import timezone

from ingestion.github.http import github_get
from ingestion.github.extract_pulls import fetch_pull_requests

GITHUB_API = "https://api.github.com"

def _headers(token: str) -> Dict[str, str]:
    return {
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }

def fetch_reviews_for_pr(token: str, repo_full_name: str, pr_number: int) -> List[Dict[str, Any]]:
    per_page = 100
    page = 1
    out: List[Dict[str, Any]] = []

    while True:
        url = f"{GITHUB_API}/repos/{repo_full_name}/pulls/{pr_number}/reviews"
        params = {"per_page": per_page, "page": page}
        resp = github_get(url, headers=_headers(token), params=params, timeout=60)
        batch = resp.json()
        if not batch:
            break
        out.extend(batch)
        if len(batch) < per_page:
            break
        page += 1

    return out

def fetch_pr_reviews_incremental(token: str, repo_full_name: str, since_ts):
    """
    Strategy:
    1) Fetch PRs updated since since_ts
    2) For each PR, fetch its reviews
    Returns:
      - reviews_rows: list of {pr_number, review_object}
      - max_pr_updated_at: watermark candidate (PR updated_at max)
    """
    since_iso = since_ts.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    prs = fetch_pull_requests(token, repo_full_name, since_iso=since_iso)

    reviews_rows = []
    max_pr_updated_at = None

    for pr in prs:
        pr_number = pr["number"]
        pr_updated_at = pr.get("updated_at")

        if pr_updated_at:
            if (max_pr_updated_at is None) or (pr_updated_at > max_pr_updated_at):
                max_pr_updated_at = pr_updated_at

        reviews = fetch_reviews_for_pr(token, repo_full_name, pr_number)
        for r in reviews:
            reviews_rows.append({"pr_number": pr_number, "review": r})

    return reviews_rows, max_pr_updated_at