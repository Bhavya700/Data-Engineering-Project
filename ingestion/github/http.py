import time
import requests
from typing import Dict, Any, Optional

def github_get(url: str, headers: Dict[str, str], params: Optional[Dict[str, Any]] = None, timeout: int = 60) -> requests.Response:
    """
    Handles GitHub rate limiting gracefully using headers.
    """
    while True:
        resp = requests.get(url, headers=headers, params=params, timeout=timeout)

        # Rate limit handling
        if resp.status_code == 403 and "rate limit" in resp.text.lower():
            reset = resp.headers.get("X-RateLimit-Reset")
            if reset:
                sleep_for = max(5, int(reset) - int(time.time()) + 2)
                time.sleep(min(sleep_for, 120))  # cap sleep to 2 minutes for MVP
            else:
                time.sleep(30)
            continue

        resp.raise_for_status()
        return resp