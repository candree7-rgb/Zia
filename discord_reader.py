import time
from typing import Any, Dict, List, Optional
import requests

class DiscordReader:
    def __init__(self, token: str, channel_id: str):
        self.token = token
        self.channel_id = channel_id
        self.headers = {
            "Authorization": token,
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        }

    def _request_with_retry(self, url: str, params: dict, max_retries: int = 3) -> requests.Response:
        """Make request with retry logic for timeouts."""
        for attempt in range(max_retries):
            try:
                r = requests.get(url, headers=self.headers, params=params, timeout=20)
                if r.status_code == 429:
                    retry = 5.0
                    try:
                        retry = float((r.json() or {}).get("retry_after", 5))
                    except Exception:
                        pass
                    time.sleep(retry + 0.25)
                    continue
                return r
            except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as e:
                if attempt < max_retries - 1:
                    time.sleep(1)  # Wait 1s before retry
                    continue
                raise
        raise requests.exceptions.Timeout("Max retries exceeded")

    def fetch_after(self, after_id: Optional[str], limit: int = 50) -> List[Dict[str, Any]]:
        collected: List[Dict[str, Any]] = []
        params = {"limit": max(1, min(limit, 100))}
        if after_id:
            params["after"] = str(after_id)

        while True:
            r = self._request_with_retry(
                f"https://discord.com/api/v10/channels/{self.channel_id}/messages",
                params
            )
            r.raise_for_status()
            page = r.json() or []
            collected.extend(page)
            if len(page) < params["limit"]:
                break
            max_id = max(int(m.get("id","0")) for m in page if "id" in m)
            params["after"] = str(max_id)
        return collected

    def fetch_message(self, message_id: str) -> Optional[Dict[str, Any]]:
        """Fetch a single message by ID directly (not via 'around' which may be cached)."""
        try:
            # Use direct message endpoint to get fresh data (avoids Discord caching)
            # GET /channels/{channel_id}/messages/{message_id}
            url = f"https://discord.com/api/v10/channels/{self.channel_id}/messages/{message_id}"
            for attempt in range(3):
                try:
                    r = requests.get(url, headers=self.headers, timeout=20)
                    if r.status_code == 429:
                        retry = 5.0
                        try:
                            retry = float((r.json() or {}).get("retry_after", 5))
                        except Exception:
                            pass
                        time.sleep(retry + 0.25)
                        continue
                    if r.status_code == 200:
                        return r.json()
                    return None
                except (requests.exceptions.Timeout, requests.exceptions.ConnectionError):
                    if attempt < 2:
                        time.sleep(1)
                        continue
                    return None
            return None
        except Exception:
            return None

    @staticmethod
    def message_timestamp_unix(msg: Dict[str, Any]) -> float:
        # Discord ISO timestamp: "2025-12-12T15:12:34.123456+00:00" or "...Z"
        ts = msg.get("timestamp") or ""
        if not ts:
            return 0.0
        try:
            # very small parser to avoid dateutil dependency
            # keep only up to seconds
            ts2 = ts.replace("Z", "+00:00")
            # yyyy-mm-ddTHH:MM:SS
            base = ts2[:19]
            y,mo,d = int(base[0:4]), int(base[5:7]), int(base[8:10])
            hh,mm,ss = int(base[11:13]), int(base[14:16]), int(base[17:19])
            import calendar
            return float(calendar.timegm((y,mo,d,hh,mm,ss)))
        except Exception:
            return 0.0

    @staticmethod
    def extract_text(msg: Dict[str, Any]) -> str:
        parts: List[str] = []
        parts.append(msg.get("content") or "")
        embeds = msg.get("embeds") or []
        for e in embeds:
            if not isinstance(e, dict):
                continue
            if e.get("title"):
                parts.append(str(e.get("title")))
            if e.get("description"):
                parts.append(str(e.get("description")))
            for f in (e.get("fields") or []):
                if not isinstance(f, dict):
                    continue
                if f.get("name"):
                    parts.append(str(f.get("name")))
                if f.get("value"):
                    parts.append(str(f.get("value")))
            footer = (e.get("footer") or {}).get("text")
            if footer:
                parts.append(str(footer))
        raw = "\n".join([p for p in parts if p]).strip()
        # Strip Discord markdown for cleaner pattern matching
        # Remove: ** (bold), * (italic), ` (code), _ (underline), ~~ (strikethrough)
        import re
        clean = re.sub(r'\*\*|__', '', raw)  # Bold
        clean = re.sub(r'\*|_', '', clean)   # Italic
        clean = re.sub(r'`', '', clean)       # Code
        clean = re.sub(r'~~', '', clean)      # Strikethrough
        return clean
