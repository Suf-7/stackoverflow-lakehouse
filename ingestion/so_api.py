"""Minimal Stack Exchange API v2.3 client, standard library only.

Handles the parts of the API that break naive scripts:
  * responses are always gzip/deflate compressed
  * the server can send a `backoff` field (seconds) that MUST be honoured
  * a daily request quota (300 per IP without a key, 10,000 with a key)
  * paging with `has_more`, max 100 items per page

Set the environment variable SE_API_KEY to use a registered app key
(register free at https://stackapps.com/apps/oauth/register).
"""
import gzip
import json
import os
import time
import urllib.error
import urllib.parse
import urllib.request
import zlib

BASE_URL = "https://api.stackexchange.com/2.3"


class QuotaExhausted(RuntimeError):
    pass


class StackExchangeClient:
    def __init__(self, site="stackoverflow", min_quota_reserve=20, pause_seconds=0.2):
        self.site = site
        self.key = os.environ.get("SE_API_KEY")
        self.min_quota_reserve = min_quota_reserve
        self.pause_seconds = pause_seconds
        self.quota_remaining = None
        self.requests_made = 0
        self._backoff_until = 0.0

    # ------------------------------------------------------------------ core
    def get(self, path, **params):
        """Single request. Returns the decoded JSON envelope."""
        if self.quota_remaining is not None and self.quota_remaining <= self.min_quota_reserve:
            raise QuotaExhausted(f"Only {self.quota_remaining} requests left today; stopping.")

        wait = self._backoff_until - time.time()
        if wait > 0:
            time.sleep(wait)

        params.setdefault("site", self.site)
        if self.key:
            params["key"] = self.key
        url = f"{BASE_URL}{path}?{urllib.parse.urlencode(params)}"
        req = urllib.request.Request(url, headers={"Accept-Encoding": "gzip", "User-Agent": "stackoverflow-lakehouse/0.1"})

        for attempt in range(4):
            try:
                with urllib.request.urlopen(req, timeout=60) as resp:
                    raw = resp.read()
                    encoding = resp.headers.get("Content-Encoding", "")
                break
            except urllib.error.HTTPError as err:
                body = self._decompress(err.read(), err.headers.get("Content-Encoding", ""))
                # 400 with error_id 502 = throttle violation; 5xx = transient
                if err.code >= 500 or b"throttle" in body.lower():
                    time.sleep(5 * (attempt + 1))
                    continue
                raise RuntimeError(f"HTTP {err.code} for {url}: {body[:300]!r}") from err
        else:
            raise RuntimeError(f"Gave up after retries: {url}")

        data = json.loads(self._decompress(raw, encoding))
        self.requests_made += 1
        self.quota_remaining = data.get("quota_remaining", self.quota_remaining)
        if "backoff" in data:
            self._backoff_until = time.time() + int(data["backoff"])
        time.sleep(self.pause_seconds)
        return data

    def paginate(self, path, max_pages=None, **params):
        """Yield every item across pages. Stops on has_more=False or max_pages."""
        page = 1
        while True:
            data = self.get(path, page=page, **params)
            for item in data.get("items", []):
                yield item
            if not data.get("has_more") or (max_pages and page >= max_pages):
                return
            page += 1

    # ------------------------------------------------------------ helpers
    def count(self, path, **params):
        """Use the built-in `total` filter to count matches in one request."""
        return self.get(path, filter="total", **params)["total"]

    def answers_for(self, question_ids, answer_filter="withbody"):
        """Fetch answers for many questions, 100 ids per request."""
        ids = list(question_ids)
        for i in range(0, len(ids), 100):
            chunk = ";".join(str(x) for x in ids[i:i + 100])
            yield from self.paginate(f"/questions/{chunk}/answers", pagesize=100,
                                     filter=answer_filter, sort="creation", order="asc")

    @staticmethod
    def _decompress(raw, encoding):
        if encoding == "gzip" or raw[:2] == b"\x1f\x8b":
            return gzip.decompress(raw)
        if encoding == "deflate":
            return zlib.decompress(raw)
        return raw
