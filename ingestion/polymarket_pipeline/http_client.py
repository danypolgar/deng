
import threading
import time

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

USER_AGENT = "deng-hs26-polymarket-pipeline/0.2"
TIMEOUT_SECONDS = 60


class ThrottledSession:

    def __init__(self, min_interval_seconds: float, max_retries: int = 6):
        self._min_interval = min_interval_seconds
        self._lock = threading.Lock()
        self._last_request = 0.0

        retry = Retry(
            total=max_retries,
            backoff_factor=1.0,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=("GET",),
            respect_retry_after_header=True,
        )
        adapter = HTTPAdapter(max_retries=retry, pool_maxsize=16)
        self._session = requests.Session()
        self._session.headers["User-Agent"] = USER_AGENT
        self._session.mount("https://", adapter)
        self._session.mount("http://", adapter)

    def _wait_turn(self) -> None:
        with self._lock:
            wait = self._last_request + self._min_interval - time.monotonic()
            if wait > 0:
                time.sleep(wait)
            self._last_request = time.monotonic()

    def get_json(self, url: str, params: dict | None = None):
        self._wait_turn()
        response = self._session.get(url, params=params, timeout=TIMEOUT_SECONDS)
        response.raise_for_status()
        return response.json()
