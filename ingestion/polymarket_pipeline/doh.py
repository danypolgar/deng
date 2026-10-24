"""DNS-over-HTTPS resolution for the Polymarket API hosts.

Some Swiss ISPs (e.g. Swisscom) answer DNS queries for *.polymarket.com with a block
page address because of the Swiss Gambling Act. This module resolves only the
configured hosts through a public DoH resolver (contacted by IP address, so it needs no
DNS itself) and patches ``socket.getaddrinfo`` for those hosts. TLS still uses the real
hostname (SNI and certificate check are unchanged), so this does not weaken security.
All other hosts use the normal system resolver. If DoH fails, the system resolver is
used as fallback.
"""

import logging
import socket
import threading
import time

import requests

log = logging.getLogger(__name__)

_CACHE_TTL_SECONDS = 300
_lock = threading.Lock()
_cache: dict[str, tuple[float, list[str]]] = {}
_installed = False


def _query(doh_url: str, host: str) -> list[str]:
    response = requests.get(
        doh_url,
        params={"name": host, "type": "A"},
        headers={"accept": "application/dns-json"},
        timeout=10,
    )
    response.raise_for_status()
    answers = response.json().get("Answer") or []
    return [a["data"] for a in answers if a.get("type") == 1]  # type 1 = A record


def _resolve(doh_url: str, host: str) -> list[str]:
    with _lock:
        hit = _cache.get(host)
        if hit and hit[0] > time.monotonic():
            return hit[1]
    ips = _query(doh_url, host)
    if ips:
        with _lock:
            _cache[host] = (time.monotonic() + _CACHE_TTL_SECONDS, ips)
    return ips


def install(hosts: set[str], doh_url: str) -> None:
    """Resolve ``hosts`` via DoH from now on (idempotent)."""
    global _installed
    if _installed or not hosts:
        return
    system_getaddrinfo = socket.getaddrinfo

    def getaddrinfo(host, port, family=0, type=0, proto=0, flags=0):
        if isinstance(host, str) and host.lower() in hosts:
            try:
                ips = _resolve(doh_url, host.lower())
            except Exception as exc:  # fall back to the system resolver
                log.warning("DoH lookup for %s failed (%s), using system DNS", host, exc)
                ips = []
            if ips:
                return [
                    (socket.AF_INET, type or socket.SOCK_STREAM, proto, "", (ip, port))
                    for ip in ips
                ]
        return system_getaddrinfo(host, port, family, type, proto, flags)

    socket.getaddrinfo = getaddrinfo
    _installed = True
    log.info("Resolving %s via DNS-over-HTTPS (%s)", ", ".join(sorted(hosts)), doh_url)
