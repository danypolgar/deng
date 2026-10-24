import socket

from polymarket_pipeline import doh


def test_doh_overrides_only_listed_hosts(monkeypatch):
    calls = []
    monkeypatch.setattr(doh, "_query", lambda url, host: calls.append(host) or ["203.0.113.7"])
    monkeypatch.setattr(doh, "_installed", False)
    monkeypatch.setattr(doh, "_cache", {})
    original = socket.getaddrinfo
    monkeypatch.setattr(socket, "getaddrinfo", original)  # restored after the test

    doh.install({"gamma-api.polymarket.com"}, "https://1.1.1.1/dns-query")
    info = socket.getaddrinfo("gamma-api.polymarket.com", 443)
    assert info[0][4] == ("203.0.113.7", 443)
    socket.getaddrinfo("gamma-api.polymarket.com", 443)
    assert calls == ["gamma-api.polymarket.com"]  # second lookup served from cache
    assert socket.getaddrinfo("localhost", 80)  # other hosts use the system resolver


def test_doh_falls_back_to_system_dns(monkeypatch):
    def boom(url, host):
        raise OSError("doh down")

    monkeypatch.setattr(doh, "_query", boom)
    monkeypatch.setattr(doh, "_installed", False)
    monkeypatch.setattr(doh, "_cache", {})
    monkeypatch.setattr(socket, "getaddrinfo", socket.getaddrinfo)

    doh.install({"localhost"}, "https://1.1.1.1/dns-query")
    assert socket.getaddrinfo("localhost", 80)
