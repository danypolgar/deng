from .http_client import ThrottledSession

DAILY_FIDELITY_MINUTES = 1440


def fetch_price_history(session: ThrottledSession, base_url: str, token_id: str) -> dict:

    return session.get_json(
        f"{base_url}/prices-history",
        params={"market": token_id, "interval": "max", "fidelity": DAILY_FIDELITY_MINUTES},
    )
