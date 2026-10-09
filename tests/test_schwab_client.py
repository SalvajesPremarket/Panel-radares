from urllib.parse import urlparse, parse_qs

import pytest

from TradeScanner.schwab_client import SchwabAPIError, SchwabMarketDataClient


class FakeResponse:
    def __init__(self, status_code=200, payload=None):
        self.status_code = status_code
        self._payload = payload or {}

    def json(self):
        return self._payload


class FakeSession:
    def __init__(self):
        self.posts = []
        self.gets = []
        self.post_response = FakeResponse(payload={"access_token": "access", "refresh_token": "refresh", "expires_in": 1800})
        self.get_response = FakeResponse(payload={"AAPL": {"quote": {}}})

    def post(self, url, **kwargs):
        self.posts.append((url, kwargs))
        return self.post_response

    def get(self, url, **kwargs):
        self.gets.append((url, kwargs))
        return self.get_response


def test_authorization_url_uses_registered_callback():
    client = SchwabMarketDataClient(
        app_key="key", app_secret="secret", callback_url="https://127.0.0.1:8189/",
        token_path="/tmp/schwab-test-no-token.json", session=FakeSession(),
    )
    query = parse_qs(urlparse(client.authorization_url()).query)
    assert query["client_id"] == ["key"]
    assert query["redirect_uri"] == ["https://127.0.0.1:8189/"]
    assert query["response_type"] == ["code"]


def test_exchange_code_persists_tokens_outside_repo(tmp_path):
    session = FakeSession()
    token_path = tmp_path / "private" / "tokens.json"
    client = SchwabMarketDataClient("key", "secret", "https://127.0.0.1:8189/", token_path, session)
    result = client.exchange_code("auth-code")
    assert result["access_token"] == "access"
    assert token_path.exists()
    assert session.posts[0][1]["data"]["grant_type"] == "authorization_code"
    assert session.posts[0][1]["data"]["redirect_uri"] == "https://127.0.0.1:8189/"


def test_quote_uses_bearer_token_and_uppercases_symbol(tmp_path):
    session = FakeSession()
    client = SchwabMarketDataClient("key", "secret", token_path=tmp_path / "tokens.json", session=session)
    client._tokens = {"access_token": "access", "obtained_at": __import__("time").time(), "expires_in": 1800}
    result = client.get_quote("aapl")
    assert "AAPL" in result
    assert session.gets[0][0].endswith("/quotes/AAPL")
    assert session.gets[0][1]["headers"]["Authorization"] == "Bearer access"


def test_expired_access_token_refreshes_and_saves(tmp_path):
    session = FakeSession()
    client = SchwabMarketDataClient("key", "secret", token_path=tmp_path / "tokens.json", session=session)
    client._tokens = {"access_token": "old", "refresh_token": "refresh", "obtained_at": 1, "expires_in": 1}
    client.get_quote("MSFT")
    assert session.posts[0][1]["data"]["grant_type"] == "refresh_token"
    assert client._tokens["access_token"] == "access"


def test_empty_symbol_is_rejected(tmp_path):
    client = SchwabMarketDataClient("key", "secret", token_path=tmp_path / "tokens.json", session=FakeSession())
    with pytest.raises(ValueError):
        client.get_quote("  ")


def test_api_error_does_not_expose_response_body(tmp_path):
    session = FakeSession()
    session.get_response = FakeResponse(status_code=401, payload={"error": "invalid_token"})
    client = SchwabMarketDataClient("key", "secret", token_path=tmp_path / "tokens.json", session=session)
    client._tokens = {"access_token": "access", "obtained_at": __import__("time").time(), "expires_in": 1800}
    with pytest.raises(SchwabAPIError, match="HTTP 401"):
        client.get_quote("AAPL")
