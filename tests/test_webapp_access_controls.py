from datetime import datetime, timedelta, timezone

import pytest
from fastapi import HTTPException

from webapp.account.server import account_user
from webapp.api.server import commercial_user, require_ingest_key, runtime_operator
from webapp.auth.server import trial_has_expired


def user(**overrides):
    value = {
        "user_id": "test-user",
        "role": "user",
        "account_status": "trial",
        "trial_ends_at": (datetime.now(timezone.utc) + timedelta(days=3)).isoformat(),
    }
    value.update(overrides)
    return value


def test_active_trial_can_access_commercial_features():
    current = user()
    assert commercial_user(user=current) is current
    assert account_user(user=current) is current


@pytest.mark.parametrize("status", ["suspended", "cancelled", "inactive", "expired", "pending"])
def test_non_active_account_status_is_denied_by_account_and_commercial_guards(status):
    for guard in (commercial_user, account_user):
        with pytest.raises(HTTPException) as exc:
            guard(user=user(account_status=status))
        assert exc.value.status_code == 403


def test_expired_trial_is_denied_by_both_account_and_commercial_guards():
    expired = user(
        trial_ends_at=(datetime.now(timezone.utc) - timedelta(seconds=1)).isoformat()
    )
    assert trial_has_expired(expired) is True
    for guard in (commercial_user, account_user):
        with pytest.raises(HTTPException) as exc:
            guard(user=expired)
        assert exc.value.status_code == 403


@pytest.mark.parametrize("trial_end", [None, "", "not-a-date"])
def test_invalid_or_missing_trial_end_fails_closed(trial_end):
    current = user(trial_ends_at=trial_end)
    assert trial_has_expired(current) is True
    for guard in (commercial_user, account_user):
        with pytest.raises(HTTPException) as exc:
            guard(user=current)
        assert exc.value.status_code == 403


def test_admin_bypass_is_limited_to_expiration_not_suspended_status():
    admin = user(role="admin", account_status="admin", trial_ends_at="invalid")
    assert trial_has_expired(admin) is False
    assert commercial_user(user=admin) is admin
    assert account_user(user=admin) is admin
    for guard in (commercial_user, account_user):
        with pytest.raises(HTTPException) as exc:
            guard(user=user(role="admin", account_status="suspended"))
        assert exc.value.status_code == 403


def test_only_admin_can_control_global_paper_runtime():
    admin = user(role="admin", account_status="admin")
    assert runtime_operator(user=admin) is admin
    for role in ("user", "support", ""):
        with pytest.raises(HTTPException) as exc:
            runtime_operator(user=user(role=role, account_status="active_monthly"))
        assert exc.value.status_code == 403


def test_signal_ingest_requires_configured_shared_secret(monkeypatch):
    monkeypatch.delenv("TRADESCANNER_SIGNAL_INGEST_SECRET", raising=False)
    with pytest.raises(HTTPException) as exc:
        require_ingest_key(x_tradescanner_signal_key="anything")
    assert exc.value.status_code == 503


def test_signal_ingest_rejects_wrong_secret_and_accepts_matching_secret(monkeypatch):
    monkeypatch.setenv("TRADESCANNER_SIGNAL_INGEST_SECRET", "test-secret-123")
    with pytest.raises(HTTPException) as exc:
        require_ingest_key(x_tradescanner_signal_key="wrong-secret")
    assert exc.value.status_code == 401
    assert require_ingest_key(x_tradescanner_signal_key="test-secret-123") is None




def test_scanner_forward_auth_endpoint_enforces_commercial_access():
    """Exercise the guard used by Caddy forward_auth without an HTTP test client."""
    from webapp.api.server import commercial_user, scanner_status
    from webapp.auth.server import current_user

    now = datetime.now(timezone.utc)
    active_trial = user(trial_ends_at=(now + timedelta(days=2)).isoformat())
    expired_trial = user(trial_ends_at=(now - timedelta(seconds=1)).isoformat())
    suspended = user(account_status="suspended")

    assert scanner_status(user=commercial_user(user=active_trial))["authorized"] is True

    with pytest.raises(HTTPException) as exc:
        scanner_status(user=commercial_user(user=expired_trial))
    assert exc.value.status_code == 403

    with pytest.raises(HTTPException) as exc:
        scanner_status(user=commercial_user(user=suspended))
    assert exc.value.status_code == 403

    with pytest.raises(HTTPException) as exc:
        current_user(None)
    assert exc.value.status_code == 401
