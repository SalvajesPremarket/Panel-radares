from datetime import datetime, timedelta, timezone

import pytest
from fastapi import HTTPException

from webapp.account.server import account_user
from webapp.api.server import commercial_user


def _trial_user(end, role="user"):
    return {
        "user_id": "trial-access-test",
        "role": role,
        "account_status": "trial",
        "trial_ends_at": end,
    }


def test_current_trial_keeps_account_and_commercial_api_access():
    end = (datetime.now(timezone.utc) + timedelta(days=1)).isoformat()
    user = _trial_user(end)

    assert account_user(user=user) is user
    assert commercial_user(user=user) is user


def test_expired_trial_is_blocked_from_account_and_commercial_api():
    end = (datetime.now(timezone.utc) - timedelta(seconds=1)).isoformat()
    user = _trial_user(end)

    with pytest.raises(HTTPException) as account_error:
        account_user(user=user)
    assert account_error.value.status_code == 403

    with pytest.raises(HTTPException) as api_error:
        commercial_user(user=user)
    assert api_error.value.status_code == 403


def test_invalid_trial_expiry_fails_closed_but_admin_is_not_locked_out():
    with pytest.raises(HTTPException) as error:
        commercial_user(user=_trial_user("not-a-date"))
    assert error.value.status_code == 403

    admin = _trial_user("not-a-date", role="admin")
    assert account_user(user=admin) is admin
    assert commercial_user(user=admin) is admin
