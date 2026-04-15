"""Tests for the SDK exception hierarchy."""

import pytest

from kalamdb import (
    KalamError,
    KalamConnectionError,
    KalamAuthError,
    KalamServerError,
    KalamConfigError,
)


def test_exception_hierarchy():
    """All custom exceptions should inherit from KalamError."""
    assert issubclass(KalamConnectionError, KalamError)
    assert issubclass(KalamAuthError, KalamError)
    assert issubclass(KalamServerError, KalamError)
    assert issubclass(KalamConfigError, KalamError)


def test_kalam_error_is_exception():
    assert issubclass(KalamError, Exception)


def test_can_raise_and_catch():
    with pytest.raises(KalamError):
        raise KalamConnectionError("oops")

    with pytest.raises(KalamConnectionError):
        raise KalamConnectionError("network down")
