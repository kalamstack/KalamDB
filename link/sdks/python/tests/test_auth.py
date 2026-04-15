"""Unit tests for the Auth helper (no server required)."""

import pytest

from kalamdb import Auth


def test_basic_auth_returns_dict():
    creds = Auth.basic("alice", "secret")
    assert creds == {"type": "basic", "username": "alice", "password": "secret"}


def test_jwt_auth_returns_dict():
    creds = Auth.jwt("eyJhbGciOiJIUzI1NiJ9.payload.sig")
    assert creds == {"type": "jwt", "token": "eyJhbGciOiJIUzI1NiJ9.payload.sig"}


def test_basic_auth_preserves_special_chars():
    creds = Auth.basic("user@example.com", "p@ss w0rd!")
    assert creds["username"] == "user@example.com"
    assert creds["password"] == "p@ss w0rd!"
