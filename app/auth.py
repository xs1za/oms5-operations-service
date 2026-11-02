from __future__ import annotations

import json
import logging
from typing import Any
import urllib.request

from fastapi import Header, HTTPException, status

from app.settings import settings

logger = logging.getLogger(settings.service_name)

OPS_ROLES = {"manager", "supervisor", "administrator"}


def verify_oms1_token(token: str) -> dict[str, Any] | None:
    """Validate a bearer token against OMS1 /auth/verify and return the principal."""
    try:
        request = urllib.request.Request(
            f"{settings.oms1_auth_base_url}/auth/verify",
            headers={"Authorization": f"Bearer {token}"},
        )
        with urllib.request.urlopen(request, timeout=5) as response:
            data = json.loads(response.read().decode("utf-8"))
        return data if data.get("active") else None
    except Exception:
        logger.warning("OMS1 token verification failed", exc_info=True)
        return None


def require_principal(authorization: str | None = Header(default=None)) -> dict[str, Any]:
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail={"code": "invalid_token", "message": "Missing or malformed bearer token"},
        )
    principal = verify_oms1_token(authorization[len("Bearer "):])
    if principal is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail={"code": "invalid_token", "message": "Token is not active"},
        )
    if principal.get("type") != "user":
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail={"code": "invalid_token", "message": "A human user token is required"},
        )
    return principal


def require_operator(principal: dict[str, Any] = None) -> dict[str, Any]:
    if principal is None:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail={"code": "invalid_token"})
    roles = set(principal.get("roles") or [])
    if not roles.intersection(OPS_ROLES):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail={"code": "operator_role_required", "message": "An operator role is required"},
        )
    return principal


def principal_person_id(principal: dict[str, Any]) -> int | None:
    value = principal.get("person_id")
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def require_service_token(x_service_token: str | None = Header(default=None, alias="X-Service-Token")) -> None:
    """Internal endpoint guard: OMS2 presents its shared service token."""
    expected = settings.oms2_service_secret
    if not expected or x_service_token != expected:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail={"code": "service_token_required", "message": "A valid service token is required"},
        )
