"""
JWT Bearer token authentication for the Assessment API.

When ``ASSESSMENT_JWT_SECRET_KEY`` is set, every request must include a valid
JWT in the ``Authorization: Bearer <token>`` header.  The token is verified
using HS256 with the configured secret.

When the secret is empty (default), authentication is **disabled** and all
requests are allowed through.

Generate a token (example using PyJWT):

    import jwt
    token = jwt.encode({"sub": "assessment-client"}, "your-secret", algorithm="HS256")
"""

from __future__ import annotations

from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

import jwt

from .config import settings

_bearer_scheme = HTTPBearer(auto_error=False)


async def verify_jwt(
    credentials: HTTPAuthorizationCredentials | None = Depends(_bearer_scheme),
) -> None:
    """
    FastAPI dependency that validates the JWT bearer token.

    * If ``jwt_secret_key`` is empty, authentication is disabled (pass-through).
    * Otherwise the token is decoded with HS256 and the configured secret.
    """
    if not settings.jwt_secret_key:
        # Auth disabled
        return

    if credentials is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing Authorization header",
            headers={"WWW-Authenticate": "Bearer"},
        )

    token = credentials.credentials
    try:
        jwt.decode(token, settings.jwt_secret_key, algorithms=["HS256"])
    except jwt.ExpiredSignatureError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token has expired",
            headers={"WWW-Authenticate": "Bearer"},
        )
    except jwt.InvalidTokenError as exc:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=f"Invalid token: {exc}",
            headers={"WWW-Authenticate": "Bearer"},
        )
