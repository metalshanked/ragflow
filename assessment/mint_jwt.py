"""
CLI helper to mint JWTs accepted by the Assessment API.

Examples:
    python -m assessment.mint_jwt --subject admin --role admin
    python -m assessment.mint_jwt --subject analyst --role viewer --ttl-minutes 480
"""

from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone
from typing import Sequence

import jwt

from .auth import KNOWN_ROLES
from .config import settings


def _normalize_roles(values: Sequence[str]) -> list[str]:
    roles: list[str] = []
    for value in values:
        role = value.strip().lower()
        if not role:
            continue
        if role not in KNOWN_ROLES:
            raise ValueError(
                f"Unsupported role '{value}'. Expected one of: {', '.join(KNOWN_ROLES)}"
            )
        if role not in roles:
            roles.append(role)
    if not roles:
        raise ValueError("At least one --role is required.")
    return roles


def _normalize_groups(values: Sequence[str]) -> list[str]:
    groups: list[str] = []
    for value in values:
        group = value.strip()
        if group and group not in groups:
            groups.append(group)
    return groups


def mint_token(
    *,
    subject: str,
    roles: Sequence[str],
    groups: Sequence[str],
    ttl_minutes: int,
    secret: str,
    algorithm: str,
    token_type: str = "access",
    auth_type: str = "jwt",
) -> str:
    if not subject.strip():
        raise ValueError("--subject is required.")
    if not secret.strip():
        raise ValueError(
            "JWT secret is empty. Set ASSESSMENT_JWT_SECRET_KEY or pass --secret."
        )
    if ttl_minutes < 1:
        raise ValueError("--ttl-minutes must be >= 1.")
    if token_type not in {"access", "refresh"}:
        raise ValueError("--token-type must be 'access' or 'refresh'.")

    normalized_roles = _normalize_roles(roles)
    normalized_groups = _normalize_groups(groups)
    now = datetime.now(timezone.utc)
    exp = now + timedelta(minutes=ttl_minutes)
    payload = {
        "sub": subject.strip(),
        "roles": normalized_roles,
        "auth_type": auth_type.strip() or "jwt",
        "type": token_type,
        "exp": int(exp.timestamp()),
    }
    if normalized_groups:
        payload["groups"] = normalized_groups
    return jwt.encode(payload, secret, algorithm=algorithm)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Mint a JWT accepted by the Assessment API."
    )
    parser.add_argument(
        "--subject",
        required=True,
        help="JWT subject, usually the username or service identity.",
    )
    parser.add_argument(
        "--role",
        action="append",
        default=[],
        help="Role claim to include. Repeat for multiple roles.",
    )
    parser.add_argument(
        "--group",
        action="append",
        default=[],
        help="Optional group claim to include. Repeat for multiple groups.",
    )
    parser.add_argument(
        "--ttl-minutes",
        type=int,
        default=60,
        help="Token lifetime in minutes. Default: 60.",
    )
    parser.add_argument(
        "--token-type",
        choices=["access", "refresh"],
        default="access",
        help="Token type claim. Default: access.",
    )
    parser.add_argument(
        "--auth-type",
        default="jwt",
        help="Authentication source claim. Default: jwt.",
    )
    parser.add_argument(
        "--secret",
        default=settings.jwt_secret_key,
        help="Signing secret. Defaults to ASSESSMENT_JWT_SECRET_KEY.",
    )
    parser.add_argument(
        "--algorithm",
        default=settings.jwt_algorithm,
        help=f"JWT signing algorithm. Default: {settings.jwt_algorithm}.",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        token = mint_token(
            subject=args.subject,
            roles=args.role,
            groups=args.group,
            ttl_minutes=args.ttl_minutes,
            secret=args.secret,
            algorithm=args.algorithm,
            token_type=args.token_type,
            auth_type=args.auth_type,
        )
    except ValueError as exc:
        parser.exit(status=2, message=f"error: {exc}\n")

    print(token)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
