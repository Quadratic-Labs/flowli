"""Authentication and authorization. See docs/specs/09-http-api.md sections 5 and 6.

The actor of every write comes from the access token. Nothing reads an actor
from a request body: Flowlet v1 did, and its review trail was self-asserted.

The engine records an actor and checks nothing, so every capability check
lives here.
"""

from __future__ import annotations

import time
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any, Protocol

from flowlet.domain import Actor

# --- capabilities -------------------------------------------------------------------

WORKFLOWS_READ = "workflows:read"
EXECUTIONS_READ = "executions:read"
EXECUTIONS_START = "executions:start"
EXECUTIONS_SIGNAL = "executions:signal"
EXECUTIONS_CANCEL = "executions:cancel"
EXECUTIONS_MIGRATE = "executions:migrate"
REVIEWS_READ = "reviews:read"
REVIEWS_DECIDE = "reviews:decide"
QUEUES_READ = "queues:read"
TASKS_CONSUME = "tasks:consume"
EVIDENCE_READ = "evidence:read"

SCOPED = frozenset({REVIEWS_DECIDE, TASKS_CONSUME})


class Unauthenticated(Exception):
    """No token, or a token that does not verify."""


class Forbidden(Exception):
    """A verified token without the capability."""

    def __init__(self, capability: str) -> None:
        super().__init__(f"missing capability {capability}")  # pragma: no mutate
        self.capability = capability


@dataclass(frozen=True, slots=True)
class Principal:
    """Who is calling, and what they may do."""

    actor: Actor
    capabilities: frozenset[str] = field(default_factory=frozenset)

    def allows(self, capability: str, scope: str | None = None) -> bool:
        if capability in self.capabilities:
            return True
        if scope is None:
            return False
        return f"{capability}:{scope}" in self.capabilities or (
            f"{capability}:*" in self.capabilities
        )

    def require(self, capability: str, scope: str | None = None) -> None:
        if not self.allows(capability, scope):
            raise Forbidden(capability if scope is None else f"{capability}:{scope}")


# --- authenticators -----------------------------------------------------------------


class Authenticator(Protocol):
    def authenticate(self, token: str | None, on_behalf_of: str | None) -> Principal: ...


def _bearer(header: str | None) -> str | None:
    if not header:
        return None
    kind, _, token = header.partition(" ")
    return token.strip() if kind.lower() == "bearer" and token.strip() else None


@dataclass
class StaticAuthenticator:
    """A fixed map from token to principal, for tests and for a local run.

    It is not a development back door: an unknown token is refused, and there
    is no way to name an actor from outside the map.
    """

    principals: Mapping[str, Principal]

    def authenticate(self, token: str | None, on_behalf_of: str | None) -> Principal:
        if token is None or token not in self.principals:
            raise Unauthenticated("unknown token")  # pragma: no mutate
        principal = self.principals[token]
        if on_behalf_of is None:
            return principal
        return Principal(
            Actor(principal.actor.kind, principal.actor.id, on_behalf_of),
            principal.capabilities,
        )


@dataclass
class OIDCConfig:
    issuer: str
    audience: str
    jwks_url: str
    actor_claim: str = "email"  # "sub" where the provider re-assigns addresses
    groups_claim: str = "groups"
    scope_claim: str = "scope"
    client_claim: str = "client_id"
    roles: Mapping[str, Sequence[str]] = field(default_factory=dict)
    machine_actor_kind: str = "worker"
    leeway: float = 30.0


class OIDCAuthenticator:
    """Bearer JWT against the JWKS of one issuer.

    The service starts no flow of its own. The web application does the
    authorization-code flow with PKCE and sends the access token.
    """

    def __init__(self, config: OIDCConfig, *, jwk_client: Any | None = None) -> None:
        self.config = config
        if jwk_client is None:
            import jwt  # imported here so the package works without the api extra

            jwk_client = jwt.PyJWKClient(config.jwks_url, cache_keys=True)
        self._jwks = jwk_client

    def authenticate(self, token: str | None, on_behalf_of: str | None) -> Principal:
        import jwt

        if token is None:
            raise Unauthenticated("no bearer token")
        try:
            key = self._jwks.get_signing_key_from_jwt(token).key
            claims = jwt.decode(
                token,
                key,
                algorithms=["RS256", "ES256"],
                audience=self.config.audience,
                issuer=self.config.issuer,
                leeway=self.config.leeway,
                options={"require": ["exp", "iss", "aud"]},
            )
        except Exception as exc:  # every verification failure is one answer
            raise Unauthenticated(str(exc)) from exc
        return self._principal(claims, on_behalf_of)

    # -- claims to principal ---------------------------------------------------------

    def _principal(self, claims: Mapping[str, Any], on_behalf_of: str | None) -> Principal:
        actor = self._actor(claims)
        capabilities = self._capabilities(claims)
        if on_behalf_of is not None:
            scopes = str(claims.get(self.config.scope_claim, "")).split()
            if "act:on_behalf_of" not in scopes:
                raise Forbidden("act:on_behalf_of")
            actor = Actor(actor.kind, actor.id, on_behalf_of)
        return Principal(actor, capabilities)

    def _actor(self, claims: Mapping[str, Any]) -> Actor:
        value = claims.get(self.config.actor_claim)
        if value and (self.config.actor_claim != "email" or claims.get("email_verified", False)):
            return Actor.human(str(value))
        client = claims.get(self.config.client_claim) or claims.get("azp") or claims.get("sub")
        if not client:
            raise Unauthenticated("token names no principal")
        if self.config.machine_actor_kind == "system":
            return Actor.system(str(client))
        return Actor.worker(str(client))

    def _capabilities(self, claims: Mapping[str, Any]) -> frozenset[str]:
        groups = claims.get(self.config.groups_claim) or []
        if isinstance(groups, str):
            groups = groups.split()
        out: set[str] = set()
        for group in groups:
            out.update(self.config.roles.get(str(group), ()))
        return frozenset(out)


@dataclass
class _CachedPrincipal:
    principal: Principal
    at: float


class CachingAuthenticator:
    """Verifies each token once per `ttl`. A JWT verification is CPU work."""

    def __init__(self, inner: Authenticator, ttl: float = 60.0) -> None:
        self.inner = inner
        self.ttl = ttl
        self._cache: dict[tuple[str, str | None], _CachedPrincipal] = {}

    def authenticate(self, token: str | None, on_behalf_of: str | None) -> Principal:
        if token is None:
            raise Unauthenticated("no bearer token")
        now = time.monotonic()
        hit = self._cache.get((token, on_behalf_of))
        if hit is not None and now - hit.at < self.ttl:
            return hit.principal
        principal = self.inner.authenticate(token, on_behalf_of)
        self._cache[(token, on_behalf_of)] = _CachedPrincipal(principal, now)
        return principal


__all__ = [
    "Authenticator",
    "CachingAuthenticator",
    "Forbidden",
    "OIDCAuthenticator",
    "OIDCConfig",
    "Principal",
    "StaticAuthenticator",
    "Unauthenticated",
    "_bearer",
]
