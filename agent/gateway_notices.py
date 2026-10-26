"""Gateway-injected internal notices: model-facing, never user bubbles."""

from __future__ import annotations

INTERNAL_NOTICE_PREFIXES = ("[IMPORTANT:", "[INTERNAL NOTIFICATION", "[System:")


def is_internal_notice(content) -> bool:
    """True when *content* is a gateway-injected notice rather than the user's words."""
    if not isinstance(content, str):
        return False
    return content.lstrip().startswith(INTERNAL_NOTICE_PREFIXES)
