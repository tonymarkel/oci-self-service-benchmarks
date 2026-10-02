"""Pure OCI shape and platform-image identity helpers."""

from __future__ import annotations

import re


_STANDARD_FLEX_SHAPE = re.compile(
    r"VM\.Standard(?:[A-Za-z0-9]+|\.[A-Za-z0-9]+)"
    r"(?:\.[A-Za-z0-9]+)*\.Flex"
)


def is_standard_flex_shape(value: object) -> bool:
    """Return whether ``value`` names an OCI Standard flexible VM shape.

    This deliberately validates the shape class instead of maintaining a
    generation allowlist. Live OCI preflight remains authoritative for exact
    availability, flexibility, capacity, and image compatibility.
    """

    return isinstance(value, str) and bool(_STANDARD_FLEX_SHAPE.fullmatch(value))


def oracle_linux_9_image_architecture(display_name: object) -> str:
    """Derive the benchmark architecture from an exact OL9 platform image.

    OCI's ``Ax`` suffix does not identify a CPU architecture: Standard4.Ax
    and Standard.E6.Ax are x86 while Standard.A4.Ax is Arm. The compatible
    platform image selected for the exact shape is therefore the durable
    architecture authority.
    """

    name = str(display_name or "").strip()
    if not name.startswith("Oracle-Linux-9"):
        raise ValueError(
            "OCI distributed DeathStarBench requires an identifiable "
            "Oracle Linux 9 platform image."
        )
    return "arm64" if "aarch64" in name.casefold() else "x86_64"


__all__ = (
    "is_standard_flex_shape",
    "oracle_linux_9_image_architecture",
)
