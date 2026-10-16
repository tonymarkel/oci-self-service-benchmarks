"""Source boundary for the distinct startup-safe Hotel image evidence.

This recipe is not accepted by any existing runtime or image-access factory.
The historical baseline constants select original inputs only; they are never
adoption authority and must still pass the original fresh adopter.
"""

from pathlib import Path

from scripts import prepare_deathstarbench_hotel_startup as preparation
from scripts import validate_deathstarbench_hotel_startup as validation
from scripts.deathstarbench_hotel_initialization_artifact_common import (
    ARCHITECTURES, BASELINE_ARTIFACT, BASELINE_LOCK_SHA256,
    BASELINE_PUBLICATION_SHA256, BASELINE_RUN, PLATFORMS, verify_baseline_files,
)


PROGRAMS = tuple(sorted((*preparation.APPLICATIONS, 'hotel-initialize')))


def validate_startup_preparation(preparation_root: Path, upstream: Path) -> dict:
    """Reconstruct the complete exact source tree, receipts and modes afresh.

    No registry observation or caller-supplied tree/context digest substitutes
    for the independent reconstruction performed by the startup validator.
    """
    return validation.validate_preparation(Path(preparation_root), Path(upstream))
