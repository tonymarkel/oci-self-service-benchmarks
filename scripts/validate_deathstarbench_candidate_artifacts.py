#!/usr/bin/env python3
"""Prepare Hotel/Media twice and verify deterministic, unreleased receipts.

This is source validation, never image publication or runtime qualification.
The output must be a new directory outside both protected input paths.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts import prepare_deathstarbench_candidate_load_driver as driver
from scripts import prepare_deathstarbench_hotel_images as hotel
from scripts import prepare_deathstarbench_media_images as media
from scripts.deathstarbench_artifact_common import (
    PreparationError, UPSTREAM_REVISION, candidate_output, sha256, tree_sha256,
)


def validate(upstream: Path, luasocket_rock: Path, output: Path) -> dict:
    with candidate_output(upstream, output, protected_paths=(luasocket_rock,)) as destination:
        rounds = []
        for round_name in ("first", "second"):
            root = destination / round_name
            root.mkdir()
            receipts = {}
            for name, workload, prepare in (
                ("hotel", "hotel_reservation", hotel.prepare_contexts),
                ("media", "media_microservices", media.prepare_contexts),
                ("hotel-driver", "hotel_reservation", None),
                ("media-driver", "media_microservices", None),
            ):
                target = root / name
                manifest = (
                    prepare(upstream, target) if prepare is not None
                    else driver.prepare_context(workload, upstream, luasocket_rock, target)
                )
                if (manifest.get("candidate_only") is not True
                        or manifest.get("released") is not False
                        or manifest.get("workload_id") != workload
                        or manifest.get("upstream_revision") != UPSTREAM_REVISION):
                    raise PreparationError(f"Invalid unreleased candidate receipt: {name}")
                receipt_path = target / "context-manifest.json"
                if json.loads(receipt_path.read_text(encoding="utf-8")) != manifest:
                    raise PreparationError(f"Serialized candidate receipt drifted: {name}")
                contexts = manifest.get("contexts")
                if prepare is None:
                    contexts = {"driver": {"path": manifest.get("context_path"),
                                           "context_sha256": manifest.get("context_sha256")}}
                if not isinstance(contexts, dict) or not contexts:
                    raise PreparationError(f"Missing candidate contexts: {name}")
                for context in contexts.values():
                    relative = context.get("path")
                    if (not isinstance(relative, str) or not relative
                            or Path(relative).is_absolute() or ".." in Path(relative).parts
                            or relative != Path(relative).as_posix()):
                        raise PreparationError(f"Unsafe candidate context path: {name}")
                    path = target / relative
                    if path.is_symlink() or not path.is_dir():
                        raise PreparationError(f"Missing or unsafe candidate context: {name}")
                    if tree_sha256(path) != context.get("context_sha256"):
                        raise PreparationError(f"Candidate context digest drifted: {name}")
                receipts[name] = {"manifest_sha256": sha256(receipt_path),
                                  "prepared_tree_sha256": tree_sha256(target)}
            rounds.append(receipts)
        if rounds[0] != rounds[1]:
            raise PreparationError("Candidate preparation is not deterministic.")
        result = {
            "schema_version": 1, "candidate_only": True, "released": False,
            "upstream_revision": UPSTREAM_REVISION,
            "source_preparation_passed": True,
            "independent_preparation_rounds": 2,
            "artifact_receipts": rounds[0],
            "images_built": False, "images_published": False,
            "runtime_qualified": False, "byte_reproducible_image_rebuild": False,
        }
        (destination / "preparation-validation.json").write_text(
            json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8",
        )
        return result


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--upstream", type=Path, required=True)
    parser.add_argument("--luasocket-rock", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    arguments = parser.parse_args()
    print(json.dumps(validate(arguments.upstream, arguments.luasocket_rock, arguments.output), sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
