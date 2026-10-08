"""Read-only, digest-verifying OCI registry access for candidate evidence.

Only Docker Hub and GHCR are supported. No login, push, Docker credential
lookup, or release mutation occurs. REGISTRY_USERNAME and REGISTRY_TOKEN, if
explicitly supplied, authenticate only to GHCR's validated scoped token
endpoint using Basic auth. The resulting bearer JWT is never printed.
Anonymous inspection obtains public, repository-scoped bearer tokens.
"""

from __future__ import annotations

import hashlib
import argparse
import base64
import json
import os
import re
import urllib.error
import urllib.parse
import urllib.request
import sys
from pathlib import Path

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts.deathstarbench_artifact_common import PreparationError


DIGEST = re.compile(r"sha256:[0-9a-f]{64}\Z")
ACCEPT = ", ".join((
    "application/vnd.oci.image.index.v1+json",
    "application/vnd.docker.distribution.manifest.list.v2+json",
    "application/vnd.oci.image.manifest.v1+json",
    "application/vnd.docker.distribution.manifest.v2+json",
))
MAX_DOCUMENT_BYTES = 8 * 1024 * 1024


def digest_bytes(raw: bytes) -> str:
    return "sha256:" + hashlib.sha256(raw).hexdigest()


def parse_reference(reference: str) -> tuple[str, str, str]:
    if not isinstance(reference, str) or any(c.isspace() for c in reference):
        raise PreparationError("Invalid OCI image reference.")
    if "/" not in reference:
        raise PreparationError("An explicit supported registry is required.")
    registry, rest = reference.split("/", 1)
    if registry not in {"docker.io", "ghcr.io"}:
        raise PreparationError("Only Docker Hub and GHCR registry inspection is supported.")
    if "@" in rest:
        repository, selector = rest.split("@", 1)
        if not DIGEST.fullmatch(selector):
            raise PreparationError("A SHA-256 manifest digest is required.")
        if ":" in repository:
            repository, tag = repository.rsplit(":", 1)
            if not re.fullmatch(r"[A-Za-z0-9_][A-Za-z0-9_.-]{0,127}", tag):
                raise PreparationError("Invalid OCI tag.")
    elif ":" in rest:
        repository, selector = rest.rsplit(":", 1)
        if not re.fullmatch(r"[A-Za-z0-9_][A-Za-z0-9_.-]{0,127}", selector):
            raise PreparationError("Invalid OCI tag.")
    else:
        raise PreparationError("An explicit OCI tag or digest is required.")
    if not repository or any(not re.fullmatch(r"[a-z0-9][a-z0-9._-]*", p) or p in {".", ".."}
                             for p in repository.split("/")):
        raise PreparationError("Invalid OCI repository path.")
    return registry, repository, selector


class _SafeRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, request, fp, code, message, headers, newurl):
        if urllib.parse.urlparse(newurl).scheme != "https":
            raise PreparationError("Registry redirect must remain HTTPS.")
        redirected = super().redirect_request(request, fp, code, message, headers, newurl)
        if redirected is not None:
            redirected.remove_header("Authorization")
        return redirected


def _open(url: str, headers: dict[str, str]) -> tuple[bytes, dict[str, str]]:
    request = urllib.request.Request(url, headers=headers)
    opener = urllib.request.build_opener(_SafeRedirect())
    with opener.open(request, timeout=30) as response:
        raw = response.read(MAX_DOCUMENT_BYTES + 1)
        if len(raw) > MAX_DOCUMENT_BYTES:
            raise PreparationError("Registry document exceeds the bounded inspection size.")
        return raw, {key.lower(): value for key, value in response.headers.items()}


def _request(registry: str, repository: str, kind: str, selector: str) -> tuple[bytes, dict[str, str]]:
    host = "registry-1.docker.io" if registry == "docker.io" else "ghcr.io"
    url = f"https://{host}/v2/{repository}/{kind}/{selector}"
    headers = {"Accept": ACCEPT, "User-Agent": "DeathStarBench-candidate-readonly/1"}
    supplied = os.environ.get("REGISTRY_TOKEN") if registry == "ghcr.io" else None
    username = os.environ.get("REGISTRY_USERNAME") if registry == "ghcr.io" else None
    if supplied:
        if any(c in supplied for c in "\r\n") or len(supplied) > 16384:
            raise PreparationError("Invalid explicitly supplied registry token.")
        if not username or any(c in username for c in ":\r\n") or len(username) > 256:
            raise PreparationError("Explicit GHCR authentication requires a valid registry username.")
    try:
        return _open(url, headers)
    except urllib.error.HTTPError as exc:
        exc.close()
        if exc.code != 401:
            raise PreparationError("Registry inspection was denied or unavailable.") from None
        challenge = exc.headers.get("WWW-Authenticate", "")
        pairs = dict(re.findall(r'(realm|service|scope)="([^"]+)"', challenge))
        allowed_realm = "https://auth.docker.io/token" if registry == "docker.io" else "https://ghcr.io/token"
        if not challenge.startswith("Bearer ") or pairs.get("realm") != allowed_realm:
            raise PreparationError("Unexpected registry authentication challenge.") from None
        expected_service = "registry.docker.io" if registry == "docker.io" else "ghcr.io"
        if pairs.get("service") != expected_service:
            raise PreparationError("Unexpected registry token service.") from None
        query = urllib.parse.urlencode({"service": expected_service, "scope": f"repository:{repository}:pull"})
        try:
            token_headers = {"Accept": "application/json"}
            if supplied:
                credentials = base64.b64encode((username + ":" + supplied).encode()).decode("ascii")
                token_headers["Authorization"] = "Basic " + credentials
            raw_token, _ = _open(allowed_realm + "?" + query, token_headers)
            token_document = json.loads(raw_token)
            token = token_document.get("token") or token_document.get("access_token")
            if not isinstance(token, str) or not token or any(c in token for c in "\r\n") or len(token) > 16384:
                raise PreparationError("Registry bearer token is invalid.")
            headers["Authorization"] = "Bearer " + token
            return _open(url, headers)
        except (urllib.error.URLError, ValueError, TypeError, AttributeError):
            raise PreparationError("Registry inspection failed.") from None
    except urllib.error.URLError:
        raise PreparationError("Registry inspection is unavailable.") from None


def inspect_manifest(reference: str) -> bytes:
    registry, repository, selector = parse_reference(reference)
    raw, headers = _request(registry, repository, "manifests", selector)
    observed = digest_bytes(raw)
    declared = headers.get("docker-content-digest")
    if not declared or not DIGEST.fullmatch(declared) or declared != observed:
        raise PreparationError("Registry manifest bytes do not match its declared SHA-256.")
    if DIGEST.fullmatch(selector) and observed != selector:
        raise PreparationError("Registry manifest bytes do not match the requested SHA-256.")
    return raw


def inspect_blob(repository: str, digest: str) -> bytes:
    if not DIGEST.fullmatch(digest):
        raise PreparationError("A SHA-256 config digest is required.")
    registry, name, _ = parse_reference(repository + "@" + digest)
    raw, _ = _request(registry, name, "blobs", digest)
    if digest_bytes(raw) != digest:
        raise PreparationError("Registry config bytes do not match the requested SHA-256.")
    return raw


def resolve_platform(reference: str, platform: str) -> dict[str, object]:
    """Verify index/manifest/config bytes and exactly one requested platform."""
    if platform not in {"linux/amd64", "linux/arm64"}:
        raise PreparationError("Only Linux amd64/arm64 platforms are supported.")
    registry, name, _ = parse_reference(reference)
    repository = registry + "/" + name
    original = inspect_manifest(reference)
    try:
        document = json.loads(original)
        if document.get("schemaVersion") != 2:
            raise PreparationError("Unsupported OCI manifest schema.")
        index_image = None
        if "manifests" in document:
            candidates = [m for m in document["manifests"] if m.get("platform", {}).get("os") == "linux"
                          and m.get("platform", {}).get("architecture") == platform.split("/")[1]
                          and m.get("platform", {}).get("variant", "") in ({"", "v8"} if platform == "linux/arm64" else {""})]
            if len(candidates) != 1:
                raise PreparationError("OCI index lacks exactly one compatible requested platform.")
            descriptor = candidates[0]
            index_image = repository + "@" + digest_bytes(original)
            image = repository + "@" + descriptor["digest"]
            manifest_raw = inspect_manifest(image)
            if len(manifest_raw) != descriptor.get("size"):
                raise PreparationError("OCI platform descriptor size does not match manifest bytes.")
            manifest = json.loads(manifest_raw)
        else:
            manifest_raw, manifest = original, document
            image = repository + "@" + digest_bytes(original)
        if manifest.get("schemaVersion") != 2 or "config" not in manifest or "layers" not in manifest:
            raise PreparationError("An OCI image manifest with config and layers is required.")
        config_descriptor = manifest["config"]
        config_raw = inspect_blob(repository, config_descriptor["digest"])
        if len(config_raw) != config_descriptor.get("size"):
            raise PreparationError("OCI config descriptor size does not match bytes.")
        config = json.loads(config_raw)
        os_name, architecture = platform.split("/")
        if config.get("os") != os_name or config.get("architecture") != architecture:
            raise PreparationError("OCI config architecture differs from the requested platform.")
        return {"requested_image": reference, "image": image, "index_image": index_image,
                "platform": platform, "architecture": architecture,
                "manifest_digest": digest_bytes(manifest_raw), "config_digest": digest_bytes(config_raw),
                "manifest": manifest, "config": config}
    except (ValueError, TypeError, KeyError, AttributeError):
        raise PreparationError("Malformed OCI index, image manifest, or configuration.") from None


def get_platform_image(reference: str, platform: str) -> str:
    return resolve_platform(reference, platform)["image"]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--image", required=True)
    parser.add_argument("--platform", required=True, choices=("linux/amd64", "linux/arm64"))
    args = parser.parse_args()
    print(get_platform_image(args.image, args.platform))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
