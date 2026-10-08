"""Fail-closed source and output helpers for unreleased workload artifacts.

These helpers do not build, publish, qualify, or promote an image. Git status
alone is insufficient: ignored build outputs must not enter a source context.
"""

from __future__ import annotations

import hashlib
import os
import shutil
import stat
import subprocess
from contextlib import contextmanager
from pathlib import Path, PurePosixPath


UPSTREAM_REPOSITORY = "https://github.com/delimitrou/DeathStarBench.git"
UPSTREAM_REVISION = "6ecb09706140f8730b5385c08f1386c654c3c526"


class PreparationError(RuntimeError):
    """A pinned input, safe output, or patch precondition was not satisfied."""


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _relative(value: str) -> PurePosixPath:
    path = PurePosixPath(value)
    if (not value or "\0" in value or "\\" in value or path.is_absolute()
            or ".." in path.parts or ".git" in path.parts
            or path.as_posix() != value or value == "."):
        raise PreparationError(f"Unsafe source-relative path: {value!r}")
    return path


def _git(directory: Path, *arguments: str) -> bytes:
    try:
        return subprocess.run(
            ["git", "-C", str(directory), *arguments], check=True,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        ).stdout
    except (OSError, subprocess.CalledProcessError) as exc:
        raise PreparationError("Pinned Git source validation failed.") from exc


def _safe_symlink(path: Path, root: Path) -> str:
    target = os.readlink(path)
    if os.path.isabs(target):
        raise PreparationError(f"Absolute source symlink: {path}")
    try:
        (path.parent / target).resolve(strict=True).relative_to(root.resolve())
    except (ValueError, OSError, RuntimeError) as exc:
        raise PreparationError(f"Escaping, cyclic, or broken source symlink: {path}") from exc
    return target


def _entries(root: Path):
    """Walk without following directory symlinks; reject special file types."""
    for path in sorted(root.iterdir()):
        mode = path.lstat().st_mode
        if stat.S_ISLNK(mode):
            yield path
        elif stat.S_ISDIR(mode):
            yield path
            yield from _entries(path)
        elif stat.S_ISREG(mode):
            yield path
        else:
            raise PreparationError(f"Special source file is forbidden: {path}")


def tree_sha256(root: Path) -> str:
    """Hash relative names, kinds, permissions, and bytes (not timestamps)."""
    digest = hashlib.sha256()
    for path in sorted(_entries(root), key=lambda item: item.relative_to(root).as_posix()):
        mode = path.lstat().st_mode
        relative = path.relative_to(root).as_posix().encode("utf-8")
        if stat.S_ISLNK(mode):
            kind, payload = b"L", _safe_symlink(path, root).encode("utf-8")
        elif stat.S_ISDIR(mode):
            kind, payload = b"D", b""
        else:
            kind, payload = b"F", path.read_bytes()
        digest.update(kind + b"\0" + relative + b"\0")
        digest.update(f"{stat.S_IMODE(mode):o}".encode("ascii") + b"\0")
        digest.update(str(len(payload)).encode("ascii") + b"\0" + payload + b"\0")
    return digest.hexdigest()


def validate_tracked_source(
    upstream: Path, *, relative_roots: tuple[str, ...], anchors: dict[str, str],
) -> Path:
    """Verify exact Git bytes/modes and physical inventory for copied roots.

    Selected trees cannot contain Git submodules; the existing wrk2 validator
    separately handles its pinned recursive LuaJIT checkout. Symlinks must stay
    within their selected subtree so copying never introduces external inputs.
    """
    upstream = upstream.resolve(strict=True)
    roots = tuple(_relative(value) for value in relative_roots)
    if not roots:
        raise PreparationError("At least one tracked source root is required.")
    if _git(upstream, "rev-parse", "HEAD").decode().strip() != UPSTREAM_REVISION:
        raise PreparationError(f"Expected upstream revision {UPSTREAM_REVISION}.")
    if _git(upstream, "status", "--porcelain=v1", "--untracked-files=all"):
        raise PreparationError("The upstream checkout or a submodule is dirty.")
    raw = _git(upstream, "ls-tree", "-r", "-z", "HEAD", "--", *map(str, roots))
    tracked = {}
    for item in raw.split(b"\0"):
        if not item:
            continue
        try:
            header, name = item.split(b"\t", 1)
            mode, kind, blob = header.decode("ascii").split()
            relative = _relative(name.decode("utf-8"))
        except (ValueError, UnicodeError) as exc:
            raise PreparationError("Invalid tracked source inventory.") from exc
        if kind != "blob" or mode not in ("100644", "100755", "120000"):
            raise PreparationError(f"Unsupported tracked entry: {relative}")
        tracked[relative] = (mode, blob)
    if not tracked:
        raise PreparationError("No tracked source files found.")
    for root in roots:
        selected = {name for name in tracked if name == root or root in name.parents}
        if not selected:
            raise PreparationError(f"Untracked source root: {root}")
        path = upstream / root
        expected = set(selected)
        for name in selected:
            expected.update(parent for parent in name.parents if parent == root or root in parent.parents)
        if path.is_dir() and not path.is_symlink():
            actual = {root, *(PurePosixPath(p.relative_to(upstream).as_posix()) for p in _entries(path))}
        else:
            actual = {root} if os.path.lexists(path) else set()
        if actual != expected:
            extras, missing = sorted(actual - expected), sorted(expected - actual)
            detail = f"unexpected {extras[0]}" if extras else f"missing {missing[0]}"
            raise PreparationError(f"Source differs from pinned Git inventory: {detail}")
        for name in selected:
            entry = upstream / name
            mode, blob = tracked[name]
            physical_mode = entry.lstat().st_mode
            if mode == "120000":
                if not stat.S_ISLNK(physical_mode):
                    raise PreparationError(f"Tracked symlink type drifted: {name}")
                payload = _safe_symlink(entry, path if path.is_dir() else path.parent).encode("utf-8")
            else:
                if not stat.S_ISREG(physical_mode) or bool(physical_mode & 0o111) != (mode == "100755"):
                    raise PreparationError(f"Tracked file type or executable mode drifted: {name}")
                payload = entry.read_bytes()
            git_blob = hashlib.sha1(b"blob " + str(len(payload)).encode() + b"\0" + payload).hexdigest()
            if git_blob != blob:
                raise PreparationError(f"Tracked source bytes drifted: {name}")
    for relative, expected_hash in anchors.items():
        name = _relative(relative)
        if name not in tracked or tracked[name][0] == "120000" or sha256(upstream / name) != expected_hash:
            raise PreparationError(f"Pinned source anchor drifted: {relative}")
    return upstream


def replace_exact(text: str, old: str, new: str, *, label: str, count: int = 1) -> str:
    if not old or count < 1 or text.count(old) != count:
        raise PreparationError(f"{label} patch anchor drifted.")
    return text.replace(old, new)


def _overlaps(left: Path, right: Path) -> bool:
    return left == right or left in right.parents or right in left.parents


@contextmanager
def candidate_output(upstream: Path, output: Path, *, protected_paths=()):
    """Create a fresh output, removing only this directory on preparation failure."""
    supplied = output.absolute()
    if os.path.lexists(supplied):
        raise PreparationError("Candidate output must not already exist (including symlinks).")
    output = supplied.resolve()
    for source in (upstream, *protected_paths):
        if _overlaps(output, Path(source).resolve()):
            raise PreparationError("Candidate output overlaps a protected source input.")
    if not output.parent.is_dir():
        raise PreparationError("Candidate output parent must already exist.")
    try:
        output.mkdir(mode=0o755)
    except OSError as exc:
        raise PreparationError("Cannot create fresh candidate output.") from exc
    created = output.lstat()
    identity = (created.st_dev, created.st_ino)
    try:
        yield output
    except BaseException as exc:
        # Never remove a different directory/symlink substituted at this path.
        # The original directory may have been moved, so preserve it as well.
        try:
            current = output.lstat()
        except FileNotFoundError:
            current = None
        if current is not None:
            if (not stat.S_ISDIR(current.st_mode)
                    or (current.st_dev, current.st_ino) != identity):
                raise PreparationError(
                    "Candidate output was replaced; cleanup refused and replacement preserved."
                ) from exc
            shutil.rmtree(output)
        raise


def copy_tracked_tree(upstream: Path, relative: str, destination: Path, *, exclude: tuple[str, ...] = ()) -> None:
    """Copy a previously validated tree/file without dereferencing symlinks."""
    source = upstream / _relative(relative)
    excluded = tuple(_relative(value) for value in exclude)
    if os.path.lexists(destination):
        raise PreparationError(f"Copy destination already exists: {destination}")
    destination.parent.mkdir(parents=True, exist_ok=True)
    if source.is_file() and not source.is_symlink():
        shutil.copy2(source, destination)
        destination.chmod(0o755 if source.stat().st_mode & 0o111 else 0o644)
        return
    if source.is_symlink() or not source.is_dir():
        raise PreparationError(f"Copy root must be a regular file or directory: {relative}")
    destination.mkdir(mode=0o755)
    for path in _entries(source):
        name = PurePosixPath(path.relative_to(source).as_posix())
        if ".git" in name.parts or any(name == value or value in name.parents for value in excluded):
            continue
        target = destination / name
        mode = path.lstat().st_mode
        if stat.S_ISLNK(mode):
            link = _safe_symlink(path, source)
            resolved_name = PurePosixPath((path.parent / link).resolve().relative_to(source.resolve()).as_posix())
            if any(resolved_name == value or value in resolved_name.parents for value in excluded):
                raise PreparationError("A copied symlink targets an excluded source.")
            target.symlink_to(link)
        elif stat.S_ISDIR(mode):
            target.mkdir(mode=0o755)
        else:
            shutil.copy2(path, target)
            target.chmod(0o755 if mode & 0o111 else 0o644)
