#!/usr/bin/env python3
"""Prepare an additive Hotel seed/verify recipe; never execute or publish it.

The retained v1 recipe remains byte-for-byte unchanged and adoptable. This
distinct preparation is not accepted by the current candidate lock factory or
support operator. Native publication, new adoption and a run-bound controller
remain mandatory before any initializer or application may be dispatched.
"""

from __future__ import annotations

import argparse
from contextlib import contextmanager
import hashlib
import json
import os
from pathlib import Path
from pathlib import PurePosixPath
import stat
import sys

if __package__ in (None, ''):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.deathstarbench_hotel_initialization import (
    CONTRACT_REVISION, DATASET_REVISION, RECIPE_REVISION, hotel_initialization_policy,
)
from scripts import prepare_deathstarbench_hotel_images as legacy
from scripts.deathstarbench_artifact_common import (
    PreparationError, candidate_output, replace_exact,
)
from scripts.deathstarbench_hotel_seed_source import generate_seed_sources, seed_source_receipts


ASSET_ROOT = Path(__file__).resolve().parent / 'assets/deathstarbench/hotel-initialization-v1'
MODULE = 'github.com/delimitrou/DeathStarBench/tree/master/hotelReservation'
PREPARATION_SCHEMA = 'hotel-guarded-initialization-preparation-v1'
PATCH_REVISION = 'hotel-reservation-no-startup-seeding-candidate-v1'
COMPONENTS = ('attractions', 'geo', 'profile', 'rate', 'recommendation', 'reservation', 'review', 'user')
APPLICATIONS = tuple(sorted((*COMPONENTS, 'frontend', 'search')))
CONFIG_KEYS = {
    'attractions': 'AttractionsMongoAddress', 'geo': 'GeoMongoAddress',
    'profile': 'ProfileMongoAddress', 'rate': 'RateMongoAddress',
    'recommendation': 'RecommendMongoAddress', 'reservation': 'ReserveMongoAddress',
    'review': 'ReviewMongoAddress', 'user': 'UserMongoAddress',
}


def _identity(details):
    return details.st_dev, details.st_ino, details.st_mode


def _signature(details):
    return (*_identity(details), details.st_nlink, details.st_size,
            details.st_mtime_ns, details.st_ctime_ns)


def _relative(value):
    path = PurePosixPath(value)
    if (not value or '\0' in value or '\\' in value or path.is_absolute()
            or '..' in path.parts or path.as_posix() != value or value == '.'):
        raise PreparationError('Unsafe generated Hotel initialization path.')
    return path.parts


class _GuardedFiles:
    """Local preparation guard, not a concurrent-writer or runtime authority.

    All I/O is relative to held real-directory descriptors. Input bytes are
    read once with identity checks; an already observed file is never reopened
    for truncation unless its exact identity still matches. A replaced path
    therefore cannot redirect a write into an unrelated file or directory.
    """

    def __init__(self, root, *, capture=True, writable=False, max_file_bytes=128 * 1024 * 1024):
        if type(max_file_bytes) is not int or max_file_bytes < 1:
            raise PreparationError('Hotel preparation file bound must be a positive integer.')
        self.root = root.absolute()
        self.writable = writable
        self.max_file_bytes = max_file_bytes
        self.parents = {path: _identity(path.lstat()) for path in (*reversed(self.root.parents), self.root)}
        if any(not stat.S_ISDIR(value[2]) for value in self.parents.values()):
            raise PreparationError('Hotel preparation requires real input/output directories, not links.')
        self.fd = os.open(self.root, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        self.directories = {(): _identity(os.fstat(self.fd))}
        self.files = {}
        try:
            self.check_root()
            if writable:
                os.fchmod(self.fd, 0o755)
                self.parents[self.root] = self.directories[()] = _identity(os.fstat(self.fd))
            if capture:
                self.capture()
        except BaseException:
            os.close(self.fd)
            raise

    def __enter__(self):
        return self

    def __exit__(self, *args):
        os.close(self.fd)

    def check_root(self):
        try:
            if any(_identity(path.lstat()) != expected for path, expected in self.parents.items()):
                raise PreparationError('Hotel preparation root or ancestor directory was replaced.')
            if _identity(os.fstat(self.fd)) != self.parents[self.root]:
                raise PreparationError('Hotel preparation root changed while opening.')
        except OSError as exc:
            raise PreparationError('Hotel preparation directory is unavailable.') from exc

    @contextmanager
    def directory(self, parts=(), *, create=False):
        self.check_root()
        descriptor = os.dup(self.fd)
        try:
            for index, name in enumerate(parts):
                relative = tuple(parts[:index + 1])
                created = False
                try:
                    details = os.stat(name, dir_fd=descriptor, follow_symlinks=False)
                    if create and relative not in self.directories:
                        raise PreparationError('Unexpected existing Hotel preparation output directory.')
                except FileNotFoundError:
                    if not create or relative in self.directories:
                        raise PreparationError('Hotel preparation directory disappeared.')
                    os.mkdir(name, mode=0o755, dir_fd=descriptor)
                    details = os.stat(name, dir_fd=descriptor, follow_symlinks=False)
                    created = True
                if (not stat.S_ISDIR(details.st_mode)
                        or self.directories.get(relative, _identity(details)) != _identity(details)):
                    raise PreparationError('Hotel preparation directory identity or type changed.')
                opened = os.open(name, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=descriptor)
                if _identity(os.fstat(opened)) != _identity(details):
                    os.close(opened)
                    raise PreparationError('Hotel preparation directory changed while opening.')
                if created:
                    os.fchmod(opened, 0o755)
                    details = os.fstat(opened)
                self.directories[relative] = _identity(details)
                os.close(descriptor)
                descriptor = opened
            yield descriptor
            self.check_root()
        except OSError as exc:
            raise PreparationError('Unsafe or changed Hotel preparation path.') from exc
        finally:
            os.close(descriptor)

    def read(self, relative):
        parts = _relative(relative)
        with self.directory(parts[:-1]) as parent:
            before = os.stat(parts[-1], dir_fd=parent, follow_symlinks=False)
            if (not stat.S_ISREG(before.st_mode) or before.st_nlink != 1
                    or before.st_size > self.max_file_bytes
                    or self.files.get(relative, _signature(before)) != _signature(before)):
                raise PreparationError('Hotel preparation input identity, type or size changed.')
            descriptor = os.open(parts[-1], os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=parent)
            try:
                if _signature(os.fstat(descriptor)) != _signature(before):
                    raise PreparationError('Hotel preparation input changed while opening.')
                with os.fdopen(os.dup(descriptor), 'rb') as stream:
                    raw = stream.read(self.max_file_bytes + 1)
                after = os.stat(parts[-1], dir_fd=parent, follow_symlinks=False)
                if (_signature(os.fstat(descriptor)) != _signature(before)
                        or _signature(after) != _signature(before) or len(raw) != before.st_size):
                    raise PreparationError('Hotel preparation input changed while reading.')
            finally:
                os.close(descriptor)
            self.files[relative] = _signature(before)
            return raw

    def _inventory(self):
        files, directories = set(), {()}

        def visit(parts):
            with self.directory(parts) as descriptor:
                names = sorted(os.listdir(descriptor))
                for name in names:
                    relative = (*parts, name)
                    details = os.stat(name, dir_fd=descriptor, follow_symlinks=False)
                    if stat.S_ISDIR(details.st_mode):
                        directories.add(relative)
                        visit(relative)
                    elif stat.S_ISREG(details.st_mode):
                        value = '/'.join(relative)
                        files.add(value)
                        self.read(value)
                    else:
                        raise PreparationError('Hotel preparation inputs contain a link or special file.')
                if names != sorted(os.listdir(descriptor)):
                    raise PreparationError('Hotel preparation inventory changed while reading.')

        visit(())
        return files, directories

    def capture(self):
        files, directories = self._inventory()
        if files != set(self.files) or directories != set(self.directories):
            raise PreparationError('Hotel preparation inventory changed.')

    def verify(self):
        expected = set(self.files), set(self.directories)
        if self._inventory() != expected:
            raise PreparationError('Hotel preparation inventory changed.')

    def write(self, relative, text, *, new=False):
        if not self.writable:
            raise PreparationError('Hotel preparation input is read-only.')
        parts = _relative(relative)
        raw = text.encode('utf-8') if isinstance(text, str) else text
        with self.directory(parts[:-1], create=True) as parent:
            expected = self.files.get(relative)
            if new and expected is not None:
                raise PreparationError('Hotel preparation output paths overlap.')
            flags = os.O_WRONLY | os.O_NOFOLLOW | os.O_NONBLOCK
            if expected is None:
                flags |= os.O_CREAT | os.O_EXCL
            descriptor = os.open(parts[-1], flags, 0o644, dir_fd=parent)
            try:
                details = os.fstat(descriptor)
                if (not stat.S_ISREG(details.st_mode) or details.st_nlink != 1
                        or expected is not None and _signature(details) != expected):
                    raise PreparationError('Hotel preparation output file was replaced.')
                os.ftruncate(descriptor, 0)
                with os.fdopen(os.dup(descriptor), 'wb') as stream:
                    stream.write(raw)
                os.fchmod(descriptor, 0o644)
                details = os.fstat(descriptor)
                if _signature(os.stat(parts[-1], dir_fd=parent, follow_symlinks=False)) != _signature(details):
                    raise PreparationError('Hotel preparation output changed while writing.')
                self.files[relative] = _signature(details)
            finally:
                os.close(descriptor)

    def digest(self, relative):
        return hashlib.sha256(self.read(relative)).hexdigest()

    def tree_digest(self, relative=None):
        prefix = _relative(relative) if relative is not None else ()
        digest = hashlib.sha256()
        entries = [(parts, b'D', b'', self.directories[parts][2])
                   for parts in self.directories if parts[:len(prefix)] == prefix and parts != prefix]
        entries.extend((tuple(name.split('/')), b'F', self.read(name), details[2])
                       for name, details in self.files.items() if tuple(name.split('/'))[:len(prefix)] == prefix)
        for parts, kind, raw, mode in sorted(entries, key=lambda entry: '/'.join(entry[0][len(prefix):])):
            name = '/'.join(parts[len(prefix):]).encode('utf-8')
            digest.update(kind + b'\0' + name + b'\0' + f'{stat.S_IMODE(mode):o}'.encode('ascii') + b'\0')
            digest.update(str(len(raw)).encode('ascii') + b'\0' + raw + b'\0')
        return digest.hexdigest()

    def take_legacy_output(self):
        with self.directory(('base',)) as parent:
            for source, target, kind in (('app', 'app', stat.S_ISDIR),
                                         ('context-manifest.json', 'base-preparation.json', stat.S_ISREG)):
                if not kind(os.stat(source, dir_fd=parent, follow_symlinks=False).st_mode):
                    raise PreparationError('Legacy Hotel preparation output type changed.')
                try:
                    os.stat(target, dir_fd=self.fd, follow_symlinks=False)
                except FileNotFoundError:
                    pass
                else:
                    raise PreparationError('Unexpected existing Hotel preparation destination.')
            os.rename('app', 'app', src_dir_fd=parent, dst_dir_fd=self.fd)
            os.rename('context-manifest.json', 'base-preparation.json', src_dir_fd=parent, dst_dir_fd=self.fd)
        os.rmdir('base', dir_fd=self.fd)
        self.directories = {(): self.directories[()]}
        self.capture()


def _imports(text: str, additions: str) -> str:
    return replace_exact(text, 'import (\n', 'import (\n' + additions,
                         label='guarded Hotel imports')


def _main(text: str) -> str:
    text = _imports(text, f'\t"{MODULE}/initialization"\n\t"{MODULE}/initialization/catalog"\n')
    preflight = '''func main() {
\tdataset, candidateErr := initialization.BuildDataset(catalog.Collections(), catalog.SourceFingerprint)
\tif candidateErr != nil {
\t\tfmt.Fprintln(os.Stderr, "Invalid candidate Hotel dataset contract")
\t\tos.Exit(2)
\t}
\tif handled, err := initialization.Attest(os.Stdout, os.Args[1:], dataset); handled || err != nil {
\t\tif err != nil { fmt.Fprintln(os.Stderr, "Candidate attestation refused"); os.Exit(2) }
\t\treturn
\t}
\tidentity, candidateErr := initialization.IdentityFromEnv()
\tif candidateErr != nil || identity.ContractSHA256 != dataset.Fingerprint() {
\t\tfmt.Fprintln(os.Stderr, "Explicit candidate Hotel initialization identity is required")
\t\tos.Exit(2)
\t}
'''
    if '\t"fmt"\n' not in text:
        text = _imports(text, '\t"fmt"\n')
    return replace_exact(text, 'func main() {\n', preflight, label='no-DB Hotel preflight before tune/config')


def _service_database(component: str) -> str:
    # The original signature stays compatible with the ten upstream commands;
    # there is deliberately no write/seed mode or environment fallback here.
    helper = ('\nfunc seededUsername(suffix string) string {\n'
              '\treturn fmt.Sprintf("Cornell_%s", suffix)\n}\n') if component == 'user' else ''
    return f'''package main

import (
    "context"
    "fmt"
    "time"
    "{MODULE}/initialization"
    "{MODULE}/initialization/catalog"
    "go.mongodb.org/mongo-driver/mongo"
)

func initializeDatabase(address string) (*mongo.Client, func()) {{
    dataset, err := initialization.BuildDataset(catalog.Collections(), catalog.SourceFingerprint)
    if err != nil {{ panic("Invalid candidate Hotel dataset contract") }}
    identity, err := initialization.IdentityFromEnv()
    if err != nil {{ panic("Explicit candidate Hotel identity is required") }}
    ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
    defer cancel()
    client, close, err := initialization.ConnectAndVerify(ctx, "{component}", address, identity, dataset)
    if err != nil {{ panic(fmt.Sprintf("Candidate Hotel {component} baseline verification failed: %v", err)) }}
    return client, close
}}
{helper}'''


def _initializer(config_hash: str) -> str:
    mapping = '\n'.join(f'        "{component}": config["{key}"],' for component, key in sorted(CONFIG_KEYS.items()))
    return f'''package main

import (
    "context"
    "crypto/sha256"
    "encoding/hex"
    "encoding/json"
    "fmt"
    "io"
    "os"
    "time"
    "{MODULE}/initialization"
    "{MODULE}/initialization/catalog"
)

func run() error {{
    dataset, err := initialization.BuildDataset(catalog.Collections(), catalog.SourceFingerprint)
    if err != nil {{ return fmt.Errorf("invalid Hotel dataset contract") }}
    if handled, err := initialization.Attest(os.Stdout, os.Args[1:], dataset); handled || err != nil {{ return err }}
    if len(os.Args) != 2 || (os.Args[1] != "initialize-empty" && os.Args[1] != "verify") {{
        return fmt.Errorf("explicit initialize-empty or read-only verify mode is required")
    }}
    identity, err := initialization.IdentityFromEnv()
    if err != nil || identity.ContractSHA256 != dataset.Fingerprint() {{ return fmt.Errorf("explicit Hotel identity is required") }}
    file, err := os.Open("config.json")
    if err != nil {{ return fmt.Errorf("baked configuration unavailable") }}
    defer file.Close()
    raw, err := io.ReadAll(io.LimitReader(file, 65537))
    if err != nil || len(raw) > 65536 {{ return fmt.Errorf("baked configuration invalid") }}
    sum := sha256.Sum256(raw)
    if hex.EncodeToString(sum[:]) != "{config_hash}" {{ return fmt.Errorf("baked configuration identity differs") }}
    var config map[string]string
    if json.Unmarshal(raw, &config) != nil {{ return fmt.Errorf("baked configuration invalid") }}
    addresses := map[string]string{{
{mapping}
    }}
    for component, address := range addresses {{
        if address != "mongodb-"+component+":27017" {{ return fmt.Errorf("fixed Hotel store address differs") }}
    }}
    ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
    defer cancel()
    stores, close, err := initialization.OpenMongoStores(ctx, addresses)
    if err != nil {{ return fmt.Errorf("Hotel stores unavailable") }}
    defer close()
    if os.Args[1] == "initialize-empty" {{
        if err := initialization.Initialize(ctx, stores, identity, dataset); err != nil {{ return err }}
    }}
    if err := initialization.Verify(ctx, stores, identity, dataset); err != nil {{ return err }}
    // This is local native evidence, not a controller/Job/namespace identity
    // attestation or permission to start applications or measurements.
    return json.NewEncoder(os.Stdout).Encode(map[string]interface{{}}{{
        "candidate_only": true, "released": false, "application_start_allowed": false,
        "runtime_qualified": false, "measurement_qualified": false,
        "run_id": identity.RunID, "contract_sha256": dataset.Fingerprint(),
        "mode": os.Args[1], "database_verification_completed": true,
    }})
}}

func main() {{
    if err := run(); err != nil {{ fmt.Fprintln(os.Stderr, "Candidate Hotel initialization/verification refused"); os.Exit(2) }}
}}
'''


def _dockerfile() -> str:
    value = legacy._dockerfile().replace(legacy.RECIPE_REVISION, RECIPE_REVISION)
    value = replace_exact(value, 'COPY config.json config.json\n',
                          'COPY initialization/ initialization/\nCOPY config.json config.json\n', label='initializer build inputs')
    return replace_exact(value, ' '.join(legacy.GO_TEST_PACKAGES),
                         ' '.join((*legacy.GO_TEST_PACKAGES, './initialization/...', './cmd/...')),
                         label='guarded Hotel offline native tests')


def prepare_contexts(upstream: Path, output: Path) -> dict:
    # Preserve the old preparer, its output hashes and its default API. The old
    # receipt is retained as explicitly named input evidence, not an adoptable
    # v1 context next to the newly patched app directory.
    with candidate_output(upstream, output, protected_paths=(ASSET_ROOT, Path(__file__))) as destination, \
            _GuardedFiles(destination, capture=False, writable=True) as owned, \
            _GuardedFiles(ASSET_ROOT) as assets:
        base = legacy.prepare_contexts(upstream, destination / 'base')
        owned.take_legacy_output()
        app = destination / 'app'
        if (json.loads(owned.read('base-preparation.json')) != base
                or owned.tree_digest('app') != base['contexts']['app']['context_sha256']
                or owned.digest('app/config.json') != base['config_sha256']):
            raise PreparationError('Legacy Hotel preparation changed before guarded adoption.')
        # Preserve the exact retained receipt bytes with a canonical file mode,
        # even when the caller's umask restricted the legacy receipt creation.
        owned.write('base-preparation.json', owned.read('base-preparation.json'))
        source_receipts = seed_source_receipts(app)
        generated = generate_seed_sources(app)
        owned.verify()
        expected_generated = source_receipts['generated_source_sha256']
        if expected_generated != {name: hashlib.sha256(text.encode('utf-8')).hexdigest() for name, text in generated.items()}:
            raise PreparationError('Hotel seed source changed between validation and generation.')
        for relative, text in generated.items():
            parts = _relative(relative)
            if len(parts) < 2 or parts[0] != 'initialization':
                raise PreparationError('Unsafe generated Hotel initialization path.')
            owned.write('app/' + relative, text, new=True)
        asset_hashes = {}
        if set(assets.directories) != {(), ('initialization',)}:
            raise PreparationError('Unexpected Hotel initialization source asset directory.')
        for relative in sorted(assets.files):
            parts = _relative(relative)
            if parts[-1] == '.DS_Store' and len(parts) <= 2:
                continue
            if len(parts) != 2 or parts[0] != 'initialization' or not parts[1].endswith('.go'):
                raise PreparationError('Unexpected or unsafe Hotel initialization source asset.')
            raw = assets.read(relative)
            asset_hashes[relative] = hashlib.sha256(raw).hexdigest()
            owned.write('app/' + relative, raw, new=True)
        if not asset_hashes:
            raise PreparationError('Hotel initialization library assets are missing.')
        inventory = {item['path']: item['sha256'] for item in base['source_inventory']}
        patches = {}
        for component in APPLICATIONS:
            relative = f'cmd/{component}/main.go'
            raw = owned.read('app/' + relative)
            original = hashlib.sha256(raw).hexdigest()
            if original != inventory.get('hotelReservation/' + relative):
                raise PreparationError('Hotel command source differs from the validated upstream inventory.')
            owned.write('app/' + relative, _main(raw.decode('utf-8')))
            patches[relative] = {'original_sha256': original, 'patched_sha256': owned.digest('app/' + relative)}
        for component in COMPONENTS:
            relative = f'cmd/{component}/db.go'
            original = owned.digest('app/' + relative)
            owned.write('app/' + relative, _service_database(component))
            patches[relative] = {'original_sha256': original, 'patched_sha256': owned.digest('app/' + relative)}
        owned.write('app/cmd/hotel-initialize/main.go', _initializer(base['config_sha256']), new=True)
        owned.write('app/Dockerfile.candidate', _dockerfile())
        assets.verify()
        owned.verify()
        manifest = {
            'schema_version': 1, 'candidate_schema': PREPARATION_SCHEMA,
            'candidate_only': True, 'released': False, 'workload_id': 'hotel_reservation',
            'upstream_revision': legacy.UPSTREAM_REVISION,
            'patch_revision': PATCH_REVISION, 'recipe_revision': RECIPE_REVISION,
            'dataset_revision': DATASET_REVISION, 'initialization_revision': CONTRACT_REVISION,
            'initialization_policy': hotel_initialization_policy(),
            'legacy_base_preparation_sha256': owned.digest('base-preparation.json'),
            'legacy_base_context_sha256': base['contexts']['app']['context_sha256'],
            'seed_source': source_receipts, 'startup_source_patches': patches,
            'initialization_asset_sha256': asset_hashes,
            'initializer_source_sha256': owned.digest('app/cmd/hotel-initialize/main.go'),
            'contexts': {'app': {'path': 'app', 'dockerfile': 'Dockerfile.candidate', 'context_sha256': owned.tree_digest('app')}},
            'software': base['software'], 'license_identities': base['license_identities'],
            'config_sha256': base['config_sha256'], 'native_platforms': base['native_platforms'],
            'images_built': False, 'images_published': False, 'database_executed': False,
            'application_start_allowed': False, 'runtime_qualified': False, 'measurement_qualified': False,
            'current_candidate_factory_accepts_recipe': False,
            'release_blockers': [
                'New native app/initializer publication, smoke, exact artifact adoption and anonymous image access are required.',
                'A new run/namespace/storage/Pod-bound initialization controller and read-only response-loss reconciliation are required.',
                'Per-database markers are not a shared transaction or an exactly-once controller proof.',
                'Consul restart recovery, preload error handling, atomic reservation capacity and post-warmup epochs remain unqualified.',
                'Semantic transactions, measurement, denied-network behavior and public release remain gated.',
            ],
        }
        owned.write('context-manifest.json', json.dumps(manifest, sort_keys=True, indent=2) + '\n', new=True)
        assets.verify()
        owned.verify()
        return manifest


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--upstream', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    print(json.dumps(prepare_contexts(args.upstream, args.output), sort_keys=True))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
