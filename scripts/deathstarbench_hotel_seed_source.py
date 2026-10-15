"""Pure typed Hotel seed-source extraction from the exact prepared v1 app.

This module does not write files, execute Go, contact databases or networks,
publish images, or grant initialization/runtime authority. The caller must
validate the complete v1 preparation first; all eight seed files are checked
again here against independent complete prepared-source SHA-256 anchors.
Original Go structs, pointer values and loops are retained, not evaluated or
re-encoded through Python/JSON. Exactly six loop-coordinate expressions gain
explicit typed operation-rounding barriers, preventing architecture-dependent
fused multiply-add construction without rounding stored/comparison values.
"""

from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import re
import stat

from scripts.deathstarbench_artifact_common import PreparationError


GENERATOR_REVISION = 'hotel-typed-seed-source-candidate-v2'
ARITHMETIC_POLICY_REVISION = 'hotel-seed-explicit-ieee754-operation-rounding-v1'
MODULE = 'github.com/delimitrou/DeathStarBench/tree/master/hotelReservation'
MAX_SOURCE_BYTES = 128 * 1024
PREPARED_SOURCE_SHA256 = {
    'cmd/attractions/db.go': 'be72ded0c1cadc8c515457fda790aa936e7cde307aca2fbf4dcf4d28d0015de2',
    'cmd/geo/db.go': '1080bc0fe779b9d2523d2289bc52ef7a90fbe9be98da2a730875617003c49219',
    'cmd/profile/db.go': 'cbfa8112e162e62696a28a2bdf23245368ac8f7165dd5ce5bdcd939b23da7191',
    'cmd/rate/db.go': 'ca75806fddf2d6119321de18854672abfd60a988e3de9b7949db6db5c8d66be2',
    'cmd/recommendation/db.go': 'c921b415b63e0ed69d1ccecfdfde8d8671cb268419508df95b827fd9c65899b7',
    'cmd/reservation/db.go': 'c4c8858fd1a6101c65ac898835da9a4a731861c8b9369100f2177e28414a013e',
    'cmd/review/db.go': '49bc45dd35e5caf27cdb8dcc3b7422e78f2d1db0aa0a7cef98bea17dd80cd740',
    # Prepared v1's deliberate decimal seededUsername correction, not upstream.
    'cmd/user/db.go': 'eb0244f1390b9277d2f2ebfb83f140f638cb8d431505c8faac4d43ef62ffa4a7',
}
COMPONENTS = tuple(sorted(path.split('/')[1] for path in PREPARED_SOURCE_SHA256))
COMMANDS = frozenset((*COMPONENTS, 'frontend', 'search'))
COLLECTION_SPECS = {
    'attractions': (('hotels', 'newPoints', 6), ('museums', 'newMuseums', 6), ('restaurants', 'newRestaurants', 6)),
    'geo': (('geo', 'newPoints', 80),),
    'profile': (('hotels', 'newProfiles', 80),),
    'rate': (('inventory', 'newRatePlans', 27),),
    'recommendation': (('recommendation', 'newHotels', 80),),
    'reservation': (('number', 'newNumbers', 80), ('reservation', 'newReservations', 1)),
    'review': (('reviews', 'newReviews', 6),),
    'user': (('user', 'newUsers', 501),),
}
_IMPORTS = {
    'attractions': (), 'geo': ('strconv',), 'profile': ('fmt', 'strconv'),
    'rate': ('fmt', 'strconv'), 'recommendation': ('strconv',),
    'reservation': ('strconv',), 'review': (), 'user': ('crypto/sha256', 'fmt', 'strconv'),
}
_FUNCTION = 'func initializeDatabase(url string) (*mongo.Client, func()) {\n'
_URI = '\turi := fmt.Sprintf("mongodb://%s", url)\n'
_GENERATING = '\tlog.Info().Msg("Generating test data...")\n'
_PREFIX = 'package main\n\nimport (\n'


def arithmetic_policy() -> dict:
    """Detached exact source arithmetic contract, not a numeric data digest.

    Go permits FMA even across assignments. Explicit conversions require the
    quotient, product and sum to round to their declared IEEE-754 type:
    https://go.dev/ref/spec#Floating_point_operators
    """
    expressions = []
    for component, kind in (('geo', 'float64'), ('profile', 'float32'), ('recommendation', 'float64')):
        for variable, base, scale in (('lat', '37.7835', '3'), ('lon', '-122.41', '4')):
            original = f'{base} + {kind}(i)/500.0*{scale}'
            replacement = f'{kind}({kind}({base}) + {kind}({kind}({kind}(i) / {kind}(500.0)) * {kind}({scale})))'
            expressions.append({'component': component, 'variable': variable, 'float_type': kind,
                                'original': original, 'replacement': replacement})
    return {'revision': ARITHMETIC_POLICY_REVISION,
            'rule': 'explicit-target-type-conversions-after-division-product-and-sum',
            'scope': 'six-generated-loop-coordinate-expressions-only', 'expressions': expressions}


def _require(condition, message):
    if not condition:
        raise PreparationError(message)


def _sha(raw):
    return hashlib.sha256(raw).hexdigest()


def _canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False) + '\n'


def _directory(path):
    details = path.lstat()
    _require(stat.S_ISDIR(details.st_mode), 'Hotel seed source requires real directories, not links.')


def _source(app, relative):
    parts = relative.split('/')
    _require(len(parts) == 3 and parts[0] == 'cmd' and parts[1] in COMPONENTS and parts[2] == 'db.go',
             'Unknown Hotel seed source path.')
    _directory(app / parts[0] / parts[1])
    path = app / relative
    before = path.lstat()
    _require(stat.S_ISREG(before.st_mode) and before.st_nlink == 1 and 0 < before.st_size <= MAX_SOURCE_BYTES,
             'Hotel seed source must be a bounded regular file, not a link or special file.')
    with os.fdopen(os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK), 'rb') as stream:
        opened = os.fstat(stream.fileno())
        _require((opened.st_dev, opened.st_ino) == (before.st_dev, before.st_ino),
                 'Hotel seed source changed while opening.')
        raw = stream.read(MAX_SOURCE_BYTES + 1)
    after = path.lstat()
    _require((after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns) ==
             (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns)
             and len(raw) == before.st_size and len(raw) <= MAX_SOURCE_BYTES,
             'Hotel seed source changed while reading.')
    digest = _sha(raw)
    _require(digest == PREPARED_SOURCE_SHA256.get(relative), 'Complete prepared Hotel seed source SHA-256 drifted: ' + relative)
    return raw.decode('utf-8'), digest


def _targets(component, suffix):
    """Bind every retained row variable to the original audited insert target."""
    specs = COLLECTION_SPECS[component]
    _require(suffix.count('.InsertMany(') == len(specs) and suffix.count('.Collection(') == len(specs)
             and suffix.count('.Database(') == (3 if component == 'attractions' else 1),
             'Hotel seed collection/write inventory drifted.')
    if component == 'reservation':
        anchors = (
            'database := client.Database("reservation-db")',
            'numCollection := database.Collection("number")',
            'resCollection := database.Collection("reservation")',
            '_, err = numCollection.InsertMany(context.TODO(), newNumbers)',
            '_, err = resCollection.InsertMany(context.TODO(), newReservations)',
        )
    else:
        variables = {'hotels': 'collectionH', 'museums': 'collectionM', 'restaurants': 'collectionR'}
        anchors = tuple(anchor for name, rows, _count in specs for anchor in (
            f'{variables[name] if component == "attractions" else "collection"} := client.Database("{component}-db").Collection("{name}")',
            f'_, err = {variables[name] if component == "attractions" else "collection"}.InsertMany(context.TODO(), {rows})',
        ))
    for anchor in anchors:
        _require(suffix.count(anchor) == 1, 'Hotel seed insert-target anchor drifted: ' + component)


def _extract(component, text):
    _require(text.startswith(_PREFIX) and text.count(_FUNCTION) == 1 and text.count(_URI) == 1,
             'Hotel seed package/function/URI extraction boundary drifted: ' + component)
    import_end = text.find('\n)\n\n', len(_PREFIX))
    start, end = text.index(_FUNCTION), text.index(_URI)
    _require(import_end >= 0 and import_end + len('\n)\n\n') < start < end,
             'Hotel seed declarations or construction boundary is invalid.')
    declarations = text[import_end + len('\n)\n\n'):start].strip('\n')
    construction = text[start + len(_FUNCTION):end]
    expected_log_count = 0 if component in {'attractions', 'review'} else 1
    _require(construction.count(_GENERATING) == expected_log_count,
             'Hotel seed construction log anchor drifted: ' + component)
    construction = construction.replace(_GENERATING, '').strip('\n')
    _require(declarations.startswith('type ') and bool(construction), 'Hotel seed typed declarations or rows are missing.')
    _require(not re.search(r'\b(?:mongo|context|log|os|exec|net)\s*\.|\bfunc\s+init\s*\(', declarations + '\n' + construction),
             'Hotel seed extraction retained runtime effects.')
    if component == 'user':
        _require(declarations.count('func seededUsername(suffix string) string {') == 1
                 and declarations.count('return fmt.Sprintf("Cornell_%s", suffix)') == 1
                 and construction.count('seededUsername(suffix)') == 1,
                 'Hotel decimal user seed helper drifted.')
    else:
        _require('func ' not in declarations, 'Unexpected helper in Hotel typed seed declarations.')
    for _name, rows, _count in COLLECTION_SPECS[component]:
        _require(construction.count(rows + ' := []interface{}{') == 1,
                 'Hotel seed row-variable declaration drifted: ' + component)
    _targets(component, text[end:])
    return declarations, construction


def _round_coordinates(component, construction):
    expressions = [item for item in arithmetic_policy()['expressions'] if item['component'] == component]
    # Reject missing/additional assignments, compound mutations and wrong
    # operand types even if a test substitutes the trusted complete-file SHA.
    assignments = re.findall(r'(?m)^[\t ]*(lat|lon)[\t ]*(?::=|[+*/-]?=)[\t ]*([^\n]*)$', construction)
    expected = [(item['variable'], item['original']) for item in expressions]
    _require(assignments == expected, 'Hotel loop-coordinate arithmetic inventory/type drifted: ' + component)
    generated = construction
    for item in expressions:
        before = f'\t\t{item["variable"]} := {item["original"]}'
        after = f'\t\t{item["variable"]} := {item["replacement"]}'
        _require(generated.count(before) == 1, 'Hotel coordinate arithmetic assignment anchor drifted: ' + component)
        generated = generated.replace(before, after, 1)
    return generated, len(expressions)


def _generate(v1_app):
    try:
        app = Path(v1_app)
        _require('..' not in app.parts, 'Hotel seed source root must not contain traversal.')
        _directory(app)
        _directory(app / 'cmd')
        _require({entry.name for entry in (app / 'cmd').iterdir()} == COMMANDS,
                 'Hotel command inventory contains missing or unknown components.')
        for component in COMMANDS:
            _directory(app / 'cmd' / component)
            if component not in COMPONENTS:
                _require(not (app / 'cmd' / component / 'db.go').exists()
                         and not (app / 'cmd' / component / 'db.go').is_symlink(),
                         'Unexpected Hotel seeder in an application-only command.')
        generated, sources, collections = {}, {}, []
        rounded_expression_count = 0
        for component in COMPONENTS:
            relative = f'cmd/{component}/db.go'
            text, digest = _source(app, relative)
            declarations, construction = _extract(component, text)
            generated_construction, rounded_count = _round_coordinates(component, construction)
            rounded_expression_count += rounded_count
            specs = COLLECTION_SPECS[component]
            entries = [{'component': component, 'database': component + '-db', 'name': name,
                        'documents_variable': rows, 'expected_documents': count} for name, rows, count in specs]
            collections.extend(entries)
            imports = [*(_IMPORTS[component]), MODULE + '/initialization']
            import_block = '\n'.join('\t' + json.dumps(value) for value in imports)
            returns = '\n'.join(
                f'\t\t{{Component: "{component}", Database: "{component}-db", Name: "{name}", Documents: {rows}}},'
                for name, rows, _count in specs)
            path = f'initialization/seeddata/{component}/seed.go'
            generated[path] = (
                f'// Code generated by {GENERATOR_REVISION}; DO NOT EDIT.\n'
                f'// Exact prepared source: {relative} sha256:{digest}. Candidate only.\n'
                f'package {component}\n\nimport (\n{import_block}\n)\n\n{declarations}\n\n'
                f'func Collections() []initialization.Collection {{\n{generated_construction}\n\n'
                f'\treturn []initialization.Collection{{\n{returns}\n\t}}\n}}\n'
            )
            sources[relative] = {'sha256': digest, 'declarations_sha256': _sha(declarations.encode()),
                                 'original_construction_sha256': _sha(construction.encode()),
                                 'generated_construction_sha256': _sha(generated_construction.encode()), 'collections': entries}
        _require(rounded_expression_count == 6, 'Exactly six Hotel coordinate expressions must receive typed rounding barriers.')
        fingerprint_input = {'generator_revision': GENERATOR_REVISION,
                             'source_files': {path: receipt['sha256'] for path, receipt in sources.items()},
                             'collections': collections, 'arithmetic_policy': arithmetic_policy()}
        source_fingerprint = 'sha256:' + _sha(_canonical(fingerprint_input).encode())
        imports = '\n'.join('\t' + json.dumps(MODULE + '/initialization/seeddata/' + component) for component in COMPONENTS)
        appends = '\n'.join(f'\tcollections = append(collections, {component}.Collections()...)' for component in COMPONENTS)
        generated['initialization/catalog/catalog.go'] = (
            f'// Code generated by {GENERATOR_REVISION}; DO NOT EDIT. Candidate only.\n'
            f'package catalog\n\nimport (\n\t"{MODULE}/initialization"\n{imports}\n)\n\n'
            f'const SourceFingerprint = "{source_fingerprint}"\n\n'
            'func Collections() []initialization.Collection {\n'
            '\tcollections := make([]initialization.Collection, 0, 11)\n'
            f'{appends}\n\treturn collections\n}}\n'
        )
        receipts = {'schema_version': 1, 'generator_revision': GENERATOR_REVISION,
                    'candidate_only': True, 'released': False, 'qualification_authority': False,
                    'arithmetic_policy': arithmetic_policy(),
                    'source_fingerprint': source_fingerprint, 'source_fingerprint_input': fingerprint_input,
                    'source_files': sources,
                    'generated_source_sha256': {path: _sha(text.encode()) for path, text in sorted(generated.items())}}
        return dict(sorted(generated.items())), receipts
    except (OSError, UnicodeError, TypeError, ValueError) as exc:
        raise PreparationError('Cannot validate exact prepared Hotel seed sources.') from exc


def generate_seed_sources(v1_app: Path) -> dict[str, str]:
    """Return nine generated relative paths/texts after validating all inputs."""
    return _generate(v1_app)[0]


def seed_source_receipts(v1_app: Path) -> dict:
    """Return deterministic source/generator bindings, never row-content proof."""
    return _generate(v1_app)[1]
