"""Occurrence-checked source changes for the separate offline Hotel startup recipe.

Only startup identity/preload code is changed. Runtime request handlers, database
selectors and types remain the pinned upstream candidate's own implementation.
This module neither executes a service nor authorizes application startup.
"""

from __future__ import annotations

import hashlib

from scripts.deathstarbench_artifact_common import PreparationError, replace_exact


SERVICE_PATHS = tuple(
    f'services/{component}/server.go'
    for component in ('attractions', 'geo', 'profile', 'rate', 'recommendation',
                      'reservation', 'review', 'search', 'user')
)

# Exact old helper blocks, excluding the following type declarations. The new
# helpers are copied as separate reviewed preload.go assets by the new recipe.
_PRELOADS = {
    'geo': ('// newGeoIndex returns a geo index with points loaded\n', '\ntype point struct {',
            '325c61ff195029324f472891e415a788a5714afd68d5d174e13c6d2155049387'),
    'user': ('// loadUsers loads hotel users from mongodb.\n', '\ntype User struct {',
             '06dbd58ff34c82fdf7bbdee6e55af7c11f909ea0e6e2e1a19a062fe3460fc528'),
    'recommendation': ('// loadRecommendations loads hotel recommendations from mongodb.\n',
                       '\ntype Hotel struct {',
                       'dd2375c3947a1e85ae83400bdaaa496c137789bf24a7de6f2df89b2e391237b8'),
    'attractions': ('// newGeoIndex returns a geo index with points loaded\n', '\ntype point struct {',
                    '573e44a99e60d0d24b92e9d6bea714c609046143c7b5692913d8953643130898'),
}


def _preload(text: str, component: str) -> str:
    start, end, expected = _PRELOADS[component]
    if text.count(start) != 1 or text.count(end) != 1:
        raise PreparationError('Hotel startup preload method boundaries drifted.')
    first, last = text.index(start), text.index(end)
    if first >= last:
        raise PreparationError('Hotel startup preload method ordering drifted.')
    original = text[first:last]
    if hashlib.sha256(original.encode('utf-8')).hexdigest() != expected:
        raise PreparationError('Hotel startup preload method bytes drifted.')
    text = replace_exact(text, original, '', label='separate verified Hotel preload helper')
    if component != 'attractions':
        text = replace_exact(text, '\t"go.mongodb.org/mongo-driver/bson"\n', '',
                             label='unused preload BSON import')
    fields = {
        'geo': (('index', 'newGeoIndex'),),
        'user': (('users', 'loadUsers'),),
        'recommendation': (('hotels', 'loadRecommendations'),),
        'attractions': (('indexH', 'newGeoIndex'), ('indexR', 'newGeoIndexRest'),
                        ('indexM', 'newGeoIndexMus'), ('indexC', 'newGeoIndexCinema')),
    }[component]
    for field, helper in fields:
        old = f'\tif s.{field} == nil {{\n\t\ts.{field} = {helper}(s.MongoClient)\n\t}}'
        # There is no pre-populated field bypass: every Run must load verified
        # rows before it can create a listener or register its service identity.
        new = (f'\tcandidate{field}, candidateErr := {helper}(s.MongoClient)\n'
               '\tif candidateErr != nil {\n'
               f'\t\treturn fmt.Errorf("candidate Hotel {component} preload refused")\n'
               '\t}\n'
               f'\ts.{field} = candidate{field}')
        text = replace_exact(text, old, new, label='fail-closed Hotel ' + component + ' preload')
    return text


def patch_service(relative: str, text: str) -> str:
    """Patch one exact, independently source-validated non-frontend service."""
    if relative not in SERVICE_PATHS:
        raise PreparationError('Unexpected Hotel startup service source path.')
    component = relative.split('/')[1]
    text = replace_exact(text, '\t"github.com/google/uuid"\n', '',
                         label='unused random service identity import')
    text = replace_exact(
        text, '\ts.uuid = uuid.New().String()',
        '\tcandidateID, err := registry.ServiceID(name)\n'
        '\tif err != nil {\n'
        '\t\treturn fmt.Errorf("candidate Hotel service identity refused")\n'
        '\t}\n'
        '\ts.uuid = candidateID',
        label='bound Hotel service identity',
    )
    if component in _PRELOADS:
        text = _preload(text, component)
    return text
