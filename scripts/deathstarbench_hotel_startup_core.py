"""Exact additive source patches for the offline guarded Hotel startup recipe.

Only already-guarded copied sources are accepted. These helpers perform no
execution, publication, networking, or application-start authorization.
"""

from __future__ import annotations

import re

from scripts.deathstarbench_artifact_common import PreparationError, replace_exact


MODULE = 'github.com/delimitrou/DeathStarBench/tree/master/hotelReservation'
APPLICATIONS = ('attractions', 'frontend', 'geo', 'profile', 'rate',
                'recommendation', 'reservation', 'review', 'search', 'user')


def patch_main(text: str, component: str, config_sha256: str) -> str:
    """Fail closed on source drift, replacing unchecked config before tuning."""
    if component not in APPLICATIONS or type(component) is not str:
        raise PreparationError('Guarded startup requires an exact Hotel application component.')
    digest = config_sha256.removeprefix('sha256:') if type(config_sha256) is str else ''
    if not re.fullmatch(r'[0-9a-f]{64}', digest) or digest == '0' * 64:
        raise PreparationError('Guarded startup requires the exact baked config SHA-256.')
    text = replace_exact(text, '\t"encoding/json"\n', '', label='guarded startup config JSON import')
    text = replace_exact(text, '\t"io/ioutil"\n', '', label='guarded startup unchecked read import')
    text = replace_exact(text, 'import (\n', 'import (\n\t"' + MODULE + '/startup"\n',
                         label='guarded startup import')
    # The original no-environment initialization attestation is deliberately
    # earlier than this anchor and remains unchanged.
    anchor = '\tidentity, candidateErr := initialization.IdentityFromEnv()\n'
    preflight = f'''\tif handled, err := startup.Attest(os.Stdout, os.Args[1:], "{component}", "{digest}"); handled || err != nil {{
\t\tif err != nil {{ fmt.Fprintln(os.Stderr, "Candidate startup attestation refused"); os.Exit(2) }}
\t\treturn
\t}}
\tresult, candidateErr := startup.Preflight("{component}", "{digest}", os.Args[1:])
\tif candidateErr != nil {{ fmt.Fprintln(os.Stderr, "Candidate Hotel startup preflight refused"); os.Exit(2) }}
\tcandidatePodIdentity, candidateErr := startup.CurrentIdentity()
\tif candidateErr != nil {{ fmt.Fprintln(os.Stderr, "Candidate Hotel startup identity refused"); os.Exit(2) }}
'''
    text = replace_exact(text, anchor, preflight + anchor, label='guarded startup pre-network preflight')
    # The pinned frontend has no blank line before defer; the other nine do.
    block = ('\tlog.Info().Msg("Reading config...")\n'
             '\tjsonFile, err := os.Open("config.json")\n'
             '\tif err != nil {\n'
             '\t\tlog.Error().Msgf("Got error while reading config: %v", err)\n'
             '\t}\n' + ('' if component == 'frontend' else '\n') +
             '\tdefer jsonFile.Close()\n\n'
             '\tbyteValue, _ := ioutil.ReadAll(jsonFile)\n\n'
             '\tvar result map[string]string\n'
             '\tjson.Unmarshal([]byte(byteValue), &result)\n')
    text = replace_exact(text, block,
                         '\t// Exact baked configuration was validated before tuning or networking.\n',
                         label='guarded startup unchecked config source')
    key = {'recommendation': 'Recommend', 'reservation': 'Reserve'}.get(component, component.title())
    variable = 'serv_ip' if component in ('attractions', 'review') else 'servIP'
    return replace_exact(text, f'\t{variable} := result["{key}IP"]',
                         f'\t{variable} := candidatePodIdentity.PodIP',
                         label='guarded startup explicit Pod IP')


def patch_tls(text: str) -> str:
    """Guard copied package initialization before any TLS credential file I/O."""
    if '\t"fmt"\n' not in text:
        text = replace_exact(text, 'import (\n', 'import (\n\t"fmt"\n',
                             label='guarded startup TLS diagnostic import')
    anchor = 'func init() {\n\tneedTLS, cipher := checkTLS()\n'
    replacement = '''func init() {
\t// Metadata-only attestations need no environment, config, credentials or network.
\tif len(os.Args) == 2 && (os.Args[1] == "--candidate-startup-attestation" || os.Args[1] == "--candidate-initialization-attestation") {
\t\treturn
\t}
\tif os.Getenv("TLS") != "false" {
\t\tfmt.Fprintln(os.Stderr, "Candidate Hotel startup requires TLS=false")
\t\tos.Exit(2)
\t}
\tneedTLS, cipher := checkTLS()
'''
    return replace_exact(text, anchor, replacement, label='guarded startup TLS package initialization')
