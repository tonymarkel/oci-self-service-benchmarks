"""Deterministic commands and result parsing for curated Phoronix profiles."""

from dataclasses import dataclass
import json
import math
import re
import shlex
from typing import Any, Iterable


PTS_REPOSITORY = (
    'https://github.com/phoronix-test-suite/phoronix-test-suite.git'
)
PTS_REVISION = 'f977d6e270d5eb9eebfa26d3ca62385c00a547a6'
PTS_DIRECTORY = '/tmp/phoronix-test-suite'
PTS_EXECUTABLE = f'{PTS_DIRECTORY}/phoronix-test-suite'

MEASURED_TRIALS = 3
PREPARE_TIMEOUT_SECONDS = 1200
PROFILE_TIMEOUT_SECONDS = 1800
SSH_TIMEOUT_SECONDS = PROFILE_TIMEOUT_SECONDS + 300
EXPORT_START = '--- OCI_PHORONIX_JSON_START ---'
EXPORT_END = '--- OCI_PHORONIX_JSON_END ---'


@dataclass(frozen=True)
class DownloadAsset:
    filename: str
    url: str
    sha256: str


@dataclass(frozen=True)
class Profile:
    id: str
    result_id: str
    profile: str
    name: str
    preset_options: str | None = None
    assets: tuple[DownloadAsset, ...] = ()


@dataclass(frozen=True)
class ProfileRun:
    benchmark_id: str
    name: str
    profile: str
    command: str
    timeout_seconds: int
    metadata: dict[str, Any]


SEVEN_ZIP_ASSET = DownloadAsset(
    filename='7z2601-src.tar.xz',
    url=(
        'https://github.com/ip7z/7zip/releases/download/26.01/'
        '7z2601-src.tar.xz'
    ),
    sha256=(
        'b2389e0e930b2f9a348cf0fe7d9870a46482a8ec044ee0bdf42e2136db31c3d6'
    ),
)

PROFILES = {
    profile.id: profile
    for profile in (
        Profile(
            id='compress_7zip',
            result_id='phoronix_compress_7zip',
            profile='pts/compress-7zip-1.13.1',
            name='Phoronix — 7-Zip Compression',
            assets=(SEVEN_ZIP_ASSET,),
        ),
        Profile(
            id='openssl',
            result_id='phoronix_openssl',
            profile='pts/openssl-3.6.0',
            name='Phoronix — OpenSSL SHA-256',
            preset_options='openssl.algo=SHA256',
        ),
        Profile(
            id='build_linux_kernel',
            result_id='phoronix_build_linux_kernel',
            profile='pts/build-linux-kernel-1.18.0',
            name='Phoronix — Linux Kernel Compilation',
            preset_options='build-linux-kernel.build=defconfig',
        ),
        Profile(
            id='tinymembench',
            result_id='phoronix_tinymembench',
            profile='pts/tinymembench-1.0.2',
            name='Phoronix — Tinymembench',
        ),
    )
}

BASE_PACKAGES = frozenset({
    'curl',
    'gcc',
    'gcc-c++',
    'git',
    'make',
    'php-cli',
    'php-common',
    'php-process',
    'php-xml',
    'unzip',
    'xz',
})
KERNEL_BUILD_PACKAGES = frozenset({
    'bc',
    'bison',
    'cpio',
    'elfutils-libelf-devel',
    'flex',
    'gawk',
    'openssl-devel',
    'perl-core',
})
OPENSSL_BUILD_PACKAGES = frozenset({'perl-core'})


def profile(profile_id: str) -> Profile:
    try:
        return PROFILES[profile_id]
    except KeyError as exc:
        raise ValueError(f'Unsupported Phoronix profile: {profile_id}') from exc


def required_packages(profile_ids: Iterable[str]) -> set[str]:
    selected = tuple(dict.fromkeys(profile_ids))
    for profile_id in selected:
        profile(profile_id)
    packages = set(BASE_PACKAGES) if selected else set()
    if 'build_linux_kernel' in selected:
        packages.update(KERNEL_BUILD_PACKAGES)
    if 'openssl' in selected:
        packages.update(OPENSSL_BUILD_PACKAGES)
    return packages


def prepare_command() -> str:
    """Return an idempotent command that pins and configures the PTS client."""
    repository = shlex.quote(PTS_REPOSITORY)
    revision = shlex.quote(PTS_REVISION)
    directory = shlex.quote(PTS_DIRECTORY)
    executable = shlex.quote(PTS_EXECUTABLE)
    return (
        'set -euo pipefail; '
        f'PTS_DIR={directory}; PTS_REV={revision}; '
        'CURRENT_REV=""; '
        'if [ -d "$PTS_DIR/.git" ]; then '
        'CURRENT_REV=$(git -C "$PTS_DIR" rev-parse HEAD 2>/dev/null || true); '
        'fi; '
        'if [ "$CURRENT_REV" != "$PTS_REV" ]; then '
        'for attempt in $(seq 1 3); do '
        'rm -rf "$PTS_DIR"; git init -q "$PTS_DIR"; '
        f'git -C "$PTS_DIR" remote add origin {repository}; '
        'if getent ahostsv4 github.com >/dev/null 2>&1 '
        '&& timeout 120 git -C "$PTS_DIR" fetch --depth 1 origin "$PTS_REV" '
        '&& git -C "$PTS_DIR" checkout --detach "$PTS_REV"; then '
        'break; fi; '
        'echo "Phoronix Test Suite checkout failed; retrying in 10 seconds '
        '($attempt/3)." >&2; sleep 10; '
        'done; fi; '
        'test "$(git -C "$PTS_DIR" rev-parse HEAD)" = "$PTS_REV"; '
        'git -C "$PTS_DIR" reset --hard "$PTS_REV"; '
        'git -C "$PTS_DIR" clean -fdx; '
        f'{executable} user-config-set '
        'SaveResults=TRUE OpenBrowser=FALSE UploadResults=FALSE '
        'PromptForTestIdentifier=FALSE PromptForTestDescription=FALSE '
        'PromptSaveName=FALSE RunAllTestCombinations=FALSE Configured=TRUE '
        'AnonymousUsageReporting=FALSE '
        'AllowResultUploadsToOpenBenchmarking=FALSE '
        'AlwaysUploadResultsToOpenBenchmarking=FALSE; '
        f'{executable} version; '
        'REFRESHED=FALSE; '
        'for attempt in $(seq 1 3); do '
        f'if timeout 180 {executable} openbenchmarking-refresh; then '
        'REFRESHED=TRUE; break; fi; '
        'echo "OpenBenchmarking metadata refresh failed; retrying in 10 seconds '
        '($attempt/3)." >&2; sleep 10; '
        'done; test "$REFRESHED" = TRUE'
    )


def _safe_run_token(run_token: str) -> str:
    token = re.sub(r'[^A-Za-z0-9_-]+', '-', str(run_token)).strip('-_')
    return token[:32] or 'run'


def _pts_clean_save_name(value: str) -> str:
    """Mirror PTS ``clean_save_name`` for a newly saved result.

    PTS replaces spaces with dashes, removes every character other than ASCII
    letters, numbers, and dashes, collapses repeated dashes, lowercases the
    result, and limits it to 126 characters. In particular, underscores are
    removed rather than retained or replaced.
    """
    cleaned = str(value).strip().replace(' ', '-')
    cleaned = re.sub(r'[^A-Za-z0-9-]', '', cleaned)
    cleaned = re.sub(r'-+', '-', cleaned)
    return cleaned.lower()[:126]


def _asset_command(asset: DownloadAsset) -> str:
    filename = shlex.quote(asset.filename)
    url = shlex.quote(asset.url)
    checksum = shlex.quote(asset.sha256)
    return (
        f'CACHE="$HOME/.phoronix-test-suite/download-cache/{filename}"; '
        'mkdir -p "$(dirname "$CACHE")"; '
        f'if ! printf "%s  %s\\n" {checksum} "$CACHE" | sha256sum -c - '
        '>/dev/null 2>&1; then '
        'for attempt in $(seq 1 6); do '
        'rm -f "$CACHE.part"; '
        f'if curl -L --fail --retry 3 --retry-all-errors '
        f'--connect-timeout 15 --max-time 300 {url} -o "$CACHE.part" '
        f'&& printf "%s  %s\\n" {checksum} "$CACHE.part" '
        '| sha256sum -c -; then '
        'mv "$CACHE.part" "$CACHE"; break; fi; '
        'echo "Phoronix asset download failed; retrying in 10 seconds '
        '($attempt/6)." >&2; sleep 10; '
        'done; fi; '
        f'printf "%s  %s\\n" {checksum} "$CACHE" | sha256sum -c -; '
    )


def benchmark_command(profile_id: str, run_token: str) -> str:
    """Build a noninteractive, bounded command for one curated profile."""
    selected = profile(profile_id)
    result_name = _pts_clean_save_name(
        f'oci-phoronix-{_safe_run_token(run_token)}-{selected.id}'
    )
    result_name_q = shlex.quote(result_name)
    profile_q = shlex.quote(selected.profile)
    executable = shlex.quote(PTS_EXECUTABLE)
    asset_commands = ''.join(
        _asset_command(asset) for asset in selected.assets
    )
    preset = (
        f'export PRESET_OPTIONS={shlex.quote(selected.preset_options)}; '
        if selected.preset_options
        else ''
    )
    return (
        'set -euo pipefail; export LC_ALL=C; '
        f'cd {shlex.quote(PTS_DIRECTORY)}; '
        f'RESULT_NAME={result_name_q}; '
        'EXPORT_PATH="/tmp/${RESULT_NAME}.json"; '
        'RAW_PATH="/tmp/${RESULT_NAME}.txt"; '
        'rm -rf "$HOME/.phoronix-test-suite/test-results/$RESULT_NAME"; '
        'rm -f "$EXPORT_PATH" "$RAW_PATH"; '
        f'{asset_commands}'
        f'export FORCE_TIMES_TO_RUN={MEASURED_TRIALS}; '
        'export TEST_RESULTS_NAME="$RESULT_NAME"; '
        'export TEST_RESULTS_IDENTIFIER="OCI benchmark runner"; '
        'export TEST_RESULTS_DESCRIPTION="Curated OCI compute benchmark"; '
        'unset PRESET_OPTIONS PRESET_OPTIONS_VALUES; '
        f'{preset}'
        f'timeout 120 {executable} info {profile_q}; '
        f'timeout --signal=TERM --kill-after=30s {PROFILE_TIMEOUT_SECONDS} '
        f'{executable} batch-benchmark {profile_q} '
        '2>&1 | tee "$RAW_PATH"; '
        'test -s "$HOME/.phoronix-test-suite/test-results/'
        '$RESULT_NAME/composite.xml"; '
        f'OUTPUT_FILE="$EXPORT_PATH" {executable} '
        'result-file-to-json "$RESULT_NAME"; '
        'test -s "$EXPORT_PATH"; '
        f'printf "\\n{EXPORT_START}\\n"; '
        'cat "$EXPORT_PATH"; '
        f'printf "\\n{EXPORT_END}\\n"'
    )


def profile_runs(
    profile_ids: Iterable[str],
    run_token: str,
) -> tuple[ProfileRun, ...]:
    """Create one independently executable result specification per profile."""
    runs = []
    for profile_id in dict.fromkeys(profile_ids):
        selected = profile(profile_id)
        runs.append(ProfileRun(
            benchmark_id=selected.result_id,
            name=selected.name,
            profile=selected.profile,
            command=benchmark_command(selected.id, run_token),
            timeout_seconds=SSH_TIMEOUT_SECONDS,
            metadata={
                'phoronix_profile': selected.profile,
                'phoronix_client_revision': PTS_REVISION,
                'measured_trials': MEASURED_TRIALS,
                'profile_timeout_seconds': PROFILE_TIMEOUT_SECONDS,
                **(
                    {'fixed_options': selected.preset_options}
                    if selected.preset_options
                    else {}
                ),
            },
        ))
    return tuple(runs)


def extract_result_export(output: str) -> dict[str, Any]:
    start = output.rfind(EXPORT_START)
    if start < 0:
        raise ValueError('Phoronix did not emit its structured JSON result.')
    start += len(EXPORT_START)
    end = output.find(EXPORT_END, start)
    if end < 0:
        raise ValueError('Phoronix emitted an incomplete structured JSON result.')
    payload_text = output[start:end].strip()
    try:
        payload = json.loads(payload_text)
    except json.JSONDecodeError as exc:
        raise ValueError('Phoronix emitted invalid structured JSON.') from exc
    if not isinstance(payload, dict):
        raise ValueError('Phoronix JSON result must be an object.')
    if not isinstance(payload.get('results'), dict) or not payload['results']:
        raise ValueError('Phoronix JSON result contains no benchmark results.')
    return payload


def parse_result_output(
    output: str,
    expected_profile: str | None = None,
) -> dict[str, Any]:
    """Validate a PTS JSON export and return report-friendly measurements."""
    payload = extract_result_export(output)
    measurements = []
    for result in payload['results'].values():
        if not isinstance(result, dict):
            raise ValueError('Phoronix JSON contains an invalid result record.')
        result_profile = result.get('identifier')
        if expected_profile and result_profile != expected_profile:
            raise ValueError(
                'Phoronix returned an unexpected profile: '
                f'{result_profile or "missing identifier"}; expected '
                f'{expected_profile}.'
            )
        result_buffers = result.get('results')
        if not isinstance(result_buffers, dict) or not result_buffers:
            raise ValueError('Phoronix result contains no measurements.')
        scale = result.get('scale')
        if not scale:
            raise ValueError('Phoronix result is missing its measurement unit.')
        proportion = result.get('proportion')
        direction = {
            'HIB': 'Higher is better',
            'LIB': 'Lower is better',
        }.get(proportion, str(proportion or 'Not specified'))
        for system_identifier, measurement in result_buffers.items():
            if not isinstance(measurement, dict):
                raise ValueError('Phoronix JSON contains an invalid measurement.')
            value = measurement.get('value')
            if (
                not isinstance(value, (int, float))
                or isinstance(value, bool)
                or not math.isfinite(float(value))
            ):
                raise ValueError('Phoronix measurement does not contain a score.')
            raw_values = measurement.get('raw_values')
            if not isinstance(raw_values, list):
                raise ValueError('Phoronix measurement is missing trial values.')
            if len(raw_values) != MEASURED_TRIALS:
                raise ValueError(
                    f'Phoronix completed {len(raw_values)} of '
                    f'{MEASURED_TRIALS} required measured trials.'
                )
            if any(
                not isinstance(item, (int, float))
                or isinstance(item, bool)
                or not math.isfinite(float(item))
                for item in raw_values
            ):
                raise ValueError('Phoronix trial values must be numeric.')
            parsed = {
                'profile': result_profile,
                'benchmark': result.get('title'),
                'configuration': (
                    result.get('description') or result.get('arguments') or 'Default'
                ),
                'system': system_identifier,
                'score': value,
                'unit': scale,
                'direction': direction,
                'measured_trials': raw_values,
            }
            run_times = measurement.get('test_run_times')
            if isinstance(run_times, list):
                parsed['test_run_times_seconds'] = run_times
            measurements.append(parsed)
    if not measurements:
        raise ValueError('Phoronix JSON result contains no measurements.')

    metrics: dict[str, Any] = {'measurement_count': len(measurements)}
    if len(measurements) == 1:
        metrics.update(measurements[0])
    else:
        metrics['measurements'] = measurements
    return metrics
