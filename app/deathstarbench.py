import base64
import json
import re
from ipaddress import ip_address


REPOSITORY = 'https://github.com/delimitrou/DeathStarBench.git'
REVISION = '6ecb09706140f8730b5385c08f1386c654c3c526'
REVISION_TAG = REVISION[:12]
REMOTE_ROOT = '/tmp/deathstarbench'
LUAJIT_PREFIX = '/tmp/deathstarbench-luajit'
LUAROCKS_TREE = '/tmp/deathstarbench-luarocks'
# The pinned OpenResty image uses LuaJIT/Lua 5.1, which cannot compile the
# current oversized LuaRocks root manifest. Install this self-contained source
# rock directly and verify it instead of performing a name-based manifest lookup.
OPENRESTY_JWT_VERSION = '0.2.2-0'
OPENRESTY_JWT_FILENAME = f'lua-resty-jwt-{OPENRESTY_JWT_VERSION}.src.rock'
OPENRESTY_JWT_URL = (
    'https://luarocks.org/manifests/cdbattags/' + OPENRESTY_JWT_FILENAME
)
OPENRESTY_JWT_SHA256 = (
    '8a6bc8a12679953e345da55fe1653f2c581301140a90245e2af971c978e49ca8'
)

WORKLOADS = {
    'media_microservices': {
        'name': 'Media Microservices',
        'directory': 'mediaMicroservices',
        'port': 8080,
        'script': 'mediaMicroservices/wrk2/scripts/media-microservices/compose-review.lua',
        # The upstream Lua script appends /wrk2-api/review/compose to wrk2's
        # global URL. Supplying that path here would duplicate it and turn
        # every measured request into a 404.
        'endpoint': '',
        'project': 'oci_dsb_media',
        'network': 'oci_dsb_media_network',
        'frontend_service': 'nginx-web-server',
    },
    'hotel_reservation': {
        'name': 'Hotel Reservation',
        'directory': 'hotelReservation',
        'port': 5000,
        'script': 'hotelReservation/wrk2/scripts/hotel-reservation/mixed-workload_type_1.lua',
        'endpoint': '/',
        'project': 'oci_dsb_hotel',
        'network': 'oci_dsb_hotel_network',
        'frontend_service': 'frontend',
    },
    'social_network': {
        'name': 'Social Network',
        'directory': 'socialNetwork',
        'port': 8080,
        'script': 'socialNetwork/wrk2/scripts/social-network/mixed-workload.lua',
        'endpoint': '/',
        'project': 'oci_dsb_social',
        'network': 'oci_dsb_social_network',
        'frontend_service': 'nginx-thrift',
    },
}

# Pull registry inputs explicitly before building. Besides making the build
# order visible, this lets Podman's registry retries absorb transient failures
# from OCI's VCN resolver and keeps Compose startup independent of the registry.
WORKLOAD_REGISTRY_IMAGES = {
    'media_microservices': (
        'docker.io/library/ubuntu:16.04',
        'docker.io/library/ubuntu:xenial',
        'docker.io/jaegertracing/all-in-one:1.57.0',
        'docker.io/library/memcached:1.6.26',
        'docker.io/library/mongo:4.4.6',
        'docker.io/library/redis:7.2.4',
    ),
    'social_network': (
        'docker.io/library/ubuntu:16.04',
        'docker.io/library/ubuntu:xenial',
        'docker.io/jaegertracing/all-in-one:1.57.0',
        'docker.io/library/memcached:1.6.26',
        'docker.io/library/mongo:4.4.6',
        'docker.io/library/redis:7.2.4',
    ),
    'hotel_reservation': (
        'docker.io/library/golang:1.21',
        'docker.io/hashicorp/consul:1.19.0',
        'docker.io/jaegertracing/all-in-one:1.57.0',
        'docker.io/library/memcached:1.6.26',
        'docker.io/library/mongo:5.0',
    ),
}


def workload(workload_id):
    try:
        return WORKLOADS[workload_id]
    except KeyError:
        raise ValueError(f'Unsupported DeathStarBench workload: {workload_id}') from None


def _luarocks_runtime_environment():
    """Return shell assignments for the exact private LuaRocks tree layout.

    Oracle Linux's LuaRocks package deploys native modules below ``lib64``,
    while other distributions use ``lib``.  Ask the same LuaRocks
    installation that deployed LuaSocket for its runtime paths instead of
    duplicating either platform convention here.
    """
    options = (
        f'luarocks --lua-version=5.1 --lua-dir={LUAJIT_PREFIX} '
        f'--tree={LUAROCKS_TREE}'
    )
    return (
        f'LUA_PATH="$({options} path --lr-path);;" '
        f'LUA_CPATH="$({options} path --lr-cpath);;" '
    )


def _wrk_binary_verification_command(executable=f'{REMOTE_ROOT}/wrk2/wrk'):
    """Verify wrk2 without treating its usage exit status as a failure.

    The pinned wrk2 binary prints its usage banner and exits nonzero when run
    without a workload.  Validate both that banner and wrk2's required rate
    option explicitly so ``set -euo pipefail`` does not reject a healthy build
    or accidentally accept the original wrk binary.
    """
    return (
        f'if WRK_USAGE=$({executable} 2>&1); then '
        'echo "The wrk2 binary unexpectedly accepted an argument-free '
        'invocation." >&2; exit 1; fi; '
        'case "$WRK_USAGE" in '
        '*"Usage: wrk <options> <url>"*"-R, --rate"*) '
        'echo "wrk2 binary is ready.";; '
        '*) echo "The wrk2 binary did not return the expected wrk2 usage '
        'banner." '
        '>&2; exit 1;; esac'
    )


def pinned_clone_command(destination=REMOTE_ROOT, recursive=False):
    submodules = (
        f'git -C {destination} submodule update --init --recursive --depth 1'
        if recursive
        else 'true'
    )
    return (
        '{ for attempt in $(seq 1 6); do '
        f'rm -rf {destination}; '
        f'git init -q {destination}; '
        f'git -C {destination} remote add origin {REPOSITORY}; '
        f'if git -C {destination} fetch --depth 1 origin {REVISION} '
        f'&& git -C {destination} checkout -q --detach FETCH_HEAD '
        f'&& {submodules}; then break; fi; '
        'echo "DeathStarBench checkout failed; retrying in 10 seconds '
        '($attempt/6)." >&2; sleep 10; '
        'done; '
        f'test "$(git -C {destination} rev-parse HEAD 2>/dev/null)" = '
        f'"{REVISION}"; }}'
    )


def podman_runtime_verification_command():
    return (
        'set -euo pipefail; '
        'PODMAN_PATH=$(command -v podman); '
        'PODMAN_COMPOSE_PATH=$(command -v podman-compose); '
        'DOCKER_PATH=$(command -v docker 2>/dev/null || true); '
        'if test -n "$DOCKER_PATH"; then '
        'PODMAN_TARGET=$(readlink -f "$PODMAN_PATH"); '
        'DOCKER_TARGET=$(readlink -f "$DOCKER_PATH" 2>/dev/null || true); '
        'DOCKER_OWNER=$(rpm -qf --qf "%{NAME}\\n" "$DOCKER_PATH" '
        '2>/dev/null || true); '
        'if test "$DOCKER_TARGET" = "$PODMAN_TARGET"; then '
        'echo "Verified a docker-to-Podman compatibility symlink; it will not '
        'be invoked."; '
        'elif test "$DOCKER_OWNER" = "podman-docker" '
        '&& rpm -V podman-docker >/dev/null 2>&1; then '
        'echo "Verified the OL9 podman-docker compatibility wrapper; it will '
        'not be invoked."; '
        'else echo "A non-Podman Docker executable is installed at '
        '$DOCKER_PATH; refusing to run." >&2; exit 1; fi; fi; '
        'DOCKER_COMPOSE_PATH=$(command -v docker-compose 2>/dev/null || true); '
        'if test -n "$DOCKER_COMPOSE_PATH"; then '
        'PODMAN_COMPOSE_TARGET=$(readlink -f "$PODMAN_COMPOSE_PATH"); '
        'DOCKER_COMPOSE_TARGET=$(readlink -f "$DOCKER_COMPOSE_PATH" '
        '2>/dev/null || true); '
        'if test "$DOCKER_COMPOSE_TARGET" = "$PODMAN_COMPOSE_TARGET"; then '
        'echo "Verified a docker-compose-to-podman-compose compatibility '
        'symlink; it will not be invoked."; '
        'else echo "A non-Podman Docker Compose executable is installed at '
        '$DOCKER_COMPOSE_PATH; refusing to run." >&2; exit 1; fi; fi; '
        'if command -v dockerd >/dev/null 2>&1; then '
        'echo "Docker Engine (dockerd) is installed; refusing to run." >&2; '
        'exit 1; fi; '
        'for package in docker docker-ce docker-ce-cli docker-compose '
        'docker-compose-plugin docker-engine docker-ee moby-engine; do '
        'if rpm -q "$package" >/dev/null 2>&1; then '
        'echo "Prohibited Docker package is installed: $package" >&2; '
        'exit 1; fi; done; '
        'for unit in docker.service docker.socket; do '
        'if sudo systemctl is-active --quiet "$unit" 2>/dev/null; then '
        'echo "Docker Engine unit $unit is active; refusing to run." >&2; '
        'exit 1; fi; done; '
        'printf "architecture: "; uname -m; '
        '"$PODMAN_PATH" --version; /usr/bin/podman-compose --version; '
        'RUNTIME_INFO=$(sudo podman info --format '
        "'rootless={{.Host.Security.Rootless}} "
        "networkBackend={{.Host.NetworkBackend}} "
        "graphDriver={{.Store.GraphDriverName}}'); "
        'echo "$RUNTIME_INFO"; '
        'case "$RUNTIME_INFO" in *rootless=false*networkBackend=netavark*) ;; '
        '*) echo "DeathStarBench requires rootful Podman with Netavark." >&2; '
        'exit 1;; esac'
    )


def _compose_preparation_script():
    script = r'''from pathlib import Path
import sys
import yaml

root = Path('/tmp/deathstarbench')
workload = sys.argv[1]
tag = sys.argv[2]

OPENRESTY_ROCKS_ORIGINAL = (
    'RUN luarocks install long \\\n'
    '    && luarocks install lua-resty-jwt \\\n'
    '    && ldconfig'
)
OPENRESTY_ROCKS_PINNED = (
    'RUN curl -fSL --retry 5 --retry-delay 5 \\\n'
    '        __OPENRESTY_JWT_URL__ \\\n'
    '        -o /tmp/__OPENRESTY_JWT_FILENAME__ \\\n'
    '    && echo "__OPENRESTY_JWT_SHA256__  '
    '/tmp/__OPENRESTY_JWT_FILENAME__" | sha256sum -c - \\\n'
    '    && luarocks install --deps-mode=none '
    '/tmp/__OPENRESTY_JWT_FILENAME__ \\\n'
    '    && rm -f /tmp/__OPENRESTY_JWT_FILENAME__ \\\n'
    '    && ldconfig \\\n'
    "    && luajit -e 'assert(require \"liblualongnumber\")' \\\n"
    "    && /usr/local/openresty/bin/resty -e "
    "'assert(require \"resty.jwt\")'"
)

def patch_legacy_downloads(text):
    text = text.replace(
        'FROM ubuntu:16.04',
        'FROM docker.io/library/ubuntu:16.04',
    )
    text = text.replace(
        'FROM ubuntu:xenial',
        'FROM docker.io/library/ubuntu:xenial',
    )
    text = text.replace(
        'ARG RESTY_IMAGE_BASE="ubuntu"',
        'ARG RESTY_IMAGE_BASE="docker.io/library/ubuntu"',
    )
    text = text.replace(
        'FROM golang:1.21 as builder',
        'FROM docker.io/library/golang:1.21 AS builder',
    )
    text = text.replace(
        'http://ftp.cs.stanford.edu/pub/exim/pcre/'
        'pcre-${RESTY_PCRE_VERSION}.tar.gz',
        'https://sourceforge.net/projects/pcre/files/pcre/'
        '${RESTY_PCRE_VERSION}/pcre-${RESTY_PCRE_VERSION}.tar.gz/download',
    )
    return text

def write_containerfile(source, destination, replacements=None):
    text = source.read_text()
    for old, new in (replacements or {}).items():
        matches = text.count(old)
        if matches != 1:
            raise RuntimeError(
                f'Expected exactly one occurrence of {old.splitlines()[0]!r} '
                f'in {source}; found {matches}.'
            )
        text = text.replace(old, new)
    destination.write_text(patch_legacy_downloads(text))

def replace_exact_in_file(path, old, new):
    text = path.read_text()
    matches = text.count(old)
    if matches != 1:
        raise RuntimeError(
            f'Expected exactly one occurrence of {old.splitlines()[0]!r} '
            f'in {path}; found {matches}.'
        )
    path.write_text(text.replace(old, new))

if workload == 'media_microservices':
    directory = root / 'mediaMicroservices'
    network_name = 'oci_dsb_media_network'
    frontend_service = 'nginx-web-server'
    nginx_config = directory / 'nginx-web-server/conf/nginx.conf'
    lua_package_path = (
        "  lua_package_path "
        "'/usr/local/openresty/nginx/lua-scripts/?.lua;;';"
    )
    replace_exact_in_file(
        nginx_config,
        lua_package_path,
        lua_package_path + '\n\n  client_body_buffer_size 64k;',
    )
    replace_exact_in_file(
        directory / 'nginx-web-server/lua-scripts/wrk2-api/'
        'movie-info/write.lua',
        'new_cast["charactor"]=cast["charactor"]',
        'new_cast["character"]=cast["character"]',
    )
    write_containerfile(
        directory / 'docker/thrift-microservice-deps/cpp/Dockerfile',
        directory / 'docker/thrift-microservice-deps/cpp/Containerfile.oci',
    )
    write_containerfile(
        directory / 'Dockerfile',
        directory / 'Containerfile.oci',
        {'FROM yg397/thrift-microservice-deps:xenial':
         f'FROM localhost/deathstarbench-media-deps:{tag}'},
    )
    write_containerfile(
        directory / 'docker/openresty-thrift/xenial/Dockerfile',
        directory / 'docker/openresty-thrift/xenial/Containerfile.oci',
        {OPENRESTY_ROCKS_ORIGINAL: OPENRESTY_ROCKS_PINNED},
    )
    image_map = {
        'yg397/media-microservices': f'localhost/deathstarbench-media:{tag}',
        'yg397/openresty-thrift:xenial': f'localhost/deathstarbench-openresty:{tag}',
    }
elif workload == 'social_network':
    directory = root / 'socialNetwork'
    network_name = 'oci_dsb_social_network'
    frontend_service = 'nginx-thrift'
    write_containerfile(
        directory / 'docker/thrift-microservice-deps/cpp/Dockerfile',
        directory / 'docker/thrift-microservice-deps/cpp/Containerfile.oci',
    )
    write_containerfile(
        directory / 'Dockerfile',
        directory / 'Containerfile.oci',
        {'FROM yg397/thrift-microservice-deps:xenial AS builder':
         f'FROM localhost/deathstarbench-social-deps:{tag} AS builder'},
    )
    write_containerfile(
        directory / 'docker/openresty-thrift/xenial/Dockerfile',
        directory / 'docker/openresty-thrift/xenial/Containerfile.oci',
        {OPENRESTY_ROCKS_ORIGINAL: OPENRESTY_ROCKS_PINNED},
    )
    write_containerfile(
        directory / 'docker/media-frontend/xenial/Dockerfile',
        directory / 'docker/media-frontend/xenial/Containerfile.oci',
    )
    image_map = {
        'deathstarbench/social-network-microservices:latest':
            f'localhost/deathstarbench-social:{tag}',
        'yg397/openresty-thrift:xenial':
            f'localhost/deathstarbench-openresty:{tag}',
        'yg397/media-frontend:xenial':
            f'localhost/deathstarbench-media-frontend:{tag}',
    }
elif workload == 'hotel_reservation':
    directory = root / 'hotelReservation'
    network_name = 'oci_dsb_hotel_network'
    frontend_service = 'frontend'
    write_containerfile(directory / 'Dockerfile', directory / 'Containerfile.oci')
    image_map = {
        'deathstarbench/hotel-reservation:latest':
            f'localhost/deathstarbench-hotel:{tag}',
        'hotel_reserv_review_single_node':
            f'localhost/deathstarbench-hotel:{tag}',
        'hotel_reserv_attractions_single_node':
            f'localhost/deathstarbench-hotel:{tag}',
    }
else:
    raise SystemExit(f'Unsupported workload: {workload}')

image_map.update({
    'redis': 'redis:7.2.4',
    'redis:latest': 'redis:7.2.4',
    'memcached': 'memcached:1.6.26',
    'memcached:latest': 'memcached:1.6.26',
    'jaegertracing/all-in-one:latest': 'jaegertracing/all-in-one:1.57.0',
    'hashicorp/consul:latest': 'hashicorp/consul:1.19.0',
})

source = directory / 'docker-compose.yml'
document = yaml.safe_load(source.read_text())
document.pop('version', None)
if workload == 'media_microservices':
    document['services'].pop('dns-media', None)

configs = document.pop('configs', {}) or {}
for service_name, service in document['services'].items():
    service.pop('build', None)
    service.pop('deploy', None)
    image = service.get('image')
    if image:
        image = image_map.get(image, image)
        if not image.startswith('localhost/') and not image.startswith('docker.io/'):
            if '/' not in image.split(':', 1)[0]:
                image = 'docker.io/library/' + image
            else:
                image = 'docker.io/' + image
        service['image'] = image
        # Every registry image is prefetched with explicit retries before this
        # file is used, and every custom image is built locally.
        service['pull_policy'] = 'never'

    volumes = list(service.get('volumes') or [])
    for config in service.pop('configs', []) or []:
        if isinstance(config, str):
            source_name = config
            target = '/' + config
        else:
            source_name = config['source']
            target = config.get('target', '/' + source_name)
        source_file = (configs.get(source_name) or {}).get('file')
        if source_file:
            volumes.append(f'{source_file}:{target}:ro,z')

    labeled = []
    for volume in volumes:
        if not isinstance(volume, str):
            labeled.append(volume)
            continue
        parts = volume.split(':')
        if len(parts) >= 2 and parts[0].startswith('.'):
            if len(parts) == 2:
                volume += ':z'
            elif not {'z', 'Z'} & set(parts[-1].split(',')):
                parts[-1] += ',z'
                volume = ':'.join(parts)
        labeled.append(volume)
    if labeled:
        service['volumes'] = labeled

    # Only the workload frontend is published on the service VM. Databases,
    # caches, and tracing endpoints stay inside the Podman bridge network.
    if service_name != frontend_service:
        service.pop('ports', None)

document['networks'] = {
    'default': {
        'external': True,
        'name': network_name,
    }
}

(directory / 'compose-oci.yml').write_text(
    yaml.safe_dump(document, sort_keys=False)
)
'''
    return (
        script
        .replace('__OPENRESTY_JWT_FILENAME__', OPENRESTY_JWT_FILENAME)
        .replace('__OPENRESTY_JWT_URL__', OPENRESTY_JWT_URL)
        .replace('__OPENRESTY_JWT_SHA256__', OPENRESTY_JWT_SHA256)
    )


def prepare_workload_command(workload_id):
    workload(workload_id)
    script = base64.b64encode(_compose_preparation_script().encode()).decode()
    return (
        'set -euo pipefail; '
        f'{pinned_clone_command()}; '
        f'printf %s {script} | base64 -d > /tmp/prepare-deathstarbench.py; '
        f'python3 /tmp/prepare-deathstarbench.py {workload_id} {REVISION_TAG}; '
        'rm -f /tmp/prepare-deathstarbench.py; '
        f'test -s {REMOTE_ROOT}/{workload(workload_id)["directory"]}/compose-oci.yml'
    )


def prefetch_workload_images_command(workload_id):
    workload(workload_id)
    images = WORKLOAD_REGISTRY_IMAGES[workload_id]
    return (
        f'for IMAGE in {" ".join(images)}; do '
        'echo "Prefetching $IMAGE with registry retries."; '
        'if ! sudo podman pull --retry=10 --retry-delay=10s "$IMAGE"; then '
        'echo "Registry pull failed after retries: $IMAGE" >&2; '
        'date -Ins >&2; '
        'echo "--- /etc/resolv.conf ---" >&2; cat /etc/resolv.conf >&2; '
        'echo "--- NSS hosts configuration ---" >&2; '
        'grep "^hosts:" /etc/nsswitch.conf >&2 || true; '
        'echo "--- NetworkManager DNS ---" >&2; '
        'if command -v nmcli >/dev/null 2>&1; then '
        'nmcli -f GENERAL.DEVICE,IP4.DNS,IP4.DOMAIN device show >&2 || true; fi; '
        'echo "--- route to OCI resolver ---" >&2; '
        'ip route get 169.254.169.254 >&2 || true; '
        'echo "--- OCI resolver lookups ---" >&2; '
        'for HOST in auth.docker.io registry-1.docker.io; do '
        'echo "[$HOST]" >&2; getent ahostsv4 "$HOST" >&2 || true; done; '
        'exit 1; fi; '
        'sudo podman image exists "$IMAGE" || '
        '{ echo "Prefetched image is not in local storage: $IMAGE" >&2; '
        'exit 1; }; done'
    )


def _retry_podman_build_command(command, image, retry_delay_seconds=20):
    retry_delay_seconds = max(0, int(retry_delay_seconds))
    return (
        'BUILD_COMPLETE=false; '
        'for attempt in 1 2 3; do '
        f'if {command}; then BUILD_COMPLETE=true; break; fi; '
        f'echo "Build of {image} failed; retrying cached steps '
        '($attempt/3)." >&2; '
        f'if [ "$attempt" -lt 3 ]; then sleep {retry_delay_seconds}; fi; done; '
        'if [ "$BUILD_COMPLETE" != true ]; then '
        f'echo "Build of {image} failed after three attempts." >&2; '
        'exit 1; fi'
    )


def build_workload_command(workload_id):
    workload(workload_id)
    root = REMOTE_ROOT
    tag = REVISION_TAG
    if workload_id == 'media_microservices':
        commands = [
            f'sudo podman build --pull=never --format oci '
            f'-t localhost/deathstarbench-media-deps:{tag} '
            f'-f {root}/mediaMicroservices/docker/thrift-microservice-deps/cpp/Containerfile.oci '
            f'{root}/mediaMicroservices/docker/thrift-microservice-deps/cpp',
            f'sudo podman build --pull=never --format oci '
            f'-t localhost/deathstarbench-media:{tag} '
            f'-f {root}/mediaMicroservices/Containerfile.oci '
            f'{root}/mediaMicroservices',
            f'sudo podman build --pull=never --format oci '
            f'-t localhost/deathstarbench-openresty:{tag} '
            f'-f {root}/mediaMicroservices/docker/openresty-thrift/xenial/Containerfile.oci '
            f'{root}/mediaMicroservices/docker/openresty-thrift',
        ]
    elif workload_id == 'social_network':
        commands = [
            f'sudo podman build --pull=never --format oci '
            f'-t localhost/deathstarbench-social-deps:{tag} '
            f'-f {root}/socialNetwork/docker/thrift-microservice-deps/cpp/Containerfile.oci '
            f'{root}/socialNetwork/docker/thrift-microservice-deps/cpp',
            f'sudo podman build --pull=never --format oci '
            f'-t localhost/deathstarbench-social:{tag} '
            f'-f {root}/socialNetwork/Containerfile.oci {root}/socialNetwork',
            f'sudo podman build --pull=never --format oci '
            f'-t localhost/deathstarbench-openresty:{tag} '
            f'-f {root}/socialNetwork/docker/openresty-thrift/xenial/Containerfile.oci '
            f'{root}/socialNetwork/docker/openresty-thrift',
            f'sudo podman build --pull=never --format oci '
            f'-t localhost/deathstarbench-media-frontend:{tag} '
            f'-f {root}/socialNetwork/docker/media-frontend/xenial/Containerfile.oci '
            f'{root}/socialNetwork/docker/media-frontend',
        ]
    else:
        commands = [
            f'sudo podman build --pull=never --format oci '
            f'-t localhost/deathstarbench-hotel:{tag} '
            f'-f {root}/hotelReservation/Containerfile.oci '
            f'{root}/hotelReservation'
        ]
    images = {
        'media_microservices': [
            f'localhost/deathstarbench-media-deps:{tag}',
            f'localhost/deathstarbench-media:{tag}',
            f'localhost/deathstarbench-openresty:{tag}',
        ],
        'social_network': [
            f'localhost/deathstarbench-social-deps:{tag}',
            f'localhost/deathstarbench-social:{tag}',
            f'localhost/deathstarbench-openresty:{tag}',
            f'localhost/deathstarbench-media-frontend:{tag}',
        ],
        'hotel_reservation': [f'localhost/deathstarbench-hotel:{tag}'],
    }[workload_id]
    commands = [
        _retry_podman_build_command(command, image)
        for command, image in zip(commands, images)
    ]
    validation = (
        'ARCH=$(uname -m); case "$ARCH" in '
        'x86_64) EXPECTED_ARCH=amd64;; aarch64) EXPECTED_ARCH=arm64;; '
        '*) echo "Unsupported service architecture: $ARCH" >&2; exit 1;; esac; '
        f'for IMAGE in {" ".join(images)}; do '
        'ACTUAL_ARCH=$(sudo podman image inspect --format '
        "'{{.Architecture}}' \"$IMAGE\"); "
        'echo "$IMAGE architecture: $ACTUAL_ARCH"; '
        'test "$ACTUAL_ARCH" = "$EXPECTED_ARCH" || '
        '{ echo "Native image architecture validation failed." >&2; exit 1; }; '
        'done'
    )
    return 'set -euo pipefail; ' + '; '.join([*commands, validation])


def deploy_workload_command(workload_id):
    settings = workload(workload_id)
    directory = f'{REMOTE_ROOT}/{settings["directory"]}'
    compose = (
        f'sudo /usr/bin/podman-compose -p {settings["project"]} '
        f'-f {directory}/compose-oci.yml'
    )
    network = settings['network']
    dns_files = {
        'media_microservices': [
            f'{directory}/nginx-web-server/conf/nginx.conf',
        ],
        'social_network': [
            f'{directory}/nginx-web-server/conf/nginx.conf',
            f'{directory}/media-frontend/conf/nginx.conf',
        ],
        'hotel_reservation': [],
    }[workload_id]
    dns_patch = 'true'
    nginx_tuning = 'true'
    if dns_files:
        dns_patch = (
            'DNS_GATEWAY=$(sudo podman network inspect --format '
            f"'{{{{range .Subnets}}}}{{{{.Gateway}}}}{{{{end}}}}' {network}); "
            'test -n "$DNS_GATEWAY"; '
            f'for CONFIG in {" ".join(dns_files)}; do '
            'sudo sed -Ei "s/resolver[[:space:]]+'
            '127\\.0\\.0\\.11[^;]*;/resolver ${DNS_GATEWAY} '
            'valid=10s ipv6=off;/" "$CONFIG"; '
            'grep -F "resolver ${DNS_GATEWAY}" "$CONFIG"; done'
        )
        nginx_tuning = (
            f'for CONFIG in {" ".join(dns_files)}; do '
            "sudo sed -Ei 's/worker_connections[[:space:]]+[0-9]+;/"
            "worker_connections 16384;/' \"$CONFIG\"; "
            "if ! grep -q 'worker_rlimit_nofile' \"$CONFIG\"; then "
            "sudo sed -Ei '/^[[:space:]]*worker_processes[[:space:]]/a "
            "worker_rlimit_nofile 65536;' \"$CONFIG\"; fi; "
            "grep -E 'worker_(processes|connections|rlimit_nofile)' "
            '"$CONFIG"; done'
        )
        if workload_id == 'media_microservices':
            nginx_tuning += (
                f'; sudo sed -Ei '
                "'s/^[[:space:]]*worker_processes[[:space:]]+[^;]+;/"
                f"worker_processes auto;/' {dns_files[0]}; "
                f"grep -F 'worker_processes auto;' {dns_files[0]}"
            )
    dns_check = (
        'test "$(sudo podman network inspect --format '
        f"'{{{{.DNSEnabled}}}}' {network})" +
        '" = "true"'
    )
    return (
        'set -euo pipefail; '
        f'cd {directory}; '
        f'{compose} down --remove-orphans >/dev/null 2>&1 || true; '
        f'sudo podman network rm -f {network} >/dev/null 2>&1 || true; '
        f'sudo podman network create {network}; '
        f'{dns_check}; '
        f'{dns_patch}; '
        f'{nginx_tuning}; '
        'COMPOSE_STARTED=false; '
        'for attempt in 1 2 3; do '
        f'if {compose} up -d; then COMPOSE_STARTED=true; break; fi; '
        'echo "Podman Compose startup failed; retrying in 20 seconds '
        '($attempt/3)." >&2; '
        'if [ "$attempt" -lt 3 ]; then sleep 20; fi; done; '
        'if [ "$COMPOSE_STARTED" != true ]; then '
        'echo "Podman Compose failed after three attempts." >&2; exit 1; fi; '
        f'{compose} ps; '
        'sudo podman ps --format "table {{.Names}}\\t{{.Status}}\\t{{.Ports}}"; '
        'sudo podman images --digests'
    )


def load_generator_prepare_command():
    lua_environment = _luarocks_runtime_environment()
    return (
        'set -euo pipefail; '
        f'{pinned_clone_command(recursive=True)}; '
        f'make -C {REMOTE_ROOT}/wrk2 -j$(nproc); '
        f'rm -rf {LUAJIT_PREFIX} {LUAROCKS_TREE}; '
        f'make -C {REMOTE_ROOT}/wrk2/deps/luajit install '
        f'PREFIX={LUAJIT_PREFIX}; '
        f'LUAJIT_BIN_COUNT=$(find {LUAJIT_PREFIX}/bin -maxdepth 1 -type f '
        "-name 'luajit-*' -perm -111 | wc -l); "
        'test "$LUAJIT_BIN_COUNT" -eq 1; '
        f'LUAJIT_BIN=$(find {LUAJIT_PREFIX}/bin -maxdepth 1 -type f '
        "-name 'luajit-*' -perm -111 -print -quit); "
        f'ln -sfn "$(basename "$LUAJIT_BIN")" '
        f'{LUAJIT_PREFIX}/bin/luajit; '
        f'test -x {LUAJIT_PREFIX}/bin/luajit; '
        f'luarocks --lua-version=5.1 --lua-dir={LUAJIT_PREFIX} '
        f'--tree={LUAROCKS_TREE} install luasocket 3.1.0-1; '
        f'{lua_environment}'
        f'{LUAJIT_PREFIX}/bin/luajit -e '
        "'local socket = require(\"socket\"); print(socket._VERSION)'; "
        f'test -x {REMOTE_ROOT}/wrk2/wrk; '
        'test -x /usr/bin/time; '
        f'{_wrk_binary_verification_command()}'
    )


def frontend_readiness_command(workload_id, target_private_ip):
    settings = workload(workload_id)
    target = str(ip_address(target_private_ip))
    port = settings['port']
    request = {
        'media_microservices': (
            '--data "first_name=OCI&last_name=Readiness&'
            'username=oci_readiness&password=oci_readiness" '
            f'http://$TARGET:{port}/wrk2-api/user/register'
        ),
        'social_network': (
            '--data "first_name=OCI&last_name=Readiness&'
            'username=oci_readiness&password=oci_readiness&user_id=999999" '
            f'http://$TARGET:{port}/wrk2-api/user/register'
        ),
        'hotel_reservation': (
            f'"http://$TARGET:{port}/hotels?inDate=2015-04-09&'
            'outDate=2015-04-10&lat=38.0&lon=-122.0"'
        ),
    }[workload_id]
    return (
        'set -euo pipefail; '
        f'TARGET={target}; PORT={port}; '
        'for attempt in $(seq 1 120); do '
        'STATUS=$(curl -sS --max-time 5 -o /tmp/deathstarbench-ready.out '
        f'-w "%{{http_code}}" {request} 2>/dev/null || true); '
        'case "$STATUS" in 2*|3*) '
        'echo "DeathStarBench frontend is ready (HTTP $STATUS)."; exit 0;; esac; '
        'echo "Waiting for DeathStarBench frontend ($attempt/120)."; sleep 5; '
        'done; echo "DeathStarBench frontend did not become ready." >&2; exit 1'
    )


def frontend_firewall_command(workload_id):
    port = workload(workload_id)['port']
    return (
        'set -euo pipefail; '
        'if command -v firewall-cmd >/dev/null 2>&1 '
        '&& sudo systemctl is-active --quiet firewalld; then '
        'INTERFACE=$(ip -o route get 10.42.1.1 | '
        "awk '{for (i=1; i<=NF; i++) if ($i == \"dev\") "
        "{print $(i+1); exit}}'); "
        'test -n "$INTERFACE"; '
        'ZONE=$(sudo firewall-cmd --get-zone-of-interface="$INTERFACE" '
        '2>/dev/null || true); '
        'if [ -z "$ZONE" ] || [ "$ZONE" = "no zone" ]; then '
        'ZONE=$(sudo firewall-cmd --get-default-zone); fi; '
        "RULE='rule family=ipv4 source address=10.42.1.0/24 "
        f"port protocol=tcp port={port} accept'; "
        'sudo firewall-cmd --permanent --zone="$ZONE" '
        '--add-rich-rule="$RULE"; '
        'sudo firewall-cmd --reload; '
        'sudo firewall-cmd --zone="$ZONE" --query-rich-rule="$RULE"; '
        'echo "Opened TCP port '
        f'{port} for the private subnet in firewalld zone $ZONE."; '
        'else echo "firewalld is not active; no guest rule was required."; fi'
    )


def _strict_aiohttp_patch_command(path, expected_replacements):
    script = r'''from pathlib import Path
import sys

path = Path(sys.argv[1])
expected = int(sys.argv[2])
text = path.read_text()
needle = '    return await resp.text()'
replacement = (
    '    body = await resp.text()\n'
    '    if resp.status >= 400:\n'
    '      raise RuntimeError(\n'
    '        f"HTTP {resp.status} from {resp.url}: {body[:500]}"\n'
    '      )\n'
    '    return body'
)
count = text.count(needle)
already_patched = text.count(replacement)
if already_patched != expected:
    if already_patched or count != expected:
        raise SystemExit(
            f'Expected {expected} aiohttp response handlers in {path}, '
            f'found {count} ({already_patched} already strict).'
        )
    path.write_text(text.replace(needle, replacement))
'''
    encoded = base64.b64encode(script.encode()).decode()
    return (
        f'printf %s {encoded} | base64 -d | python3 - '
        f'{path} {int(expected_replacements)}'
    )


def _strict_text_patch_command(path, old, new):
    script = r'''from pathlib import Path
import base64
import sys

path = Path(sys.argv[1])
old = base64.b64decode(sys.argv[2]).decode()
new = base64.b64decode(sys.argv[3]).decode()
text = path.read_text()
old_count = text.count(old)
new_count = text.count(new)
if new_count == 1 and old_count == 0:
    raise SystemExit(0)
if old_count != 1 or new_count:
    raise SystemExit(
        f'Expected one unpatched occurrence in {path}; '
        f'found {old_count} old and {new_count} patched.'
    )
path.write_text(text.replace(old, new))
'''
    encoded_script = base64.b64encode(script.encode()).decode()
    encoded_old = base64.b64encode(old.encode()).decode()
    encoded_new = base64.b64encode(new.encode()).decode()
    return (
        f'printf %s {encoded_script} | base64 -d | python3 - '
        f'{path} {encoded_old} {encoded_new}'
    )


def _media_title_dedup_patch_commands(path):
    initialize_titles = _strict_text_patch_command(
        path,
        'async def write_movie_info(addr, raw_movies):\n'
        '  idx = 0\n'
        '  tasks = []\n'
        '  conn =',
        'async def write_movie_info(addr, raw_movies):\n'
        '  idx = 0\n'
        '  tasks = []\n'
        '  registered_titles = set()\n'
        '  conn =',
    )
    deduplicate_registrations = _strict_text_patch_command(
        path,
        '      task = asyncio.ensure_future('
        'register_movie(session, addr, movie))\n'
        '      tasks.append(task)',
        '      if movie["title"] not in registered_titles:\n'
        '        registered_titles.add(movie["title"])\n'
        '        task = asyncio.ensure_future('
        'register_movie(session, addr, movie))\n'
        '        tasks.append(task)\n'
        '      else:\n'
        '        print("Skipping duplicate movie title registration:", '
        'movie["title"], "(first mapping retained)")',
    )
    return initialize_titles, deduplicate_registrations


def initialize_workload_command(workload_id, target_private_ip):
    target = str(ip_address(target_private_ip))
    if workload_id == 'media_microservices':
        directory = f'{REMOTE_ROOT}/mediaMicroservices'
        aiohttp_patch = _strict_aiohttp_patch_command(
            f'{directory}/scripts/write_movie_info.py',
            4,
        )
        thumbnail_patch = _strict_text_patch_command(
            f'{directory}/scripts/write_movie_info.py',
            'movie["thumbnail_ids"] = [raw_movie["poster_path"]]',
            'movie["thumbnail_ids"] = ([raw_movie["poster_path"]] '
            'if raw_movie["poster_path"] else [])',
        )
        title_dedup_patches = _media_title_dedup_patch_commands(
            f'{directory}/scripts/write_movie_info.py'
        )
        return (
            'set -euo pipefail; '
            f'cd {directory}; '
            f'{thumbnail_patch}; '
            f'{title_dedup_patches[0]}; '
            f'{title_dedup_patches[1]}; '
            f'{aiohttp_patch}; '
            f'sed -i "s|127.0.0.1|{target}|g" '
            'scripts/register_users.sh scripts/register_movies.sh; '
            "sed -i '2i set -euo pipefail' "
            'scripts/register_users.sh scripts/register_movies.sh; '
            "sed -i 's#curl -d#curl --fail-with-body --silent "
            "--show-error --output /dev/null -d#' "
            'scripts/register_users.sh scripts/register_movies.sh; '
            "grep -q -- '--fail-with-body' scripts/register_users.sh; "
            "grep -q -- '--fail-with-body' scripts/register_movies.sh; "
            'python3 scripts/write_movie_info.py '
            '-c datasets/tmdb/casts.json -m datasets/tmdb/movies.json '
            f'--server_address http://{target}:8080; '
            'bash scripts/register_users.sh; bash scripts/register_movies.sh'
        )
    if workload_id == 'social_network':
        aiohttp_patch = _strict_aiohttp_patch_command(
            f'{REMOTE_ROOT}/socialNetwork/scripts/init_social_graph.py',
            3,
        )
        return (
            'set -euo pipefail; '
            f'cd {REMOTE_ROOT}/socialNetwork; '
            f'{aiohttp_patch}; '
            'python3 scripts/init_social_graph.py --graph=socfb-Reed98 '
            f'--ip={target} --port=8080 --compose '
            '| tee /tmp/deathstarbench-social-init.out; '
            "if grep -q '^Failed:' /tmp/deathstarbench-social-init.out; then "
            'echo "Social Network initialization reported failed requests." '
            '>&2; exit 1; fi; '
            "test \"$(grep -c '^Succeeded:' "
            '/tmp/deathstarbench-social-init.out)" -ge 3'
        )
    if workload_id == 'hotel_reservation':
        return 'echo "Hotel Reservation uses its bundled initialization data."'
    workload(workload_id)


def load_command(workload_id, target_private_ip, options, duration_seconds):
    settings = workload(workload_id)
    target = str(ip_address(target_private_ip))
    port = settings['port']
    script = f'{REMOTE_ROOT}/{settings["script"]}'
    endpoint = settings['endpoint']
    threads = int(options.threads)
    connections = int(options.connections)
    request_rate = int(options.request_rate)
    duration = int(duration_seconds)
    script_patches = []
    if workload_id in {'hotel_reservation', 'social_network'}:
        script_patches.append(
            f'sed -i "s|http://localhost:{port}|http://{target}:{port}|g" '
            '/tmp/deathstarbench-workload.lua'
        )
    if workload_id == 'hotel_reservation':
        script_patches.append(
            "sed -i '/local function reserve()/a\\  local lat = 38.0\\n"
            "  local lon = -122.0' /tmp/deathstarbench-workload.lua"
        )
    metrics_lua = r'''
done = function(summary, latency, requests)
  local http_errors = summary.errors.status
  local socket_errors = summary.errors.connect + summary.errors.read +
    summary.errors.write + summary.errors.timeout
  local seconds = summary.duration / 1000000.0
  local throughput = 0.0
  local error_rate = 0.0
  if seconds > 0 then throughput = summary.requests / seconds end
  if summary.requests > 0 then
    error_rate = http_errors * 100.0 / summary.requests
  end
  io.write(string.format(
    "OCI_DSB_METRICS duration_seconds=%.6f total_requests=%d " ..
    "throughput_requests_per_second=%.6f errors=%d error_rate_percent=%.6f " ..
    "socket_errors=%d connect_errors=%d read_errors=%d write_errors=%d " ..
    "timeout_errors=%d " ..
    "p50_ms=%.6f p95_ms=%.6f p99_ms=%.6f\n",
    seconds, summary.requests, throughput, http_errors, error_rate,
    socket_errors, summary.errors.connect, summary.errors.read,
    summary.errors.write, summary.errors.timeout,
    latency:percentile(50.0) / 1000.0,
    latency:percentile(95.0) / 1000.0,
    latency:percentile(99.0) / 1000.0))
end
'''
    metrics_script = base64.b64encode(metrics_lua.encode()).decode()
    patches = '; '.join(script_patches) if script_patches else 'true'
    required_nofile = connections + 128
    lua_environment = _luarocks_runtime_environment()
    return (
        'set -euo pipefail; '
        f'REQUIRED_NOFILE={required_nofile}; '
        'HARD_NOFILE=$(ulimit -Hn); '
        'case "$HARD_NOFILE" in '
        'unlimited) ;; '
        "''|*[!0-9]*) echo \"Unable to read the open-file hard limit.\" "
        '>&2; exit 1;; '
        '*) if [ "$HARD_NOFILE" -lt "$REQUIRED_NOFILE" ]; then '
        'echo "The requested connection count needs an open-file limit of '
        '$REQUIRED_NOFILE, but this VM permits only $HARD_NOFILE." >&2; '
        'exit 1; fi;; esac; '
        'ulimit -Sn "$REQUIRED_NOFILE"; '
        'echo "wrk2 open-file soft limit: $(ulimit -Sn)"; '
        'echo "OCI_DSB_LOADGEN_LOGICAL_CPUS=$(nproc)"; '
        f'cp {script} /tmp/deathstarbench-workload.lua; '
        f'{patches}; '
        f'printf %s {metrics_script} | base64 -d '
        '>> /tmp/deathstarbench-workload.lua; '
        f'{lua_environment}'
        f'/usr/bin/time -v {REMOTE_ROOT}/wrk2/wrk '
        f'-D exp -r -t {threads} -c {connections} '
        f'-d {duration}s -L -s /tmp/deathstarbench-workload.lua '
        f'-R {request_rate} http://{target}:{port}{endpoint}'
    )


def _latency_ms(value, unit):
    value = float(value)
    return {
        'us': value / 1000,
        'ms': value,
        's': value * 1000,
    }[unit]


def parse_wrk2_output(output, require_marker=True):
    marker = re.search(r'^OCI_DSB_METRICS\s+(.+)$', output, re.MULTILINE)
    if marker:
        values = dict(re.findall(r'([a-z0-9_]+)=([0-9.]+)', marker.group(1)))
        required = {
            'duration_seconds',
            'total_requests',
            'throughput_requests_per_second',
            'errors',
            'error_rate_percent',
            'socket_errors',
            'connect_errors',
            'read_errors',
            'write_errors',
            'timeout_errors',
            'p50_ms',
            'p95_ms',
            'p99_ms',
        }
        if not required.issubset(values):
            raise ValueError('Machine-readable wrk2 metrics were incomplete.')
        parsed = {
            key: int(value)
            if key in {
                'total_requests',
                'errors',
                'socket_errors',
                'connect_errors',
                'read_errors',
                'write_errors',
                'timeout_errors',
            }
            else round(float(value), 6)
            for key, value in values.items()
        }
        if parsed['total_requests'] <= 0:
            raise ValueError('wrk2 completed without any requests.')
        if parsed['errors'] >= parsed['total_requests']:
            raise ValueError('Every completed wrk2 request failed.')
        sent_matches = re.findall(
            r'^Sent\s+([0-9]+)\s+requests\s*$',
            output,
            re.MULTILINE,
        )
        if not sent_matches:
            raise ValueError('wrk2 output did not report its sent request count.')
        sent_requests = int(sent_matches[-1])
        if sent_requests < parsed['total_requests']:
            raise ValueError(
                'wrk2 reported fewer sent requests than completed requests.'
            )
        parsed['sent_requests'] = sent_requests
        parsed['uncompleted_requests'] = (
            sent_requests - parsed['total_requests']
        )
        parsed['completion_rate_percent'] = round(
            parsed['total_requests'] / sent_requests * 100
            if sent_requests
            else 0,
            6,
        )
        parsed['successful_requests'] = (
            parsed['total_requests'] - parsed['errors']
        )
        cpu_match = re.search(
            r'Percent of CPU this job got:\s*([0-9.]+)%',
            output,
        )
        logical_cpu_match = re.search(
            r'^OCI_DSB_LOADGEN_LOGICAL_CPUS=([0-9]+)$',
            output,
            re.MULTILINE,
        )
        rss_match = re.search(
            r'Maximum resident set size \(kbytes\):\s*([0-9]+)',
            output,
        )
        if cpu_match:
            generator_cpu = float(cpu_match.group(1))
            parsed['load_generator_cpu_percent'] = generator_cpu
            logical_cpus = (
                int(logical_cpu_match.group(1))
                if logical_cpu_match
                else None
            )
            if logical_cpus:
                parsed['load_generator_logical_cpus'] = logical_cpus
                parsed['load_generator_capacity_used_percent'] = round(
                    generator_cpu / logical_cpus,
                    3,
                )
            if logical_cpus and generator_cpu >= logical_cpus * 95:
                parsed['load_generator_saturation_warning'] = (
                    'The load generator averaged at least 95% of its logical '
                    'aggregate CPU capacity; treat service throughput as a '
                    'possible generator-limited result.'
                )
        if rss_match:
            parsed['load_generator_peak_rss_kb'] = int(rss_match.group(1))
        return parsed

    if require_marker:
        raise ValueError(
            'wrk2 did not emit the injected DeathStarBench metrics marker; '
            'the workload Lua script may not have loaded.'
        )

    throughput_match = re.search(r'^Requests/sec:\s+([0-9.]+)', output, re.MULTILINE)
    requests_match = re.search(r'([0-9]+) requests in ', output)
    if not throughput_match or not requests_match:
        raise ValueError('wrk2 output did not contain throughput and request totals.')

    latency = {}
    for percentile in ('50', '99'):
        match = re.search(
            rf'^\s*{percentile}\.000%\s+([0-9.]+)(us|ms|s)\s*$',
            output,
            re.MULTILINE,
        )
        if match:
            latency[f'p{percentile}_ms'] = round(
                _latency_ms(match.group(1), match.group(2)), 3
            )
    p95_match = re.search(
        r'^\s*([0-9.]+)\s+0\.950000\s+[0-9]+\s+',
        output,
        re.MULTILINE,
    )
    if p95_match:
        latency['p95_ms'] = round(float(p95_match.group(1)), 3)
    if set(latency) != {'p50_ms', 'p95_ms', 'p99_ms'}:
        raise ValueError('wrk2 output did not contain p50, p95, and p99 latency.')

    socket_errors = 0
    socket_match = re.search(
        r'Socket errors: connect ([0-9]+), read ([0-9]+), '
        r'write ([0-9]+), timeout ([0-9]+)',
        output,
    )
    if socket_match:
        socket_errors = sum(int(value) for value in socket_match.groups())
    status_match = re.search(r'Non-2xx or 3xx responses: ([0-9]+)', output)
    status_errors = int(status_match.group(1)) if status_match else 0
    requests = int(requests_match.group(1))
    sent_match = re.search(r'^Sent\s+([0-9]+)\s+requests\s*$', output, re.MULTILINE)
    sent_requests = int(sent_match.group(1)) if sent_match else None
    if requests <= 0:
        raise ValueError('wrk2 completed without any requests.')
    if status_errors >= requests:
        raise ValueError('Every completed wrk2 request failed.')
    parsed = {
        'throughput_requests_per_second': float(throughput_match.group(1)),
        'total_requests': requests,
        'errors': status_errors,
        'socket_errors': socket_errors,
        'connect_errors': int(socket_match.group(1)) if socket_match else 0,
        'read_errors': int(socket_match.group(2)) if socket_match else 0,
        'write_errors': int(socket_match.group(3)) if socket_match else 0,
        'timeout_errors': int(socket_match.group(4)) if socket_match else 0,
        'error_rate_percent': round(
            (status_errors / requests * 100) if requests else 0,
            4,
        ),
        **latency,
    }
    if sent_requests is not None:
        if sent_requests < requests:
            raise ValueError(
                'wrk2 reported fewer sent requests than completed requests.'
            )
        parsed.update({
            'sent_requests': sent_requests,
            'uncompleted_requests': sent_requests - requests,
            'completion_rate_percent': round(
                requests / sent_requests * 100 if sent_requests else 0,
                6,
            ),
            'successful_requests': requests - status_errors,
        })
    return parsed


def metadata(workload_id, options, service_architecture, loadgen_architecture):
    settings = workload(workload_id)
    values = {
        'workload': settings['name'],
        'upstream_revision': REVISION,
        'container_runtime': 'Podman with podman-compose',
        'service_architecture': service_architecture,
        'load_generator_architecture': loadgen_architecture,
        'warmup_seconds': int(options.warmup_seconds),
        'duration_seconds': int(options.duration_seconds),
        'threads': int(options.threads),
        'connections': int(options.connections),
        'requested_requests_per_second': int(options.request_rate),
        'load_generator_sizing_note': (
            'The separate generator is fixed at 2 OCPUs and 8 GB. GNU time '
            'CPU utilization and peak RSS are included in measured results '
            'so generator saturation is visible.'
        ),
        'error_rate_definition': (
            'Non-2xx/3xx HTTP responses divided by completed HTTP responses; '
            'socket error events and uncompleted sent requests are reported '
            'separately.'
        ),
    }
    if workload_id in {'media_microservices', 'social_network'}:
        values['frontend_nginx_tuning'] = (
            'worker_processes=auto for the measured ingress; '
            'worker_connections=16384 and worker_rlimit_nofile=65536'
        )
    if workload_id == 'media_microservices':
        values['media_initializer_adaptations'] = (
            'client_body_buffer_size=64k keeps the pinned Lua handler request '
            'bodies in memory; the upstream cast character spelling and null '
            'thumbnail handling are corrected before initialization; duplicate '
            'movie-title registrations use the first mapping because the '
            'upstream MovieId service keys records by title.'
        )
    return values
