#!/bin/sh
# Exact-version dependency builder for the unreleased Media contexts.
set -eu
. /tmp/media-build/source.env

work=/tmp/media-dependency-build
mkdir "$work"
cd "$work"
licenses=/usr/share/licenses/deathstarbench/dependencies
mkdir -p "$licenses"

fetch() {
    name=$1
    url=$2
    expected=$3
    directory=$4
    curl --fail --show-error --location --retry 5 --retry-delay 5 "$url" -o "$name.tar.gz"
    printf '%s  %s\n' "$expected" "$name.tar.gz" | sha256sum -c -
    tar xzf "$name.tar.gz"
    mkdir -p "$licenses/$name"
    for candidate in "$directory"/LICENSE* "$directory"/LICENCE* "$directory"/COPYING* "$directory"/COPYRIGHT* "$directory"/NOTICE*; do
        if [ -f "$candidate" ]; then cp "$candidate" "$licenses/$name/"; fi
    done
}

cmake_install() {
    directory=$1
    shift
    mkdir "$directory/candidate-build"
    cd "$directory/candidate-build"
    cmake -DCMAKE_BUILD_TYPE=Release -DCMAKE_CXX_FLAGS=-fPIC -DCMAKE_C_FLAGS=-fPIC "$@" ..
    make -j2
    make install
    cd "$work"
}

fetch thrift "$THRIFT_URL" "$THRIFT_SHA256" thrift-0.12.0
cmake_install thrift-0.12.0 -DBUILD_TESTING=OFF -DBUILD_COMPILER=OFF -DBUILD_C_GLIB=OFF -DBUILD_CPP=ON -DWITH_QT5=OFF -DWITH_JAVA=OFF -DWITH_PYTHON=OFF -DWITH_HASKELL=OFF
fetch json "$JSON_URL" "$JSON_SHA256" json-3.6.1
cmake_install json-3.6.1 -DJSON_BuildTests=OFF
fetch yaml "$YAML_URL" "$YAML_SHA256" yaml-cpp-yaml-cpp-0.6.2
cmake_install yaml-cpp-yaml-cpp-0.6.2 -DYAML_CPP_BUILD_TESTS=OFF
fetch opentracing "$OPENTRACING_URL" "$OPENTRACING_SHA256" opentracing-cpp-1.5.1
# Jaeger's dynamic plugin links the static OpenTracing library. Nginx and the
# Lua bridge also need the shared library, so neither may be disabled.
cmake_install opentracing-cpp-1.5.1 -DBUILD_TESTING=OFF -DBUILD_MOCKTRACER=OFF -DBUILD_STATIC_LIBS=ON -DBUILD_SHARED_LIBS=ON
fetch jaeger "$JAEGER_URL" "$JAEGER_SHA256" jaeger-client-cpp-0.4.2
if [ "$1" = frontend ]; then
    cmake_install jaeger-client-cpp-0.4.2 -DHUNTER_ENABLED=OFF -DBUILD_TESTING=OFF -DJAEGERTRACING_WITH_YAML_CPP=ON -DJAEGERTRACING_BUILD_EXAMPLES=OFF -DJAEGERTRACING_BUILD_CROSSDOCK=OFF -DJAEGERTRACING_PLUGIN=ON
    cp "$work/jaeger-client-cpp-0.4.2/candidate-build/libjaegertracing_plugin.so" /usr/local/lib/
elif [ "$1" = app ]; then
    cmake_install jaeger-client-cpp-0.4.2 -DHUNTER_ENABLED=OFF -DBUILD_TESTING=OFF -DJAEGERTRACING_WITH_YAML_CPP=ON -DJAEGERTRACING_BUILD_EXAMPLES=OFF -DJAEGERTRACING_BUILD_CROSSDOCK=OFF
    fetch mongo "$MONGO_URL" "$MONGO_SHA256" mongo-c-driver-1.14.0
    cmake_install mongo-c-driver-1.14.0 -DENABLE_TESTS=OFF -DENABLE_EXAMPLES=OFF
    fetch jwt "$JWT_URL" "$JWT_SHA256" cpp-jwt-1.1.1
    cp -R cpp-jwt-1.1.1/include/jwt /usr/local/include/
    rm -r /usr/local/include/jwt/json
    # The archive bundles prebuilt JWT tests and their demo keys beneath its
    # include directory. They are not headers or runtime assets (and the
    # prebuilt binaries are x86-only); never copy them into either image.
    rm -r /usr/local/include/jwt/test
    sed -i 's/#include "jwt\/json\/json.hpp"/#include <nlohmann\/json.hpp>/g' /usr/local/include/jwt/jwt.hpp
    fetch redis "$REDIS_URL" "$REDIS_SHA256" cpp_redis-bbe38a7f83de943ffcc90271092d689ae02b3489
    fetch tacopie "$TACOPIE_URL" "$TACOPIE_SHA256" tacopie-243089d84a5a8032b85e81cae237b823df99abee
    # GitHub source archives do not contain submodules. Bake the exact gitlink
    # revision rather than resolving the submodule's default branch at build.
    cp -R tacopie-243089d84a5a8032b85e81cae237b823df99abee/. cpp_redis-bbe38a7f83de943ffcc90271092d689ae02b3489/tacopie/
    cmake_install cpp_redis-bbe38a7f83de943ffcc90271092d689ae02b3489 -DBUILD_TESTS=OFF -DBUILD_EXAMPLES=OFF
else
    echo 'Unknown Media dependency profile' >&2
    exit 1
fi
ldconfig
cd /tmp
rm -r "$work"
