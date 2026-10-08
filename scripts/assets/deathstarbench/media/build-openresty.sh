#!/bin/sh
# Source downloads are byte-checked before extraction, never fetched by name.
set -eu
. /tmp/media-build/source.env
work=/tmp/media-openresty-build
mkdir "$work"
cd "$work"
licenses=/usr/share/licenses/deathstarbench/dependencies
mkdir -p "$licenses"
fetch() {
    name=$1; url=$2; expected=$3; directory=$4
    curl --fail --show-error --location --retry 5 --retry-delay 5 "$url" -o "$name.tar.gz"
    printf '%s  %s\n' "$expected" "$name.tar.gz" | sha256sum -c -
    tar xzf "$name.tar.gz"
    mkdir -p "$licenses/$name"
    for candidate in "$directory"/LICENSE* "$directory"/COPYING* "$directory"/COPYRIGHT* "$directory"/NOTICE*; do
        if [ -f "$candidate" ]; then cp "$candidate" "$licenses/$name/"; fi
    done
}
fetch openssl "$OPENSSL_URL" "$OPENSSL_SHA256" openssl-1.1.0j
fetch pcre "$PCRE_URL" "$PCRE_SHA256" pcre-8.42
fetch nginx "$NGINX_URL" "$NGINX_SHA256" nginx-opentracing-0.8.0
fetch openresty "$OPENRESTY_URL" "$OPENRESTY_SHA256" openresty-1.15.8.1rc1
cd openresty-1.15.8.1rc1
./configure -j2 \
    --error-log-path=/dev/stderr --pid-path=/tmp/nginx.pid \
    --with-openssl="$work/openssl-1.1.0j" --with-pcre="$work/pcre-8.42" \
    --with-file-aio --with-http_addition_module --with-http_auth_request_module \
    --with-http_dav_module --with-http_flv_module --with-http_geoip_module=dynamic \
    --with-http_gunzip_module --with-http_gzip_static_module --with-http_image_filter_module=dynamic \
    --with-http_mp4_module --with-http_random_index_module --with-http_realip_module \
    --with-http_secure_link_module --with-http_slice_module --with-http_ssl_module \
    --with-http_stub_status_module --with-http_sub_module --with-http_v2_module \
    --with-http_xslt_module=dynamic --with-ipv6 --with-mail --with-mail_ssl_module \
    --with-md5-asm --with-pcre-jit --with-sha1-asm --with-stream --with-stream_ssl_module \
    --with-threads --add-dynamic-module="$work/nginx-opentracing-0.8.0/opentracing"
make -j2
make install
cd "$work"
fetch hmac "$HMAC_URL" "$HMAC_SHA256" lua-resty-hmac-23da759b69f208576526c8ac21b7c5ad66740321
cd lua-resty-hmac-23da759b69f208576526c8ac21b7c5ad66740321
# The upstream Makefile fetches lua-resty-string/master. OpenResty already
# bakes its exact bundled resty.string; copy only the pinned HMAC module.
install -d /usr/local/openresty/lualib/resty
install lib/resty/*.lua /usr/local/openresty/lualib/resty/
cp README.markdown "$licenses/hmac/README.markdown"
cd "$work"
fetch luarocks "$LUAROCKS_URL" "$LUAROCKS_SHA256" luarocks-3.5.0
cd luarocks-3.5.0
./configure --prefix=/usr/local/openresty/luajit --with-lua=/usr/local/openresty/luajit \
    --lua-suffix=jit-2.1.0-beta3 --with-lua-include=/usr/local/openresty/luajit/include/luajit-2.1
make build
make install
ln -sf /dev/stdout /usr/local/openresty/nginx/logs/access.log
ln -sf /dev/stderr /usr/local/openresty/nginx/logs/error.log
cd /tmp
rm -r "$work"
