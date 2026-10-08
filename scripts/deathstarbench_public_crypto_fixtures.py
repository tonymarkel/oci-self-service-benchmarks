"""Hash-only identities of published GnuTLS known-answer self-test PEM data.

These are public test vectors, not application credentials. Only exact PEM
hashes inside a native system libgnutls ELF are recognized; an unexpected key
or even a known fixture in a configuration/source file must still fail smoke.
"""

import re

GNUTLS_SOURCE = "https://raw.githubusercontent.com/gnutls/gnutls/8a36455fd75ce76391cfc00c53213d8b0e1648da/lib/crypto-selftests-pk.c"
GNUTLS_SOURCE_SHA256 = "fce9355507865b53ba0b5c5587932b9d8519be2cd195650b0d9ff69ac2c335c7"
# SHA-256 of the complete PEM with LF line endings and no trailing newline.
PUBLIC_PEM_SHA256 = frozenset({
    "d039c8119a029ab9f9c83c04d67002d887b6bc6026c4264402ab27cdf24cf138",
    "7c4c63ee462e0e700cd9e29c8e0f730b3f1b484c4abdd83f1e69fcd477c061fa",
    "91ea1699ff6b1a34b4a1d500a9c75a808441e47b9ea68da6fb0195e01ce1dc61",
    "934b3e9bfd9936c5e30c7f0ed7d36a05e2044f755c0aa433e1d6bef6a68d1391",
    "a0b9679665e29ac220dfb0b366317e1b9dd5fe31e9def8b2e511669a041389ae",
    "293216817f0d585f0b54f225582f59e7b2e2a795b5f8002e2d71c5e3d49ce9b0",
    "f8766749b0c6f9b5661364568226af0ba7ae8d87c0073df24e440de4417521cd",
    "ef237ea8db4f2ae9ee100e8ced96d29b5dceb0e6a948443e6b8a00b1791f9ec9",
    "fa0b06a72461ec0a963dcfccb8d5b61bd88a6074fc7271573bff68ab86b8c1af",
    "a4d138d7ef9748464117b44fb9c0a4b5b85a1599a127d02690abaa96d03c16e6",
    "5a09eb5df3674472eda0077d83cbccef72692c3d052ab393368ac57295439c58",
    "fed7079dd4491609e4c996e232f366ac434fa4cc9df19df363391d079d470964",
})


def system_gnutls_path(name: str) -> bool:
    return isinstance(name, str) and bool(re.fullmatch(r"/?(?:usr/)?lib(?:64)?/(?:[a-z0-9_-]+-linux-gnu/)?libgnutls\.so(?:\.[0-9]+)+", name))
