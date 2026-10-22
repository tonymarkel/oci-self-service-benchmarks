"""Single-use deterministic Media API initializer; standard library only.

Successful upstream mutations return an empty HTTP 200. The caller verifies
MongoDB after initialization and after the canary. Never retry mutations.
"""
import argparse
from concurrent.futures import ThreadPoolExecutor
import ipaddress
import json
import re
import urllib.parse
import urllib.request


def post(target, route, values):
    address = str(ipaddress.IPv4Address(target))
    request = urllib.request.Request('http://' + address + ':8080/wrk2-api/' + route,
                                     data=urllib.parse.urlencode(values).encode())
    with urllib.request.urlopen(request, timeout=30) as response:
        body = response.read(65537)
        if response.status != 200 or body.strip():
            raise RuntimeError('Unexpected Media mutation response: HTTP ' + str(response.status))


def seed(target, titles, workers=8):
    if not 1 <= workers <= 16:
        raise ValueError('Media seeding requires 1 through 16 workers')
    if not titles or any(not isinstance(title, str) or not title for title in titles):
        raise ValueError("Media requires the published driver title list")
    movies = {}
    for index, title in enumerate(titles, 1):
        movies.setdefault(title, "movie_id_" + str(index))
    def register(index):
        post(target, 'user/register', {'first_name': 'first_name_' + str(index),
             'last_name': 'last_name_' + str(index), 'username': 'username_' + str(index),
             'password': 'password_' + str(index)})
    def register_movie(item):
        title, movie_id = item
        post(target, 'movie/register', {'title': title, 'movie_id': movie_id})
    # Queue at most one bounded batch at a time; failures stop further batches.
    with ThreadPoolExecutor(max_workers=workers) as pool:
        for start in range(1, 1001, workers):
            list(pool.map(register, range(start, min(start + workers, 1001))))
        items = list(movies.items())
        for start in range(0, len(items), workers):
            list(pool.map(register_movie, items[start:start + workers]))
    return {'users_registered': 1000, 'movies_registered': len(movies)}


def canary(target, run_id, title):
    if not re.fullmatch('[0-9a-f]{12}', run_id):
        raise ValueError('Invalid Media run identity')
    text = 'benchmark-media-canary-' + run_id
    post(target, 'review/compose', {'username': 'username_1', 'password': 'password_1',
                                  'title': title, 'rating': '5', 'text': text})
    return {'canary_text': text, 'http_status': 200}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('mode', choices=('seed', 'canary'))
    parser.add_argument('--target', required=True)
    parser.add_argument('--run-id', required=True)
    parser.add_argument('--titles-json', required=True)
    args = parser.parse_args()
    titles = json.loads(args.titles_json)
    result = seed(args.target, titles) if args.mode == 'seed' else canary(args.target, args.run_id, titles[0])
    print('MEDIA_API_RESULT ' + json.dumps({'mode': args.mode, 'run_id': args.run_id, **result}, sort_keys=True))


if __name__ == '__main__':
    main()
