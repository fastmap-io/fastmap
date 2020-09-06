coverage run --concurrency=multiprocessing tests/test_client.py || exit 1
coverage combine
coverage report --include=fastmap/*
coverage html --include=fastmap/*
