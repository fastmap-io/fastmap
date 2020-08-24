coverage run --concurrency=multiprocessing tests/test_client.py
coverage combine
coverage report --include=fastmap/*
coverage html --include=fastmap/*
