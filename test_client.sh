coverage run --concurrency=multiprocessing -m pytest ./tests/test_client.py -vx || exit 1
coverage combine
coverage report --include=fastmap/client_lib.py,fastmap/__init__.py
coverage html --include=fastmap/client_lib.py,fastmap/__init__.py
