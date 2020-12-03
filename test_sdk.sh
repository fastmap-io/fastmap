detect-secrets scan --no-keyword-scan --no-basic-auth-scan > .secrets.baseline
coverage run --concurrency=multiprocessing -m pytest ./tests/test_sdk.py -vx || exit 1
coverage combine
coverage report --include=fastmap/sdk_lib.py,fastmap/__init__.py
coverage html --include=fastmap/sdk_lib.py,fastmap/__init__.py
