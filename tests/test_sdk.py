import base64
import gzip
import io
import math
import pickle
import random
import re
import os
import sys
import time
import types

import dill
import msgpack
import pytest
import requests_mock

sys.path.append(os.getcwd().split('/tests')[0])

from fastmap import (init, global_init, fastmap, _reset_global_config,
                     FastmapException, sdk_lib, ReturnType,
                     Verbosity, ExecPolicy)

TEST_SECRET = "abcd" * (64 // 4)


def flatten(lst):
    # https://stackoverflow.com/questions/952914
    return [el for sublst in lst for el in sublst]


def primeFactors(n):
    # adapted from https://www.geeksforgeeks.org/print-all-prime-factors-of-a-given-number/
    if n == 0:
        return []
    ret = []
    # Print the number of two's that divide n
    while n % 2 == 0:
        ret.append(2)
        n = n / 2

    # n must be odd at this point
    # so a skip of 2 ( i = i + 2) can be used
    for i in range(3, int(math.sqrt(n)) + 1, 2):
        # while i divides n , print i ad divide n
        while n % i == 0:
            ret.append(i)
            n = n / i

    # Condition if n is a prime
    # number greater than 2
    if n > 2:
        ret.append(2)
    return ret


def calc_pi_basic(seed, two=2.0):
    random.seed(seed)
    x = random.random() * two - 1.0
    y = random.random() * two - 1.0
    return 1 if x**2 + y**2 <= 1.0 else 0


def calc_pi_dead_99(seed):
    assert seed != 99
    random.seed(seed)
    x = random.random() * 2.0 - 1.0
    y = random.random() * 2.0 - 1.0
    return 1 if x**2 + y**2 <= 1.0 else 0


def fake_input_yes(self, msg):
    print(msg)
    return 'y'


def fake_input_no(self, msg):
    print(msg)
    return 'n'


def test_local_basic():
    config = init(exec_policy="LOCAL")
    assert isinstance(config, sdk_lib.FastmapConfig)
    range_100 = range(100)

    gen = config.fastmap(calc_pi_basic, range_100)
    assert isinstance(gen, types.GeneratorType)
    pi = 4.0 * sum(gen) / len(range_100)
    assert pi == 3.12

    gen = config.fastmap(calc_pi_basic, list(range_100))
    assert isinstance(gen, types.GeneratorType)
    pi = 4.0 * sum(gen) / len(range_100)
    assert pi == 3.12

    gen = config.fastmap(calc_pi_basic, iter(range_100))
    assert isinstance(gen, types.GeneratorType)
    pi = 4.0 * sum(gen) / len(range_100)
    assert pi == 3.12

    gen = config.fastmap(calc_pi_basic, set(range_100))
    pi = 4.0 * sum(gen) / len(range_100)
    assert pi == 3.12


def test_return_type_seq():
    assert ReturnType.ELEMENTS == "ELEMENTS"
    assert ReturnType.BATCHES == "BATCHES"
    assert set(ReturnType) == set(("ELEMENTS", "BATCHES"))

    range_0 = range(0)
    range_1 = range(1)
    range_100 = range(100)

    for verbosity in ("QUIET", "NORMAL"):
        config = init(exec_policy="LOCAL", verbosity=verbosity)
        with pytest.raises(FastmapException):
            list(config.fastmap(lambda x: x**.5, [], return_type="FAKE_RETURN_TYPE"))

        seq = config.fastmap(lambda x: x**.5, [], return_type="BATCHES")
        assert isinstance(seq, types.GeneratorType)
        assert list(seq) == []

        seq = config.fastmap(lambda x: x**.5, range_0, return_type="BATCHES")
        assert isinstance(seq, types.GeneratorType)
        assert list(seq) == []

        seq = config.fastmap(lambda x: x**.5, list(range_1), return_type="BATCHES")
        assert isinstance(seq, types.GeneratorType)
        seq = list(seq)
        assert len(seq) == 1
        assert isinstance(seq[0], list)

        seq = config.fastmap(lambda x: x**.5, range_1, return_type="BATCHES")
        assert isinstance(seq, types.GeneratorType)
        seq = list(seq)
        assert len(seq) == 1
        assert isinstance(seq[0], list)

        seq = config.fastmap(lambda x: x**.5, range_100, return_type="BATCHES")
        assert isinstance(seq, types.GeneratorType)
        seq = list(seq)
        assert all(isinstance(e, list) for e in seq)
        assert math.isclose(sum(flatten(seq)), 661.4629471031477)

        seq = config.fastmap(lambda x: x**.5, list(range_100), return_type="BATCHES")
        assert isinstance(seq, types.GeneratorType)
        seq = list(seq)
        assert all(isinstance(e, list) for e in seq)
        assert math.isclose(sum(flatten(seq)), 661.4629471031477)


class Wrapper():
    def __init__(self, x):
        self.x = x

    def sqrt(self):
        self.x = self.x**.5


def test_objects():
    def proc(x):
        x.sqrt()
        return x

    seq_1 = [Wrapper(1)]
    gen_1 = (Wrapper(x) for x in range(1, 2))
    seq_100 = [Wrapper(x) for x in range(100)]
    gen_100 = (Wrapper(x) for x in range(100))
    seq_200000 = [Wrapper(x) for x in range(200000)]
    gen_200000 = (Wrapper(x) for x in range(200000))

    config = init(exec_policy="LOCAL")
    res_seq_1 = list(config.fastmap(proc, seq_1))
    assert len(res_seq_1) == 1
    assert res_seq_1[0].x == 1
    res_gen_1 = list(config.fastmap(proc, gen_1))
    assert len(res_gen_1) == 1
    assert res_gen_1[0].x == 1

    res_seq_100 = list(config.fastmap(proc, seq_100))
    assert len(res_seq_100) == 100
    assert res_seq_100[99].x == 99 ** .5
    res_gen_100 = list(config.fastmap(proc, gen_100))
    assert len(res_gen_100) == 100
    assert res_gen_100[99].x == 99 ** .5

    res_seq_200000 = list(config.fastmap(proc, seq_200000))
    assert len(res_seq_200000) == 200000
    assert res_seq_200000[99999].x == 99999 ** .5
    res_gen_200000 = list(config.fastmap(proc, gen_200000))
    assert len(res_gen_200000) == 200000
    assert res_gen_200000[99999].x == 99999 ** .5


def test_local_empty():
    config = init(exec_policy="LOCAL")

    gen = config.fastmap(calc_pi_basic, [])
    assert isinstance(gen, types.GeneratorType)
    assert list(gen) == []

    gen = config.fastmap(calc_pi_basic, iter([]))
    assert isinstance(gen, types.GeneratorType)
    assert list(gen) == []


def test_local_no_init():
    _reset_global_config()
    range_100 = range(100)
    pi = 4.0 * sum(fastmap(calc_pi_basic, range_100)) / len(range_100)
    assert pi == 3.12


def test_local_global_init():
    global_init(exec_policy="LOCAL")
    range_100 = range(100)
    pi = 4.0 * sum(fastmap(calc_pi_basic, range_100)) / len(range_100)
    assert pi == 3.12


def test_local_functools():
    config = init(exec_policy="LOCAL")
    range_100 = range(100)
    pi = 4.0 * sum(config.fastmap(calc_pi_basic, range_100, kwargs={'two': 2.0})) / len(range_100)
    assert pi == 3.12


def test_max_local_workers():
    config = init(exec_policy="LOCAL", max_local_workers=2)
    pi = 4.0 * sum(config.fastmap(calc_pi_basic, range(100))) / len(range(100))
    assert pi == 3.12

    # To get into max_local_workers <= 1
    config = init(exec_policy="LOCAL", max_local_workers=1)
    pi = 4.0 * sum(config.fastmap(calc_pi_basic, range(100))) / len(range(100))
    assert pi == 3.12


def test_exec_policy():
    assert ExecPolicy.LOCAL == "LOCAL"
    assert ExecPolicy.CLOUD == "CLOUD"
    assert ExecPolicy.ADAPTIVE == "ADAPTIVE"
    assert set(ExecPolicy) == set(("LOCAL", "ADAPTIVE", "CLOUD"))

    with pytest.raises(FastmapException):
        init(exec_policy="INVALID")
    for exec_policy in ("LOCAL", "CLOUD", "ADAPTIVE"):
        init(exec_policy=exec_policy)


def test_verbosity(capsys):
    assert Verbosity.SILENT == "SILENT"
    assert Verbosity.QUIET == "QUIET"
    assert Verbosity.NORMAL == "NORMAL"
    assert Verbosity.LOUD == "LOUD"
    assert set(Verbosity) == set(("SILENT", "QUIET", "NORMAL", "LOUD"))

    config = init(exec_policy="LOCAL", verbosity="QUIET")
    list(config.fastmap(lambda x: x**x, range(10)))
    stdio = capsys.readouterr()
    assert stdio.out == ""
    config = init(exec_policy="LOCAL", verbosity="SILENT")
    list(config.fastmap(lambda x: x**x, range(10)))
    stdio = capsys.readouterr()
    assert stdio.out == ""
    config = init(exec_policy="ADAPTIVE", verbosity="QUIET")
    list(config.fastmap(lambda x: x**x, range(10)))
    stdio = capsys.readouterr()
    assert "fastmap WARNING:" in stdio.out
    config = init(exec_policy="LOCAL", verbosity="NORMAL")
    list(config.fastmap(lambda x: x**x, range(10)))
    stdio = capsys.readouterr()
    assert "fastmap INFO:" in stdio.out
    config = init(exec_policy="LOCAL", verbosity="LOUD")
    list(config.fastmap(lambda x: x**x, range(10)))
    stdio = capsys.readouterr()
    assert "fastmap DEBUG:" in stdio.out
    assert "fastmap INFO:" in stdio.out
    with pytest.raises(FastmapException):
        config = init(exec_policy="LOCAL", verbosity="FAKE")


def test_lambda():
    config = init(exec_policy="LOCAL")
    range_100 = range(100)
    with pytest.raises(ZeroDivisionError):
        # zero division error raises execution error
        sum(config.fastmap(lambda x: 1.0 / x, range_100))
    range_1_100 = range(1, 1000)
    the_sum = sum(config.fastmap(lambda x: 1.0 / x if x % 2 == 1 else -1.0 / x, range_1_100))
    assert math.isclose(the_sum, 0.6936474305598223)


def test_closure_basic():
    config = init(exec_policy="LOCAL")
    range_100 = range(100)
    with pytest.raises(ZeroDivisionError):
        # zero division error raises execution error
        sum(config.fastmap(lambda x: 1.0 / x, range_100))
    range_1_100 = range(1, 1000)

    def cl(x):
        if x % 2 == 1:
            return 1.0 / x
        else:
            return -1.0 / x

    the_sum = sum(config.fastmap(cl, range_1_100))
    assert math.isclose(the_sum, 0.6936474305598223)


def test_closure_real():
    config = init(exec_policy="LOCAL")
    range_100 = range(100)
    with pytest.raises(ZeroDivisionError):
        # zero division error raises execution error
        sum(config.fastmap(lambda x: 1.0 / x, range_100))
    range_1_100 = range(1, 1000)
    one = 1.0

    def cl(x):
        if x % 2 == 1:
            return one / x
        else:
            return -1 * one / x

    the_sum = sum(config.fastmap(cl, range_1_100))
    assert math.isclose(the_sum, 0.6936474305598223)


def test_single_threaded(monkeypatch):
    # Set initial run duration to make it not process everything on first run
    # but don't change proc_overhead so that it decides processes are too much
    config = init(exec_policy="LOCAL")
    monkeypatch.setattr(sdk_lib.Mapper, "INITIAL_RUN_DUR", 0)
    range_100 = range(100)
    pi = 4.0 * sum(config.fastmap(calc_pi_basic, list(range_100))) / len(range_100)
    assert pi == 3.12
    pi = 4.0 * sum(config.fastmap(calc_pi_basic, iter(range_100))) / len(range_100)
    assert pi == 3.12
    pi = 4.0 * sum(config.fastmap(calc_pi_basic, set(range_100))) / len(range_100)
    assert pi == 3.12


def test_process_local(monkeypatch):
    config = init(exec_policy="LOCAL")
    monkeypatch.setattr(sdk_lib.Mapper, "INITIAL_RUN_DUR", 0)
    monkeypatch.setattr(sdk_lib.Mapper, "PROC_OVERHEAD", 0)
    range_100 = range(100)
    pi = 4.0 * sum(config.fastmap(calc_pi_basic, list(range_100))) / len(range_100)
    assert pi == 3.12
    pi = 4.0 * sum(config.fastmap(calc_pi_basic, iter(range_100))) / len(range_100)
    assert pi == 3.12
    pi = 4.0 * sum(config.fastmap(calc_pi_basic, set(range_100))) / len(range_100)
    assert pi == 3.12


def test_single_threaded_process(capsys, monkeypatch):
    config = init(exec_policy="LOCAL", max_local_workers=1)
    monkeypatch.setattr(sdk_lib.Mapper, "INITIAL_RUN_DUR", 0)
    monkeypatch.setattr(sdk_lib.Mapper, "PROC_OVERHEAD", 0)
    range_100 = range(100)
    pi = 4.0 * sum(config.fastmap(calc_pi_basic, list(range_100))) / len(range_100)
    assert pi == 3.12


def test_single_threaded_process_exception(capsys, monkeypatch):
    config = init(exec_policy="LOCAL", max_local_workers=1)
    monkeypatch.setattr(sdk_lib.Mapper, "INITIAL_RUN_DUR", 0)
    monkeypatch.setattr(sdk_lib.Mapper, "PROC_OVERHEAD", 0)
    with pytest.raises(AssertionError):
        list(config.fastmap(calc_pi_dead_99, range(100)))


def test_process_exception(capsys, monkeypatch):
    config = init(exec_policy="LOCAL", max_local_workers=2)
    monkeypatch.setattr(sdk_lib.Mapper, "INITIAL_RUN_DUR", 0)
    monkeypatch.setattr(sdk_lib.Mapper, "PROC_OVERHEAD", 0)
    with pytest.raises(FastmapException):
        list(config.fastmap(calc_pi_dead_99, range(100)))


def test_process_adaptive(capsys, monkeypatch):
    # remote will die but this will continue
    config = init(exec_policy="ADAPTIVE")
    monkeypatch.setattr(sdk_lib.Mapper, "INITIAL_RUN_DUR", 0)
    monkeypatch.setattr(sdk_lib.Mapper, "PROC_OVERHEAD", 0)
    range_100 = range(100)
    pi = 4.0 * sum(config.fastmap(calc_pi_basic, list(range_100))) / len(range_100)
    assert pi == 3.12
    pi = 4.0 * sum(config.fastmap(calc_pi_basic, iter(range_100))) / len(range_100)
    assert pi == 3.12


def test_slow_generator():
    def slow_gen(iterable):
        for el in iterable:
            yield el
            time.sleep(.01)
    config = init(exec_policy="LOCAL")
    pi = 4.0 * sum(config.fastmap(calc_pi_basic, slow_gen(range(100)))) / 100
    assert pi == 3.12

    # test the do_die in the _FillInbox generators
    with pytest.raises(FastmapException):
        sum(config.fastmap(lambda x: 1 / (x - 50), slow_gen(range(100))))
    with pytest.raises(FastmapException):
        sum(config.fastmap(lambda x: 1 / (x - 50), slow_gen(list(range(100)))))


def test_order(monkeypatch):
    config = init(exec_policy="LOCAL")
    monkeypatch.setattr(sdk_lib.Mapper, "INITIAL_RUN_DUR", 0)
    monkeypatch.setattr(sdk_lib.Mapper, "PROC_OVERHEAD", 0)
    monkeypatch.setattr(sdk_lib._FillInbox, "BATCH_DUR_GOAL", .0001)
    order_range = list(config.fastmap(lambda x: int((x**2)**.5), range(10000)))
    assert order_range == list(range(10000))


def test_no_secret(monkeypatch, capsys):
    config = init(exec_policy="CLOUD")
    stdio = capsys.readouterr()
    assert re.search("fastmap WARNING:.*?LOCAL.\n", stdio.out)
    assert config.exec_policy == "LOCAL"

    config = init(exec_policy="ADAPTIVE")
    stdio = capsys.readouterr()
    assert re.search("fastmap WARNING:.*?LOCAL.\n", stdio.out)
    assert config.exec_policy == "LOCAL"


def test_remote_no_connection(monkeypatch, capsys):
    config = init(exec_policy="CLOUD", verbosity="LOUD", secret=TEST_SECRET,
                  cloud_url="localhost:9999")
    monkeypatch.setattr(sdk_lib.Mapper, "INITIAL_RUN_DUR", 0)
    monkeypatch.setattr(sdk_lib.Mapper, "PROC_OVERHEAD", 0)
    range_100 = range(100)
    with pytest.raises(FastmapException):
        list(config.fastmap(lambda x: x**.5, range_100))
    stdio = capsys.readouterr()
    assert re.search("could not connect", stdio.out)


def test_invalid_token(capsys):
    init(exec_policy="CLOUD", verbosity="LOUD", secret=None)
    for bad_token in (5, "_" * 64, "a" * 63, "a" * 65):
        with pytest.raises(FastmapException):
            init(exec_policy="CLOUD", verbosity="LOUD", secret=bad_token)
    init(exec_policy="CLOUD", verbosity="LOUD", secret="a"*64)


def test_confirm_charges_basic(capsys, monkeypatch):
    # Basic local should not warn about confirming charges or any issues with
    # the secret
    # config = init(exec_policy="LOCAL", max_local_workers=2)
    # stdio = capsys.readouterr()
    # assert not re.search("fastmap WARNING:.*?confirm_charges", stdio.out)
    # assert not re.search("fastmap WARNING:.*?secret.*?LOCAL", stdio.out)
    # assert isinstance(config, FastmapConfig)
    # assert config.exec_policy == "LOCAL"

    # # Basic cloud should warn about an absent secret and set execpolicy to local
    # # (and say something about it)
    # config = init(exec_policy="CLOUD", max_local_workers=2)
    # stdio = capsys.readouterr()
    # assert not re.search("fastmap WARNING:.*?confirm_charges", stdio.out)
    # assert re.search("fastmap WARNING:.*?secret.*?LOCAL", stdio.out)
    # assert config.exec_policy == "LOCAL"

    # If a secret is correctly provided for cloud, warn about confirming
    # charges and do not set to local config policy
    # config = init(exec_policy="CLOUD", secret=TEST_SECRET, max_local_workers=2)
    # stdio = capsys.readouterr()
    # assert re.search("fastmap WARNING:.*?confirm_charges", stdio.out)
    # assert not re.search("fastmap WARNING:.*?secret.*?LOCAL", stdio.out)
    # assert config.exec_policy == "CLOUD"

    # If we set confirm charges, assert no warnings are thrown
    config = init(exec_policy="CLOUD", secret=TEST_SECRET, cloud_url="https://a.a",
                  confirm_charges=True, max_local_workers=2)
    monkeypatch.setattr(sdk_lib.Mapper, "INITIAL_RUN_DUR", 0)
    monkeypatch.setattr(sdk_lib.Mapper, "PROC_OVERHEAD", 0)
    monkeypatch.setattr(sdk_lib.FastmapLogger, "input", fake_input_no)
    stdio = capsys.readouterr()
    assert not re.search("fastmap WARNING:.*?confirm_charges", stdio.out)
    assert not re.search("fastmap WARNING:.*?secret.*?LOCAL", stdio.out)
    assert config.exec_policy == "CLOUD"
    assert config.confirm_charges is True

    # Using the same config, ensure that every process dies with a fake url.
    # There should only be 1 process which can die
    config.cloud_url = "localhost:9999"
    monkeypatch.setattr(sdk_lib.AuthCheck, "was_success", lambda _: True)
    with pytest.raises(FastmapException):
        list(config.fastmap(lambda x: x**.5, range(100)))
    stdio = capsys.readouterr()
    assert re.search(r"Continue\?", stdio.out)
    with pytest.raises(FastmapException):
        list(config.fastmap(lambda x: x**.5, iter(range(100))))
    stdio = capsys.readouterr()
    assert re.search(r"Continue anyway\?", stdio.out)

    # Adaptive should log cancelled
    config = init(exec_policy="ADAPTIVE", secret=TEST_SECRET,
                  confirm_charges=True, max_local_workers=2,
                  cloud_url="localhost:9999/")
    list(config.fastmap(lambda x: x**.5, range(100)))
    stdio = capsys.readouterr()
    assert re.search(r"fastmap INFO:.*?cancelled", stdio.out)

    # Test enter yes
    monkeypatch.setattr(sdk_lib.FastmapLogger, "input", fake_input_yes)
    config = init(exec_policy="ADAPTIVE", secret=TEST_SECRET, confirm_charges=True,
                  cloud_url="https://a.a",)
    # monkeypatch.setattr('sys.stdin', io.StringIO('y\n'))
    data = list(config.fastmap(lambda x: x**.5, iter(range(100))))
    assert data

    def fake_input_try_again(self, msg, now={}):
        # clever 💯
        print(msg)
        if not now.get('done'):
            now['done'] = True
            return 'will repeat'
        return "n"

    # Test unrecognized input
    monkeypatch.setattr(sdk_lib.FastmapLogger, "input", fake_input_try_again)
    config = init(exec_policy="ADAPTIVE", secret=TEST_SECRET, cloud_url='https://a.a',
                  confirm_charges=True)
    list(config.fastmap(lambda x: x**.5, iter(range(100))))
    stdio = capsys.readouterr()
    assert "Unrecognized input" in stdio.out


def test_empty_remote():
    config = init(exec_policy="CLOUD")
    assert list(config.fastmap(lambda x: x**.5, [])) == []
    assert list(config.fastmap(lambda x: x**.5, iter([]))) == []
    config = init(exec_policy="ADAPTIVE")
    assert list(config.fastmap(lambda x: x**.5, [])) == []
    assert list(config.fastmap(lambda x: x**.5, iter([]))) == []


def resp_dump(resp_dict):
    return base64.b64encode(pickle.dumps(resp_dict))


def resp_headers():
    return {
        "X-Container-Id": "FAKE_ID",
        "X-Thread-Id": "FAKE_ID",
        "X-Process-Seconds": '4',
        "X-Total-Seconds": '5',
    }

# def test_remote_200(monkeypatch, requests_mock):
#     monkeypatch.setattr(sdk_lib.Mapper, "INITIAL_RUN_DUR", 0)
#     monkeypatch.setattr(sdk_lib.Mapper, "PROC_OVERHEAD", 0)

#     results = list(map(lambda x: 1/x, range(1, 100)))

#     resp = resp_dump({"status": "OK",
#                       "results": results[1:],
#                       "map_seconds": 5})
#     requests_mock.post('localhost:9999/api/v1/map',
#                        content=resp,
#                        status_code=200,
#                        headers=resp_headers())
#     config = init(exec_policy="CLOUD", secret=TEST_SECRET)
#     config.cloud_url = "localhost:9999"
#     assert math.isclose(sum(config.fastmap(lambda x: 1/x, range(1, 100))),
#                         sum(results))

# def test_remote_401(monkeypatch, requests_mock, capsys):
#     resp = resp_dump({"status": "UNAUTHORIZED",
#                       "reason": "UNAUTHORIZED"})
#     requests_mock.post('localhost:9999/api/v1/map',
#                        content=resp,
#                        status_code=401)
#     config = init(exec_policy="CLOUD", secret=TEST_SECRET)
#     monkeypatch.setattr(sdk_lib.Mapper, "INITIAL_RUN_DUR", 0)
#     monkeypatch.setattr(sdk_lib.Mapper, "PROC_OVERHEAD", 0)
#     with pytest.raises(FastmapException):
#         # Unauthorized will kill the cloud thread
#         list(config.fastmap(sqrt, range(100)))
#     stdio = capsys.readouterr()
#     assert re.search("fastmap ERROR:.*?Unauthorized", stdio.out)


# def test_remote_402(monkeypatch, requests_mock, capsys):
#     resp = resp_dump({"status": "NOT_ENOUGH_CREDITS",
#                       "reason": "You do not have any credits available"})
#     requests_mock.post('localhost:9999/api/v1/map',
#                        content=resp,
#                        status_code=402)
#     config = init(exec_policy="CLOUD", secret=TEST_SECRET)
#     monkeypatch.setattr(sdk_lib.Mapper, "INITIAL_RUN_DUR", 0)
#     monkeypatch.setattr(sdk_lib.Mapper, "PROC_OVERHEAD", 0)
#     with pytest.raises(FastmapException):
#         # Unauthorized will kill the cloud thread
#         list(config.fastmap(sqrt, range(100)))
#     stdio = capsys.readouterr()
#     assert re.search("fastmap ERROR:.*?credits", stdio.out)

# def test_remote_403(monkeypatch, requests_mock, capsys):
#     resp = resp_dump({"status": "NOT_ENOUGH_CREDITS",
#                       "reason": "You do not have any credits available"})
#     requests_mock.post('localhost:9999/api/v1/map',
#                        content=resp,
#                        status_code=402)
#     config = init(exec_policy="CLOUD", secret=TEST_SECRET)
#     monkeypatch.setattr(sdk_lib.Mapper, "INITIAL_RUN_DUR", 0)
#     monkeypatch.setattr(sdk_lib.Mapper, "PROC_OVERHEAD", 0)
#     with pytest.raises(FastmapException):
#         # Unauthorized will kill the cloud thread
#         list(config.fastmap(sqrt, range(100)))
#     stdio = capsys.readouterr()
#     assert re.search("fastmap ERROR:.*?credits", stdio.out)

def test_post_request(monkeypatch, capsys):
    url = "localhost:8888/api/v1/map"
    data = msgpack.dumps({"hello": "world"})
    secret = "secret_token"
    log = sdk_lib.FastmapLogger('QUIET')
    resp_headers = {
        "X-Status": "OK"
    }
    resp_dict = {
        "world": "hello"
    }

    # Bad content type on API call
    resp_headers['Content-Type'] = "text/html"
    with requests_mock.Mocker() as m:
        m.post(url, content=msgpack.dumps(resp_dict),
               headers=resp_headers)
        with pytest.raises(sdk_lib.CloudError):
            sdk_lib.post_request(url, data, secret, log)

    # Server warning and basic msgpack obj extraction
    resp_headers['Content-Type'] = "application/msgpack"
    resp_headers["X-Server-Warning"] = "abcdefg"
    with requests_mock.Mocker() as m:
        m.post(url, content=msgpack.dumps(resp_dict),
               headers=resp_headers)
        resp = sdk_lib.post_request(url, data, secret, log)
        stdio = capsys.readouterr()
        assert re.search("WARNING:[^\n]+ abcdefg", stdio.out)
        assert resp.obj['world'] == 'hello'
    del resp_headers["X-Server-Warning"]

    # Cloud error on 500 status code
    with requests_mock.Mocker() as m:
        m.post(url, content=msgpack.dumps(resp_dict),
               headers=resp_headers, status_code=500)
        with pytest.raises(sdk_lib.CloudError):
            sdk_lib.post_request(url, data, secret, log)

    # No Content signature on octet-stream
    resp_headers['Content-Type'] = "application/octet-stream"
    pickled_resp = gzip.compress(dill.dumps(resp_dict))
    with requests_mock.Mocker() as m:
        m.post(url, content=pickled_resp,
               headers=resp_headers)
        with pytest.raises(sdk_lib.CloudError):
            sdk_lib.post_request(url, data, secret, log)

    # Wrong content signature on octet-stream
    resp_headers['X-Content-Signature'] = "fake"
    with requests_mock.Mocker() as m:
        m.post(url, content=pickled_resp,
               headers=resp_headers)
        with pytest.raises(sdk_lib.CloudError):
            sdk_lib.post_request(url, data, secret, log)

    # Correct content signature. Extract works
    resp_headers['X-Content-Signature'] = sdk_lib.hmac_digest(secret, pickled_resp)
    with requests_mock.Mocker() as m:
        m.post(url, content=pickled_resp,
               headers=resp_headers)
        resp = sdk_lib.post_request(url, data, secret, log)
        assert resp.obj['world'] == 'hello'

    # Not gzipped
    pickled_resp = dill.dumps(resp_dict)
    resp_headers['X-Content-Signature'] = sdk_lib.hmac_digest(secret, pickled_resp)
    with requests_mock.Mocker() as m:
        m.post(url, content=pickled_resp,
               headers=resp_headers)
        with pytest.raises(sdk_lib.CloudError):
            sdk_lib.post_request(url, data, secret, log)

    # Not msgpacked
    pickled_resp = gzip.compress(str(resp_dict).encode())
    resp_headers['X-Content-Signature'] = sdk_lib.hmac_digest(secret, pickled_resp)
    with requests_mock.Mocker() as m:
        m.post(url, content=pickled_resp,
               headers=resp_headers)
        with pytest.raises(sdk_lib.CloudError):
            sdk_lib.post_request(url, data, secret, log)


def test_fmt_bytes():
    assert sdk_lib.fmt_bytes(1023) == "1023B"
    assert sdk_lib.fmt_bytes(1024) == "1.0KB"
    assert sdk_lib.fmt_bytes(2048) == "2.0KB"
    assert sdk_lib.fmt_bytes(1024**2) == "1.0MB"
    assert sdk_lib.fmt_bytes(1024**2 * 2) == "2.0MB"
    assert sdk_lib.fmt_bytes(1024**3) == "1.0GB"
    assert sdk_lib.fmt_bytes(1024**3 * 2) == "2.0GB"


def test_fmt_time():
    assert sdk_lib.fmt_time(59) == "59s"
    assert sdk_lib.fmt_time(60) == "01:00"
    assert sdk_lib.fmt_time(61) == "01:01"
    assert sdk_lib.fmt_time(121) == "02:01"
    assert sdk_lib.fmt_time(60 * 60) == "01:00:00"
    assert sdk_lib.fmt_time(60 * 60 + 1) == "01:00:01"
    assert sdk_lib.fmt_time(60 * 60 + 61) == "01:01:01"


def test_fmt_dur():
    assert sdk_lib.fmt_dur(.000009) == "0 milliseconds"
    assert sdk_lib.fmt_dur(.9) == "900 milliseconds"
    assert sdk_lib.fmt_dur(1) == "1.00 seconds"
    assert sdk_lib.fmt_dur(59) == "59.00 seconds"
    assert sdk_lib.fmt_dur(60) == "1.00 minutes"
    assert sdk_lib.fmt_dur(61) == "1.02 minutes"
    assert sdk_lib.fmt_dur(121) == "2.02 minutes"
    assert sdk_lib.fmt_dur(60 * 60) == "1.00 hours"
    assert sdk_lib.fmt_dur(60 * 60 + 1) == "1.00 hours"
    assert sdk_lib.fmt_dur(60 * 60 + 61) == "1.02 hours"


def test_namespace():
    ns = sdk_lib.Namespace("A", B="C")
    assert ns.A == "A"
    assert ns.B == "C"
    assert 'A' in ns
    assert 'B' in ns
    assert 'C' not in ns
    assert set(list(ns)) == set(['A', 'B'])
    with pytest.raises(AttributeError):
        ns.C


def test_short_func():
    SMALL_NUM = 15

    def generator():
        yield SMALL_NUM

    config = init(exec_policy="CLOUD", secret="0" * 64)
    list(config.fastmap(primeFactors, (SMALL_NUM,)))
    list(config.fastmap(primeFactors, generator()))

    config = init(exec_policy="ADAPTIVE", secret="0" * 64)
    list(config.fastmap(primeFactors, (SMALL_NUM,)))
    list(config.fastmap(primeFactors, generator()))

    config = init(exec_policy="LOCAL", secret="0" * 64)
    list(config.fastmap(primeFactors, (SMALL_NUM,)))
    list(config.fastmap(primeFactors, generator()))


def test_long_func(monkeypatch):
    # regression test for bug when on CLOUD exec_policy with a long initial
    # function which nevertheless clears out the iterable
    # also test with other exec policies and generator types
    # this is still only one number so will never actually call the cloud

    BIG_NUM = 29393395993999

    def generator():
        yield BIG_NUM

    config = init(exec_policy="LOCAL", verbosity="LOUD", secret="0" * 64)
    list(config.fastmap(primeFactors, (BIG_NUM,)))
    list(config.fastmap(primeFactors, generator()))

    monkeypatch.setattr(sdk_lib.AuthCheck, "was_success", lambda _: True)
    config = init(exec_policy="CLOUD", verbosity="LOUD", secret="0" * 64)
    list(config.fastmap(primeFactors, (BIG_NUM,)))
    list(config.fastmap(primeFactors, generator()))

    config = init(exec_policy="ADAPTIVE", verbosity="LOUD", secret="0" * 64)
    list(config.fastmap(primeFactors, (BIG_NUM,)))
    list(config.fastmap(primeFactors, generator()))


def test_log_etcetera(monkeypatch, capsys):
    # log functions that can't be captured in normal tests

    logger = sdk_lib.FastmapLogger("LOUD")
    logger.debug("Hello")
    stdio = capsys.readouterr()
    assert "fastmap DEBUG:" in stdio.out
    assert 'Hello' in stdio.out

    logger.hush()
    logger.error("Hello")
    stdio = capsys.readouterr()
    assert "" == stdio.out

    logger.restore_verbosity()
    logger.error("Hello")
    stdio = capsys.readouterr()
    assert "Hello" in stdio.out

    monkeypatch.setattr('sys.stdin', io.StringIO('y\n'))
    resp = logger.input("Hi")
    assert resp == 'y'
    stdio = capsys.readouterr()
    assert "Hi" in stdio.out


if __name__ == '__main__':
    pytest.main()
