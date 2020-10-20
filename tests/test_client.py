import base64
import functools
import io
import math
import multiprocessing
import pickle
import random
import re
import os
import sys
import time
import types

import pytest

sys.path.append(os.getcwd().split('/tests')[0])

from fastmap import (init, global_init, fastmap, _reset_global_config,
                     EveryProcessDead, client_lib, FastmapConfig)
# from fastmap.lib import FastmapConfig

TEST_SECRET = "TEST" * (64 // 4)


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
    assert isinstance(config, client_lib.FastmapConfig)
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
    _calc_pi_basic = functools.partial(calc_pi_basic, two=2.0)

    pi = 4.0 * sum(config.fastmap(_calc_pi_basic, range_100)) / len(range_100)
    assert pi == 3.12


def test_local_processes():
    config = init(exec_policy="LOCAL", local_processes=2)
    pi = 4.0 * sum(config.fastmap(calc_pi_basic, range(100))) / len(range(100))
    assert pi == 3.12

    # To get into local_processes <= 1
    config = init(exec_policy="LOCAL", local_processes=1)
    pi = 4.0 * sum(config.fastmap(calc_pi_basic, range(100))) / len(range(100))
    assert pi == 3.12


def test_exec_policy():
    with pytest.raises(AssertionError):
        init(exec_policy="INVALID")
    for exec_policy in ("LOCAL", "CLOUD", "ADAPTIVE"):
        init(exec_policy=exec_policy)


def test_verbosity(capsys):
    config = init(exec_policy="LOCAL", verbosity="QUIET")
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
    with pytest.raises(AssertionError):
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
    monkeypatch.setattr(client_lib.Mapper, "INITIAL_RUN_DUR", 0)
    range_100 = range(100)
    pi = 4.0 * sum(config.fastmap(calc_pi_basic, list(range_100))) / len(range_100)
    assert pi == 3.12
    pi = 4.0 * sum(config.fastmap(calc_pi_basic, iter(range_100))) / len(range_100)
    assert pi == 3.12


def test_process_local(monkeypatch):
    config = init(exec_policy="LOCAL")
    monkeypatch.setattr(client_lib.Mapper, "INITIAL_RUN_DUR", 0)
    monkeypatch.setattr(client_lib.Mapper, "PROC_OVERHEAD", 0)
    range_100 = range(100)
    pi = 4.0 * sum(config.fastmap(calc_pi_basic, list(range_100))) / len(range_100)
    assert pi == 3.12
    pi = 4.0 * sum(config.fastmap(calc_pi_basic, iter(range_100))) / len(range_100)
    assert pi == 3.12


def test_single_threaded_process(capsys, monkeypatch):
    config = init(exec_policy="LOCAL", local_processes=1)
    monkeypatch.setattr(client_lib.Mapper, "INITIAL_RUN_DUR", 0)
    monkeypatch.setattr(client_lib.Mapper, "PROC_OVERHEAD", 0)
    range_100 = range(100)
    pi = 4.0 * sum(config.fastmap(calc_pi_basic, list(range_100))) / len(range_100)
    assert pi == 3.12


def test_single_threaded_process_exception(capsys, monkeypatch):
    config = init(exec_policy="LOCAL", local_processes=1)
    monkeypatch.setattr(client_lib.Mapper, "INITIAL_RUN_DUR", 0)
    monkeypatch.setattr(client_lib.Mapper, "PROC_OVERHEAD", 0)
    with pytest.raises(AssertionError):
        list(config.fastmap(calc_pi_dead_99, range(100)))


def test_process_exception(capsys, monkeypatch):
    config = init(exec_policy="LOCAL", local_processes=2)
    monkeypatch.setattr(client_lib.Mapper, "INITIAL_RUN_DUR", 0)
    monkeypatch.setattr(client_lib.Mapper, "PROC_OVERHEAD", 0)
    with pytest.raises(EveryProcessDead):
        list(config.fastmap(calc_pi_dead_99, range(100)))


def test_process_adaptive(capsys, monkeypatch):
    # remote will die but this will continue
    config = init(exec_policy="ADAPTIVE")
    monkeypatch.setattr(client_lib.Mapper, "INITIAL_RUN_DUR", 0)
    monkeypatch.setattr(client_lib.Mapper, "PROC_OVERHEAD", 0)
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
    with pytest.raises(EveryProcessDead):
        sum(config.fastmap(lambda x: 1 / (x - 50), slow_gen(range(100))))
    with pytest.raises(EveryProcessDead):
        sum(config.fastmap(lambda x: 1 / (x - 50), slow_gen(list(range(100)))))


def test_order(monkeypatch):
    config = init(exec_policy="LOCAL")
    monkeypatch.setattr(client_lib.Mapper, "INITIAL_RUN_DUR", 0)
    monkeypatch.setattr(client_lib.Mapper, "PROC_OVERHEAD", 0)
    monkeypatch.setattr(client_lib._FillInbox, "BATCH_DUR_GOAL", .0001)
    order_range = list(config.fastmap(lambda x: int((x**2)**.5), range(10000)))
    assert order_range == list(range(10000))


def test_no_secret(monkeypatch, capsys):
    config = init(exec_policy="CLOUD")
    stdio = capsys.readouterr()
    assert re.search("fastmap WARNING:.*?LOCAL.*?$", stdio.out)
    assert config.exec_policy == "LOCAL"

    config = init(exec_policy="ADAPTIVE")
    stdio = capsys.readouterr()
    assert re.search("fastmap WARNING:.*?LOCAL.*?$", stdio.out)
    assert config.exec_policy == "LOCAL"


def test_remote_no_connection(monkeypatch, capsys):
    # disable_socket()
    config = init(exec_policy="CLOUD", verbosity="LOUD", secret=TEST_SECRET)
    config.cloud_url_base = "http://localhost:9999"
    monkeypatch.setattr(client_lib.Mapper, "INITIAL_RUN_DUR", 0)
    monkeypatch.setattr(client_lib.Mapper, "PROC_OVERHEAD", 0)
    range_100 = range(100)
    with pytest.raises(EveryProcessDead):
        list(config.fastmap(lambda x: x**.5, range_100))
    stdio = capsys.readouterr()
    assert re.search("could not connect", stdio.out)


def test_confirm_charges_basic(capsys, monkeypatch):
    # Basic local should not warn about confirming charges or any issues with
    # the secret
    config = init(exec_policy="LOCAL", local_processes=2)
    stdio = capsys.readouterr()
    assert not re.search("fastmap WARNING:.*?confirm_charges", stdio.out)
    assert not re.search("fastmap WARNING:.*?secret.*?LOCAL", stdio.out)
    assert isinstance(config, FastmapConfig)
    assert config.exec_policy == "LOCAL"

    # Basic cloud should warn about an absent secret and set execpolicy to local
    # (and say something about it)
    config = init(exec_policy="CLOUD", local_processes=2)
    stdio = capsys.readouterr()
    assert not re.search("fastmap WARNING:.*?confirm_charges", stdio.out)
    assert re.search("fastmap WARNING:.*?secret.*?LOCAL", stdio.out)
    assert config.exec_policy == "LOCAL"

    # If a secret is correctly provided for cloud, warn about confirming
    # charges and do not set to local config policy
    config = init(exec_policy="CLOUD", secret=TEST_SECRET, local_processes=2)
    stdio = capsys.readouterr()
    assert re.search("fastmap WARNING:.*?confirm_charges", stdio.out)
    assert not re.search("fastmap WARNING:.*?secret.*?LOCAL", stdio.out)
    assert config.exec_policy == "CLOUD"

    # If we set confirm charges, assert no warnings are thrown
    config = init(exec_policy="CLOUD", secret=TEST_SECRET, confirm_charges=True, local_processes=2)
    monkeypatch.setattr(client_lib.Mapper, "INITIAL_RUN_DUR", 0)
    monkeypatch.setattr(client_lib.Mapper, "PROC_OVERHEAD", 0)
    monkeypatch.setattr(client_lib.FastmapLogger, "input", fake_input_no)
    stdio = capsys.readouterr()
    assert not re.search("fastmap WARNING:.*?confirm_charges", stdio.out)
    assert not re.search("fastmap WARNING:.*?secret.*?LOCAL", stdio.out)
    assert config.exec_policy == "CLOUD"
    assert config.confirm_charges is True

    # Using the same config, ensure that every process dies with a fake url.
    # There should only be 1 process which can die
    config.cloud_url_base = "http://localhost:9999"
    with pytest.raises(EveryProcessDead):
        list(config.fastmap(lambda x: x**.5, range(100)))
    stdio = capsys.readouterr()
    assert re.search(r"Continue\?", stdio.out)

    with pytest.raises(EveryProcessDead):
        list(config.fastmap(lambda x: x**.5, iter(range(100))))
    stdio = capsys.readouterr()
    assert re.search(r"Continue anyway\?", stdio.out)

    # Adaptive should log cancelled
    config = init(exec_policy="ADAPTIVE", secret=TEST_SECRET, confirm_charges=True, local_processes=2)
    config.cloud_url_base = "http://localhost:9999"
    list(config.fastmap(lambda x: x**.5, range(100)))
    stdio = capsys.readouterr()
    assert re.search(r"fastmap INFO:.*?cancelled", stdio.out)

    # Test enter yes
    monkeypatch.setattr(client_lib.FastmapLogger, "input", fake_input_yes)
    config = init(exec_policy="ADAPTIVE", secret=TEST_SECRET, confirm_charges=True)
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
    monkeypatch.setattr(client_lib.FastmapLogger, "input", fake_input_try_again)
    config = init(exec_policy="ADAPTIVE", secret=TEST_SECRET, confirm_charges=True)
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
#     monkeypatch.setattr(client_lib.Mapper, "INITIAL_RUN_DUR", 0)
#     monkeypatch.setattr(client_lib.Mapper, "PROC_OVERHEAD", 0)

#     results = list(map(lambda x: 1/x, range(1, 100)))

#     resp = resp_dump({"status": "OK",
#                       "results": results[1:],
#                       "map_seconds": 5})
#     requests_mock.post('localhost:9999/api/v1/map',
#                        content=resp,
#                        status_code=200,
#                        headers=resp_headers())
#     config = init(exec_policy="CLOUD", secret=TEST_SECRET)
#     config.cloud_url_base = "localhost:9999"
#     assert math.isclose(sum(config.fastmap(lambda x: 1/x, range(1, 100))),
#                         sum(results))

# def test_remote_401(monkeypatch, requests_mock, capsys):
#     resp = resp_dump({"status": "UNAUTHORIZED",
#                       "reason": "UNAUTHORIZED"})
#     requests_mock.post('localhost:9999/api/v1/map',
#                        content=resp,
#                        status_code=401)
#     config = init(exec_policy="CLOUD", secret=TEST_SECRET)
#     monkeypatch.setattr(client_lib.Mapper, "INITIAL_RUN_DUR", 0)
#     monkeypatch.setattr(client_lib.Mapper, "PROC_OVERHEAD", 0)
#     with pytest.raises(EveryProcessDead):
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
#     monkeypatch.setattr(client_lib.Mapper, "INITIAL_RUN_DUR", 0)
#     monkeypatch.setattr(client_lib.Mapper, "PROC_OVERHEAD", 0)
#     with pytest.raises(EveryProcessDead):
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
#     monkeypatch.setattr(client_lib.Mapper, "INITIAL_RUN_DUR", 0)
#     monkeypatch.setattr(client_lib.Mapper, "PROC_OVERHEAD", 0)
#     with pytest.raises(EveryProcessDead):
#         # Unauthorized will kill the cloud thread
#         list(config.fastmap(sqrt, range(100)))
#     stdio = capsys.readouterr()
#     assert re.search("fastmap ERROR:.*?credits", stdio.out)


def test_fmt_bytes():
    assert client_lib.fmt_bytes(1023) == "1023.0B"
    assert client_lib.fmt_bytes(1024) == "1.0KB"
    assert client_lib.fmt_bytes(2048) == "2.0KB"
    assert client_lib.fmt_bytes(1024**2) == "1.0MB"
    assert client_lib.fmt_bytes(1024**2 * 2) == "2.0MB"
    assert client_lib.fmt_bytes(1024**3) == "1.0GB"
    assert client_lib.fmt_bytes(1024**3 * 2) == "2.0GB"


def test_fmt_time():
    assert client_lib.fmt_time(59) == "59s"
    assert client_lib.fmt_time(60) == "01:00"
    assert client_lib.fmt_time(61) == "01:01"
    assert client_lib.fmt_time(121) == "02:01"
    assert client_lib.fmt_time(60 * 60) == "01:00:00"
    assert client_lib.fmt_time(60 * 60 + 1) == "01:00:01"
    assert client_lib.fmt_time(60 * 60 + 61) == "01:01:01"


def test_namespace():
    ns = client_lib.Namespace("A", B="C")
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

    config = init(exec_policy="CLOUD", verbosity="LOUD", secret="0" * 64)
    list(config.fastmap(primeFactors, (BIG_NUM,)))
    list(config.fastmap(primeFactors, generator()))

    config = init(exec_policy="ADAPTIVE", verbosity="LOUD", secret="0" * 64)
    list(config.fastmap(primeFactors, (BIG_NUM,)))
    list(config.fastmap(primeFactors, generator()))


def test_log_etcetera(monkeypatch, capsys):
    # log functions that can't be captured in normal tests

    logger = client_lib.FastmapLogger("LOUD")
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

# def test

    # config = init(exec_policy="LOCAL", secret="0" * 64, verbosity="LOUD")
    # data = list(config.fastmap(primeFactors, range(15)))
    # assert data


    # def target():
    #     return list(config.fastmap(primeFactors, range(15)))
    # proc = multiprocessing.Process(target=target)
    # proc.start()
    # proc.join()





if __name__ == '__main__':
    pytest.main()
