import math
import random
import re
import os
import sys
import types

import pytest

sys.path.append(os.getcwd().split('/test')[0])

from fastmap import init, global_init, fastmap, _reset_global_config, ExecutionError, lib
# from fastmap.lib import FastmapConfig


def calc_pi_basic(seed):
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

def fake_input_try_again(self, msg):
    print(msg)
    return 'will repeat'


def test_local_basic():
    config = init(exec_policy="LOCAL")
    assert isinstance(config, lib.FastmapConfig)
    range_500 = range(500)

    gen = config.fastmap(calc_pi_basic, range_500)
    assert isinstance(gen, types.GeneratorType)
    pi = 4.0 * sum(gen) / len(range_500)
    assert pi == 3.128

    gen = config.fastmap(calc_pi_basic, list(range_500))
    assert isinstance(gen, types.GeneratorType)
    pi = 4.0 * sum(gen) / len(range_500)
    assert pi == 3.128

    gen = config.fastmap(calc_pi_basic, iter(range_500))
    assert isinstance(gen, types.GeneratorType)
    pi = 4.0 * sum(gen) / len(range_500)
    assert pi == 3.128

def test_local_no_init():
    _reset_global_config()
    range_500 = range(500)
    pi = 4.0 * sum(fastmap(calc_pi_basic, range_500)) / len(range_500)
    assert pi == 3.128

def test_local_global_init():
    global_init(exec_policy="LOCAL")
    range_500 = range(500)
    pi = 4.0 * sum(fastmap(calc_pi_basic, range_500)) / len(range_500)
    assert pi == 3.128

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

def test_lambda():
    config = init(exec_policy="LOCAL")
    range_500 = range(500)
    with pytest.raises(ZeroDivisionError):
        #zero division error raises execution error
        sum(config.fastmap(lambda x: 1.0/x, range_500))
    range_1_500 = range(1, 5000)
    the_sum = sum(config.fastmap(lambda x: 1.0/x if x%2 == 1 else -1.0/x, range_1_500))
    assert math.isclose(the_sum, 0.6932471905)

def test_process_local(capsys, monkeypatch):
    config = init(exec_policy="LOCAL")
    monkeypatch.setattr(lib.FastMapper, "INITIAL_RUN_DUR", 0)
    monkeypatch.setattr(lib.FastMapper, "PROCESS_OVERHEAD", 0)
    range_500 = range(500)
    pi = 4.0 * sum(config.fastmap(calc_pi_basic, list(range_500))) / len(range_500)
    assert pi == 3.128
    pi = 4.0 * sum(config.fastmap(calc_pi_basic, iter(range_500))) / len(range_500)
    assert pi == 3.128



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
    config = init(exec_policy="CLOUD", verbosity="LOUD", secret="TEST_TOKEN")
    config.cloud_url_base = "http://localhost:9999"
    monkeypatch.setattr(lib.FastMapper, "INITIAL_RUN_DUR", 0)
    monkeypatch.setattr(lib.FastMapper, "PROCESS_OVERHEAD", 0)
    range_500 = range(500)
    with pytest.raises(ExecutionError):
        list(config.fastmap(lambda x: x**.5, range_500))
    stdio = capsys.readouterr()
    assert re.search("fastmap ERROR:.*?could not connect", stdio.out)

def test_confirm_charges(capsys, monkeypatch):
    init(exec_policy="LOCAL")
    stdio = capsys.readouterr()
    assert not re.search("fastmap WARNING:.*?confirm_charges", stdio.out)
    assert not re.search("fastmap WARNING:.*?secret.*?LOCAL", stdio.out)

    init(exec_policy="CLOUD")
    stdio = capsys.readouterr()
    assert not re.search("fastmap WARNING:.*?confirm_charges", stdio.out)
    assert re.search("fastmap WARNING:.*?secret.*?LOCAL", stdio.out)

    init(exec_policy="CLOUD", secret="TEST_SECRET")
    stdio = capsys.readouterr()
    assert re.search("fastmap WARNING:.*?confirm_charges", stdio.out)
    assert not re.search("fastmap WARNING:.*?secret.*?LOCAL", stdio.out)

    config = init(exec_policy="CLOUD", secret="TEST_SECRET", confirm_charges=True)
    monkeypatch.setattr(lib.FastMapper, "INITIAL_RUN_DUR", 0)
    monkeypatch.setattr(lib.FastMapper, "PROCESS_OVERHEAD", 0)
    monkeypatch.setattr(lib.FastmapLogger, "input", fake_input_no)
    stdio = capsys.readouterr()
    assert not re.search("fastmap WARNING:.*?confirm_charges", stdio.out)
    assert not re.search("fastmap WARNING:.*?secret.*?LOCAL", stdio.out)

    config.cloud_url_base = "http://localhost:9999"
    with pytest.raises(ExecutionError):
        list(config.fastmap(lambda x: x**.5, range(500)))
    stdio = capsys.readouterr()
    assert re.search(r"Continue\?", stdio.out)

    with pytest.raises(ExecutionError):
        list(config.fastmap(lambda x: x**.5, iter(range(500))))
    stdio = capsys.readouterr()
    assert re.search(r"Continue anyway\?", stdio.out)




if __name__ == '__main__':
    pytest.main()
