
import hashlib
import importlib
import inspect
import json
import logging
import threading
import time
import urllib
from enum import Enum
from modulefinder import ModuleFinder
from multiprocessing import Pool
from timeit import timeit

import psutil

_transfer_durs = {}


FASTMAP_DOCSTRING = """
    Map a function over an iterable and return the mapped results.

    Fastmap is a parallelized/distributed drop-in replacement for 'map'.
    It runs faster than the builtin map function in almost all circumstances.

    Processing can occur locally via multithreading or on the cloud via the fastmap.io
    service. Often, both the local and cloud environments will be utilized
    simultaneously. If not explicitly specified, the division of work between the
    local environment and the cloud will be determined automatically. If any processing
    is performed on the cloud, charges will apply according to the current pricing.
    For documentation on fastmap's cloud service, visit https://fastmap.io/cloud.

    Configuration is possible by either:
    - initializing configuration globally with fastmap_global_init
    - initializing configuration for one reusable FastmapConfig object with fastmap_init

    For more documentation on configuration, please refer to the docstring for fastmap_init.

    Fastmap assumes that calls to the mapped function will be deterministic for
    a given iterable element. As a result, processing may be done out-of-order.
    """

FASTMAP_INIT_DOCSTRING = """
    Named parameters:
    token: Available to be generated from fastmap.io/generate_token. If not included, fastmap will default to running locally
    log_level: One of python's log levels: DEBUG, INFO, WARNING, ERROR, CRITICAL. Default is INFO.
    exec_policy: Where to process the map. One of 'LOCAL', 'CLOUD', or 'ADAPTIVE' (default).
    all_local_cpus: By default, fastmap will utilize n - 1 avaiable threads. This is to allow your computer to remain performant for other tasks while fastmap is running. Passing 'all_local_cpus = True' will make fastmap run faster at the cost of usability for other tasks.
    confirm_charges: Before processing on the cloud, manually confirm cloud charges.
    """


def get_json(url):
    f = urllib.request.urlopen(url)
    return json.loads(f.read().decode('utf-8'))


def post_json(url, obj):
    if not isinstance(obj, str):
        obj = json.dumps(obj)
    data = urllib.parse.urlencode(obj).encode()
    req = urllib.request.Request(url, data=data)
    return json.loads(urllib.request.urlopen(req))


def set_interval(func, sec):
    def func_wrapper():
        set_interval(func, sec)
        func()
    t = threading.Timer(sec, func_wrapper)
    t.start()
    return t


def set_docstring(docstr, docstr_prefix=''):
    def wrap(func):
        func.__doc__ = docstr_prefix + docstr
        return func
    return wrap


class Defaults(Enum):
    CLOUD_URL = "localhost"


class ExecPolicy(Enum):
    ADAPTIVE = "ADAPTIVE"
    LOCAL = "LOCAL"
    CLOUD = "CLOUD"


class FastmapConfig(object):
    def __init__(self, token=None, log_level=logging.INFO, exec_policy=ExecPolicy.ADAPTIVE,
                 confirm_charges=False, all_local_cpus=False):
        self.token = token
        self.exec_policy = exec_policy
        self.all_local_cpus = all_local_cpus
        self.kb_avg = self.ping()
        self.log = logging.get_logger('fastmap')
        self.confirm_charges = confirm_charges

        try:
            self.num_threads = len(psutil.Process().cpu_affinity())
        except AttributeError:
            self.num_threads = psutil.cpu_count()
        if not self.all_local_cpus:
            self.num_threads -= 1

        if confirm_charges:
            logging.warning("")

    def ping(self):
        pass

    @set_docstring(FASTMAP_DOCSTRING)
    def fastmap(self, func, iterable):
        main_timer = timeit.Timer()

        iterable = list(iterable)  # TODO
        if not iterable:
            self.log.warning("Returning empty set due to empty iterable")
            return []

        self.log.info(f"Mapping {func.__name__} to {len(iterable)} items")

        # if isinstance(iterable, types.GeneratorType): # TODO
        #     first_batch = list(itertools.islice(iterable, num_threads))
        first_batch = iterable[:self.num_threads]
        iterable = iterable[self.num_threads:]
        avg_size_element = len(json.dumps(first_batch)) / self.num_threads

        mapped_results = []
        with Pool(self.num_threads) as local_pool:
            timed_func_results = local_pool.map(lambda e: _timed_func(func, e), first_batch)
        avg_local_dur = sum(r[0] for r in timed_func_results) / self.num_threads
        mapped_results += [r[1] for r in timed_func_results]

        sources = _get_full_sources(func)
        module_name = func.__module__.__name__
        func_name = func.__name__
        func_hash = _gen_hash(sources, module_name, func_name)

        if func_hash in _transfer_durs:
            avg_transfer_dur = _transfer_durs[func_hash]
        else:
            start_time = time.time()
            post_payload = json.dumps({"source": sources,
                                       "module_name": module_name,
                                       "func_name": func_name})
            resp = post_json(Defaults.CLOUD_URL + "/api/v1/init_map", post_payload)
            elapsed_time = time.time() - start_time
            transfer_time = elapsed_time - resp['elapsed_time']

        avg_transfer_dur = 0  # TODO
        if True:  # avg_local_dur < _config.avg_transfer_dur():
            self.log.info(f"Local-execution-time < item-transfer-time ({avg_local_dur}s < {avg_transfer_dur}s). Executing entire map locally"),
            with Pool(self.num_threads) as local_pool:
                mapped_results += local_pool.map(func, iterable)
            self.log.info(f"Processed {len(iterable)} items in {main_timer.timeit()}s")
            return mapped_results

        self.log.info(f"item-transfer-time < local-execution-time ({avg_local_dur}s < {avg_transfer_dur}s). Executing on cloud and locally"),


def _timed_func(func, *args):
    t = timeit.Timer()
    ret = func(*args)
    return (t.timeit(), ret)


def _get_source_of_module(module):
    # print("module", module)
    try:
        module_source = inspect.getsource(module)
    except OSError:
        return {}
    module_finder = ModuleFinder()
    module_finder.run_script(module.__file__)
    # ret = ast.parse(base_source)
    # print(dir(ret))
    accum = {
        module.__name__: module_source
        }

    for child_module_name, child_module_tuple in module_finder.modules.items():
        if child_module_name == "__main__":
            continue
        if child_module_name in accum:
            continue
        # found_module = imp.find_module(child_module_tuple.__name__)
        # child_module = imp.load_module(child_module_tuple.__name__, *found_module)
        child_module = importlib.import_module(child_module_tuple.__name__)
        print("processing module", child_module_tuple.__name__, child_module)
        accum.update(_get_source_of_module(child_module))

    return accum


def _get_full_sources(func):
    base_module = inspect.getmodule(func)
    return _get_source_of_module(base_module)

    # real_modules = {}

    # for module_name, module_source in sources.items():
    #     mymodule = imp.new_module(module_name)
    #     exec(module_source, mymodule.__dict__)
    #     real_modules[module_name] = mymodule

    # return real_modules['fastmaptest.unneccesary_math_1'].map_test(10)

# class Compute(Thread):
#     def __init__(self, request, func_hash):
#         Thread.__init__(self)
#         self.request = request
#         self.func_hash = func_hash

#     def run(self):
#         sources = self.request.json['source']
#         module_name = self.request.json['module_name']
#         func_name = self.request.json['func_name']

#         real_modules = {}
#         for single_module_name, single_module_source in sources.items():
#             single_module = imp.new_module(single_module_name)
#             exec(single_module_source, single_module.__dict__)
#             real_modules[single_module_name] = single_module

#         func = real_modules[module_name].locals()[func_name]
#         cache[self.func_hash] = func


def _gen_hash(source_code, module_name, func_name):
    to_encode = ','.join(json.dumps(source_code), module_name, func_name)
    sha = hashlib.sha256()
    sha.update(to_encode.encode())
    return sha.hexdigest()
