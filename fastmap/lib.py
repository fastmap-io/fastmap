import base64
import hashlib
import logging
# import json
import threading
# import time
# import urllib
from enum import Enum
from multiprocessing import Pool, cpu_count
from timeit import timeit

# import capnp
# import fastmap_capnp
import dill
# import psutil
import requests

_transfer_durs = {}
dill.settings['recurse'] = True


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
    verbosity: One of 'QUIET', 'NORMAL', or 'LOUD'. Default is 'normal'.
    exec_policy: Where to process the map. One of 'LOCAL', 'CLOUD', or 'ADAPTIVE' (default).
    all_local_cpus: By default, fastmap will utilize n - 1 avaiable threads. This is to allow your computer to remain performant for other tasks while fastmap is running. Passing 'all_local_cpus = True' will make fastmap run faster at the cost of usability for other tasks.
    confirm_charges: Before processing on the cloud, manually confirm cloud charges.
    """



# def get_json(url):
#     f = urllib.request.urlopen(url)
#     return json.loads(f.read().decode('utf-8'))


# def post_json(url, obj):
#     if not isinstance(obj, str):
#         obj = json.dumps(obj)
#     data = urllib.parse.urlencode(obj).encode()
#     req = urllib.request.Request(url, data=data)
#     return json.loads(urllib.request.urlopen(req))


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


def encode(raw_data):
    return base64.b64encode(dill.dumps(raw_data)).decode()


def decode(encoded_data):
    return dill.loads(base64.b64decode(encoded_data.encode()))


class ExecPolicy(object):
    ADAPTIVE = "ADAPTIVE"
    LOCAL = "LOCAL"
    CLOUD = "CLOUD"


class Verbosity(object):
    QUIET = "QUIET"
    NORMAL = "NORMAL"
    LOUD = "LOUD"


class FastmapConfig(object):
    """
    The configuration object. Do not instantiate this directly. Instead, use fastmap_init or
    fastmap_global_init.

    This object exposes one public method: fastmap.
    """

    def __init__(self, token=None, verbosity=Verbosity.NORMAL, exec_policy=ExecPolicy.ADAPTIVE,
                 confirm_charges=False, all_local_cpus=False):
        self.token = token
        self.exec_policy = exec_policy
        self.all_local_cpus = all_local_cpus
        self._init_logging(verbosity)


        self.log.info("Setup fastmap")
        self.log.info(f" verbosity: {verbosity}.")
        self.log.info(f" exec_policy: {exec_policy}")

        # formatter = logging.Formatter("%(asctime)s - %(message)s")
        # self.log.setFormatter(formatter)

        # self.log.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
        self.confirm_charges = confirm_charges
        self.cloud_url_base = "https://fastmap.io"


        # self.client = capnp.TwoPartyClient(Defaults.CLOUD_URL)
        # self.fastmap_wrapper = self.client.bootstrap().cast_as(fastmap_capnp.Fastmap)

        # try:
        #     self.num_threads = len(psutil.Process().cpu_affinity())
        # except AttributeError:
        #     self.num_threads = psutil.cpu_count()
        self.num_threads = cpu_count()
        if not self.all_local_cpus:
            self.num_threads -= 1

        if not confirm_charges and exec_policy != ExecPolicy.LOCAL:
            self.log.warning("WARNING: Argument 'confirm_charges' is False. Your prepaid balance may be charged for usage without confirmation")

    @set_docstring(FASTMAP_DOCSTRING)
    def fastmap(self, func, data):
        data = list(data)
        encoded_function = encode(func)
        encoded_data = encode(data)
        self.log.info(f"Running fastmap on {func.__name__} with {len(data)} elements")
        self.log.info(f"Function payload {len(encoded_function):n} bytes")
        self.log.info(f"Data payload {len(encoded_data):n} bytes")
        payload = {'func': encoded_function, 'data': encoded_data}
        resp = requests.post(self.cloud_url_base + '/api/v1/execute', json=payload)
        if resp.status_code != 200:
            raise AssertionError("Error: %r" % resp.reason)
        self.log.info("Responded in %.2f", resp.json()['elapsed_time'])
        return decode(resp.json().get('result'))

    def _init_logging(self, verbosity):
        self.log = logging.getLogger('fastmap')

        ch = logging.StreamHandler()
        if verbosity == Verbosity.QUIET:
            self.log.setLevel(logging.CRITICAL)
        elif verbosity == Verbosity.NORMAL:
            self.log.setLevel(logging.WARNING)
        elif verbosity == Verbosity.LOUD:
            self.log.setLevel(logging.INFO)
        else:
            raise AssertionError(f"Unknown value for verbosity '{verbosity}'")

        # create formatter
        formatter = logging.Formatter('%(name)s: %(message)s')

        # add formatter to ch
        ch.setFormatter(formatter)

        # add ch to logger
        self.log.addHandler(ch)


    # @staticmethod
    # def _get_payload(func, data):


        # return dill.loads(response.result.data)


    # def fastmap(self, func, iterable):
    #     main_timer = timeit.Timer()

    #     iterable = list(iterable)  # TODO
    #     if not iterable:
    #         self.log.warning("Returning empty set due to empty iterable")
    #         return []

    #     self.log.info(f"Mapping {func.__name__} to {len(iterable)} items")

    #     # if isinstance(iterable, types.GeneratorType): # TODO
    #     #     first_batch = list(itertools.islice(iterable, num_threads))
    #     first_batch = iterable[:self.num_threads]
    #     iterable = iterable[self.num_threads:]

    #     mapped_results = []
    #     with Pool(self.num_threads) as local_pool:
    #         timed_func_results = local_pool.map(lambda e: _timed_func(func, e), first_batch)
    #     avg_local_dur = sum(r[0] for r in timed_func_results) / self.num_threads
    #     mapped_results += [r[1] for r in timed_func_results]

    #     serialized_bytes = dill.dumps(func)
    #     func_hash = _gen_hash(serialized_bytes)

    #     if func_hash in _transfer_durs:
    #         avg_transfer_dur = _transfer_durs[func_hash]
    #     else:
    #         start_time = time.time()
    #         post_payload = json.dumps({"func": serialized_bytes, "data": first_batch})
    #         resp = post_json(Defaults.CLOUD_URL + "/api/v1/init_map", post_payload)
    #         elapsed_time = time.time() - start_time
    #         transfer_time = elapsed_time - resp['elapsed_time']

    #     avg_transfer_dur = 0  # TODO
    #     if True:  # avg_local_dur < _config.avg_transfer_dur():
    #         self.log.info(f"Local-execution-time < item-transfer-time ({avg_local_dur}s < {avg_transfer_dur}s). Executing entire map locally"),
    #         with Pool(self.num_threads) as local_pool:
    #             mapped_results += local_pool.map(func, iterable)
    #         self.log.info(f"Processed {len(iterable)} items in {main_timer.timeit()}s")
    #         return mapped_results

    #     self.log.info(f"item-transfer-time < local-execution-time ({avg_local_dur}s < {avg_transfer_dur}s). Executing on cloud and locally"),


def _timed_func(func, *args):
    t = timeit.Timer()
    ret = func(*args)
    return (t.timeit(), ret)


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


def _gen_hash(serialized_bytes):
    sha = hashlib.sha256()
    sha.update(serialized_bytes)
    return sha.hexdigest()
