> Note: Fastmap is currently in alpha. Email me if you're interested in testing and I will happily give you lots of free compute time: scott@fastmap.io

![Version 0.0.1](https://img.shields.io/badge/version-0.0.1-red)

### Zero setup distributed computing

Fastmap is a drop-in replacement for `map` that makes your Python code in parallel on the cloud. Fastmap runs faster than map in most circumstances.

![Demo gif of fastmap. Text tutorial can be found below](assets/demo.gif)

- **🚀 Speed up your code**. Fastmap automatically parallelizes your code and distributes work across both your local machine and the fastmap cloud service. If the cloud won't speed things up, fastmap will do all processing locally and you won't be charged.
- **🐣 Trivial to setup**. Get an [API token](https://fastmap.io/) and replace every instance of `map` with `fastmap`. There are no servers to provision or code to upload. The SDK consists of only 3 functions.
- **💵 Cheaper than you think**. When you signup, you get $10 worth of credits for free. After that, it's $1 for 10 vCPU-hours + $1 per 10 GB returned.
- **🧟‍♂️ Continuity plan**. We know you depend on us. If for any reason, in the future, fastmap.io must shut down, everything will be open sourced.

Fastmap accelerates (often dramatically) the processing of data. Syntactically, it is all but equivalent to the builtin `map` function. Upon calling, fastmap calculates whether it would be faster to run map locally (in multiple processes) or upload most of the dataset for cloud processing. If no https://fastmap.io api token is used, fastmap will run everything locally. You are only charged when running fastmap in the cloud.

### Docs

For complete documentation, go to [https://fastmap.io/docs](https://fastmap.io/docs),


### Installation

```bash
python setup.py install
```

### Quickstart

```python
import csv
from config import FASTMAP_TOKEN
from my_project import big_function
import fastmap

# Important: Protect your API token like a password and never commit it to version control
config = fastmap.init(secret=FASTMAP_TOKEN)

with open('lots_of_data.csv') as fh:
    long_list = list(csv.reader(fh))

results = list(config.fastmap(big_function, long_list))

```


### When should you use fastmap?

Fastmap is best when map is too slow but setting up infrastracture would be overkill.

As a rule-of-thumb, fastmap will speed up any call to map that would have otherwise taken more than one second. This is possible because, under the default ADAPTIVE execution policy, fastmap algorithmically distributes work between local execution and the fastmap.io cloud service.

If you are planning to use the 'CLOUD' exec_policy, which prevents local processing, fastmap is appropriate when your function is computationally-heavy. This is because transferring data to the cloud for processing always takes a non-zero amount of time. The tradeoff depends on your network speeds and distance to the fastmap server cluster (GCP: US-Central1).

If in doubt, try running fastmap with a small test dataset. Fastmap attempts to be transparent and will inform you when using it has made your code slower.


### Questions

Fastmap.io is a new project and I would love to hear your feedback. You can contact me directly at scott@fastmap.io.
