> Note: Fastmap is currently in beta. 

![Version 0.0.4](https://img.shields.io/badge/version-0.0.4-red)

### Simple distributed computing

Fastmap is a drop-in replacement for `map` that makes your Python code in parallel on the cloud.

Fastmap is appropriate to use when `map` is too slow but setting up infrastructure would be overkill.


![Demo gif of fastmap. Text tutorial can be found below](assets/demo.gif)

- **🚀 Speed up parallel tasks**. Fastmap automatically parallelizes your code and distributes work across both your local machine and the fastmap cloud service. If the cloud won't speed things up, fastmap will do all processing locally and you won't be charged.
- **🐣 Trivial to setup**. Add `global_init` to the top of your file, and replace every instance of `map` with `fastmap`. There are no servers to provision or code to upload. The SDK consists of [only 3 functions](https://fastmap.io/docs#interface).

### Docs

For complete documentation, go to [https://fastmap.io/docs](https://fastmap.io/docs),


### Installation

```bash
pip install fastmap
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

As a rule-of-thumb, fastmap will speed up any call to map that would have otherwise taken more than one second. This is possible because, under the default ADAPTIVE execution policy, fastmap algorithmically distributes work between local execution and the fastmap.io cloud service.

If you are planning to use the 'CLOUD' exec_policy, which prevents local processing, fastmap is appropriate when your function is computationally-heavy. This is because transferring data to the cloud for processing always takes a non-zero amount of time. The tradeoff depends on your network speeds and distance to your fastmap server cluster.

If in doubt, try running fastmap with a small test dataset. Fastmap attempts to be transparent and will inform you when using it has made your code slower.


### Questions

Fastmap.io is a new project and I would love to hear your feedback. You can contact me directly at scott@fastmap.io.
