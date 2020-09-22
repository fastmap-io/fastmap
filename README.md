> Note: Fastmap is currently in alpha and new fastmap.io accounts are available by request only. **Alpha software often has bugs and service disruptions.** While unintended, expect both until fastmap graduates to beta. If you are reading this, you are the salt-of-the-earth and I could use your help working out the kinks. Email me at scott@fastmap.io for a token!

![Version 0.0.1](https://img.shields.io/badge/version-0.0.1-red)

### Zero setup distributed computing

Fastmap is a drop-in replacement for `map` that makes your code run faster. It accomplishes this by utilizing both our cloud services and your local machine. Fastmap runs faster than map in almost all circumstances.

![Demo gif of fastmap. Text tutorial can be found below](assets/demo.gif)

- **🚀 Speed up your code**. Fastmap automatically parallelizes your code. We utilize both our cloud service and your local machine. If the cloud won't speed things up, fastmap will do all processing locally and you won't be charged.
- **🐣 Trivial to setup**. Get an [API token](https://fastmap.io/) and replace every instance of `map` with `fastmap`. There are no servers to provision or code to upload.
- **💵 Cheaper than you think**. When you signup, you get 10 [vCPU](https://www.techopedia.com/definition/30859/vcpu)-hours for free. After that, we charge $10 for 200 vCPU-hours or $90 for 2000.
- **🧟‍♂️ Continuity promise**. We know you depend on us. If for any reason, we are no longer able to keep fastmap.io running, we will open-source everything.

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
from fastmap import global_init, fastmap
from my_project import big_function
from config import FASTMAP_TOKEN

# Important: Protect your API token like a password and never commit it to version control
global_init(secret=FASTMAP_TOKEN)

with open('lots_of_data.csv') as fh:
    long_list = list(csv.reader(fh))

results = fastmap(big_function, long_list)

```


### When should you use fastmap?

Fastmap is best when map is too slow but setting up a Spark cluster or deploying a bunch of Lambdas would be overkill.

As a rule-of-thumb, fastmap will speed up any call to map that would have otherwise taken more than one second. This is possible because, under the default ADAPTIVE execution policy, fastmap algorithmically distributes work between local execution and the fastmap.io cloud service.

If you are planning to only use the cloud service, fastmap is appropriate when your function is computationally-heavy. This is because transferring data to the cloud for processing always takes a non-zero amount of time. The tradeoff depends on your network speeds and distance to the fastmap server cluster (GCP: US-West).

If in doubt, try running fastmap with a small test dataset. Fastmap attempts to be transparent and will inform you when using fastmap has made your code slower.

### Tips for specific use cases

**Case 1: You are a data scientist and want your data processing to go faster but you don't want accidental large charges.**
When calling fastmap_global_init, pass in the `confirm_charges=True` to get a confirmation dialog with the price before processing your data in the cloud. While waiting for your confirmation, your code will continue to be processed locally.

**Case 2: You are a startup with slow server code that doesn't want to deal with the hassle of setting up a lot of infrastructure**
In this case, you might consider using the `exec_policy="CLOUD"` option to ensure that all of your parallel processing happens on our servers and your servers don't get further overloaded.

**Case 3: You are a poor graduate student who can't afford cloud computing but still wants their code to run faster.**
No problem! You can still run fastmap without a fastmap.io account. Instead of parallizing your code in the cloud, fastmap will just take advantage of multiprocessing.


### Questions

Fastmap.io is a new project and I would love to hear your feedback. You can contact me directly at scott@fastmap.io.
