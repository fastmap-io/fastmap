Fastmap is a distributed drop-in replacement for `map`. It runs faster than the builtin map function in almost all cases. Fastmap is adaptively run both locally and the fastmap.io cloud service.

![Demo gif of fastmap. Text tutorial can be found below](assets/demo.gif)

- **🚀 Speed up your code**. Fastmap automatically parallelizes your code. We utilize both your local machine and our cloud service. If the cloud won't speed things up, fastmap will do all processing locally and you won't be charged.
- **🐣 Trivial to setup**. Get an [API token](https://fastmap.io/), add `global_init(...)` to the top of your file, and replace every instance of `map` with `fastmap`. There are no servers to provision or code to upload.
- **💵 Cheaper than you think**. We charge $0.05 per vCPU hour - comparable to AWS. On our service, $1 is enough to calculate 5 billion digits of pi.
- **🧟‍♂️ Continuity promise**. We know you depend on us. If for any reason, we are no longer able to keep fastmap.io running, we will open-source everything.

Fastmap accelerates (often dramatically) the processing of data. Syntactically, it is all but equivalent to the builtin `map` function. Upon calling, fastmap calculates whether it would be faster to run map locally (in multiple threads) or upload most of the dataset for cloud processing. If no https://fastmap.io api token is used, fastmap will run everything locally. You are only charged when running fastmap in the cloud. 

### Docs

For complete documentation, go to [https://fastmap.io/docs](https://fastmap.io/docs),


### Installation

```bash
pip install fastmap
```

Note that in some environments, you may need to use `sudo` and/or `pip3`

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

Use fastmap whenever you have a stateless function that needs to process many elements. Most calls to `map` will already fit this criteria. 


### Tips for specific use cases

**Case 1: You are a data scientist and want your data processing to go faster but you don't want accidental large charges.**
When calling fastmap_global_init, pass in the `confirm_charges=True` to get a confirmation dialog with the price before processing your data in the cloud. While waiting for your confirmation, your code will continue to be processed locally.

**Case 2: You are a startup with slow server code that doesn't want to deal with the hassle of setting up a lot of infrastructure**
In this case, you might consider using the `exec_policy=fastmap.ExecPolicies.CLOUD` option to ensure that all of your parallel processing happens on our servers and your servers don't get further overloaded.

**Case 3: You are a poor graduate student who can't afford cloud computing but still wants their code to run faster.**
No problem! You can still run fastmap without a fastmap.io account. Instead of parallizing your code in the cloud, fastmap will just take advantage of your multiple CPUs. It will run slower than it would using the cloud but faster than it would with `map` alone.


### Limitations

1. Fastmap.io is a prepaid service. You must have credits in your fastmap.io account to take advantage of the cloud functionality
2. Network calls are not supported. For security, we run your code in a sandbox. You cannot make calls to outside services.
3. Fastmap only works on code that is not stateful. Most calls to mao.


### Questions

Fastmap.io is a new project and we would love to hear your feedback. 
