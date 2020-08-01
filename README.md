Fastmap is a distributed drop-in replacement for `map`. It runs faster than the builtin map function in almost all circumstances.

- **Speed up your code**. Fastmap automatically parallelizes your code on the [fastmap.io](https://fastmap.io) cloud service. 
- **Trivial to setup**. Get an [API token](https://fastmap.io/generate_api_toke), add `fastmap_global_init(...)` to the top of your file, and replace every instance of `map` with `fastmap`. There are no servers to provision or code to upload.
- **Cheaper than you think**. We aim to charge the same to you as our infrastructure providers charge to us. See our [current prices](https://fastmap.io/prices). If our cloud service won't speed things up, fastmap will do all processing locally and you won't be charged.

![Demo gif of fastmap. Text tutorial can be found below](demo.gif)

Fastmap accelerates (often dramatically) the processing of data. Syntactically, it is all but equivalent to the builtin `map` function. Upon calling, fastmap calculates whether it would be faster to run map locally (in multiple threads) or upload most of the dataset for cloud processing. If no https://fastmap.io token is used, fastmap will run everything locally. You are only charged when running fastmap in the cloud. 

```python
import csv
from fastmap import fastmap_init_global, fastmap

fastmap_global_init(MY_FASTMAP_CLOUD_TOKEN)

def resource_intensive_function(element):
    ...
    return calculated_value

with open('big_csv_file.csv') as file_handler:
    long_list_of_data = list(csv.reader(file_handler))

results = fastmap(resource_intensive_function, long_list_of_data)

```

### Installation

```bash
pip install fastmap
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
