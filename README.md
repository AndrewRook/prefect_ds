# prefect DS
Tools for making Prefect work better for typical data science workflows.

# Install

```bash
$ pip install prefect-ds
```

# Usage

`prefect_ds` is a lightweight wrapper around [`Prefect`](https://docs.prefect.io/), designed
to make it easier to run workflows I typically encounter when doing data science — especially
tasks related to analyzing large datasets and building models. Specifically, it implements the 
following:

## [`PandasResultHandler`](prefect_ds/pandas_result_handler.py)

A result handler that reads to and writes from Pandas DataFrames. It should be able to handle
any file type Pandas supports, and unlike built-in handlers like `LocalResultHandler` requires
the full specification of the file path — this makes it easy to inspect task results, or
use those results in other analysis. It also has support for templating, so task arguments
can be injected into the filenames (useful for things like `map`).

```python
>>> import os
>>> os.environ["PREFECT__LOGGING__LEVEL"] = "ERROR"

>>> import pandas as pd
>>> import time

>>> from prefect import task
>>> from prefect_ds.pandas_result_handler import PandasResultHandler 

>>> # Note the use of the task argument {id} as a template in the filename
>>> @task(result_handler=PandasResultHandler("data_{id}.csv", "csv"))
... def demo_task(id):
...     time.sleep(5)
...     return pd.DataFrame({"one": [1, 2, 3], "two": [4, 5, 6]})

```

Note that in order to use the templating functionality of `PandasResultHandler`, you will need
to run your flow using the `DSTaskRunner` (see below for more details).

## [`checkpoint_handler`](prefect_ds/checkpoint_handler.py) and [`DSTaskRunner`](prefect_ds/task_runner.py)

A state handler that implements filename-based checkpointing, in concert with the specialty 
result handlers in prefect_ds. It intercepts the state change from `Pending`
to `Running`, runs the `read` method of the result handler, and if successful loads
the result of that method as the result of the task, then sets the task to the `Success`
state. Conversely, if the `read` method fails, the task is run as normal and instead the
`checkpoint_handler` runs the `write` method of the result handler afterwards. Using the 
`checkpoint_handler` makes it much easier to cache data across Prefect runs — you don't have to
explicitly persist the final flow state between runs, and you don't have to have the cache expire
after a certain amount of time. 

This handler combines with `DSTaskRunner`, an extension to Prefect's `TaskRunner` that implements
the necessary hacks to allow for the templating of task arguments. This templating is required
to handle cases like `map`, where without the templating the `checkpoint_handler` will read from/write
to the same file for every iteration of the `map`. 

```python
>>> import contextlib

>>> from prefect import Flow
>>> from prefect.engine import FlowRunner
>>> from prefect_ds.checkpoint_handler import checkpoint_handler
>>> from prefect_ds.task_runner import DSTaskRunner

>>> with Flow("test") as flow:
...     output = demo_task(1)

>>> # First, clean up any existing task results
>>> with contextlib.suppress(FileNotFoundError):
...     os.remove("data_1.csv")

>>> # Run the flow. Since the result file doesn't exist, will run the task
>>> start = time.time()
>>> state = FlowRunner(flow=flow, task_runner_cls=DSTaskRunner).run(
...     task_runner_state_handlers=[checkpoint_handler]
... )
>>> print(f"Took more than 5 seconds: {(time.time() - start) > 5}")
Took more than 5 seconds: True

>>> # Run the flow again. Now that the result file exists, the task is short-circuited
>>> start = time.time()
>>> state = FlowRunner(flow=flow, task_runner_cls=DSTaskRunner).run(
...     task_runner_state_handlers=[checkpoint_handler]
... )
>>> print(f"Took less than 1 second: {(time.time() - start) < 1}")
Took less than 1 second: True

```

## [`DSFlowRunner`](prefect_ds/flow_runner.py)

An extension to Prefect's `FlowRunner`, which will automatically purge the results of upstream
tasks once all of their downstream tasks have been run. This can be useful if your task outputs are
large datasets; by default Prefect stores the results of every task for the duration of the
flow, which can overwhelm your RAM if your results are all things like multi-GB Pandas DataFrames.
While `PandasResultHandler`, `checkpoint_handler`, and `DSTaskRunner` are all designed to be used
together, `DSFlowRunner` can have value on its own.

```python
>>> from prefect_ds.flow_runner import DSFlowRunner

>>> @task()
... def generate_data():
...     return pd.DataFrame({"one": [1, 2, 3], "two": [4, 5, 6]})

>>> @task()
... def double_data(input_data):
...     return input_data * 2


>>> with Flow("test") as flow:
...     initial_data = generate_data()
...     two_x_data = double_data(initial_data)
...     four_x_data = double_data(two_x_data)

>>> state = DSFlowRunner(flow=flow).run(return_tasks=flow.tasks)
>>> state.result[initial_data].result # actual type is prefect_ds.result.PurgedResult

>>> state.result[two_x_data].result # another PurgedResult

>>> state.result[four_x_data].result # final result is correct, even though upstream results are gone
   one  two
0    4   16
1    8   20
2   12   24

```  

# Caveat
While these components have unit tests covering what I consider to be typical use cases, 
I have not attempted to comprehensively test every possible interaction with Prefect. 
As my understanding of Prefect is still relatively immature, I expect there 
are edge cases where the added functionality of prefect_ds breaks something in Prefect. I think
this is especially likely with Prefect Cloud, which I have not done any testing on. If you find
such a situation, please feel free to post an issue describing the problem and (ideally) including
a minimum reproducible example of the bug.