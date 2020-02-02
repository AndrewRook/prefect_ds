import pandas as pd

from prefect import Flow, task
from prefect.engine.flow_runner import FlowRunner


from prefect_ds.checkpoint_handler import checkpoint_handler
from prefect_ds.pandas_result_handler import PandasResultHandler
from prefect_ds.task_runner import DSTaskRunner


def test_task_runner_works_in_normal_usage(tmp_path):
    @task()
    def generate_list():
        return [0, 2]

    @task(result_handler=PandasResultHandler(tmp_path / "test_{offset}.csv", write_kwargs={"index": False}))
    def generate_data(offset):
        return pd.DataFrame({"one": [1, 2, 3], "two": [4, 5, 6]}) + offset

    @task(result_handler=PandasResultHandler(tmp_path / "test_broken.csv", write_kwargs={"index": False}))
    def broken_task():
        assert False

    with Flow("test") as flow:
        offsets = generate_list()
        data = generate_data.map(offsets)
        only_works_from_cache = broken_task()

    cached_broken_data = pd.DataFrame({"one": [0, 0, 0], "two": [1, 1, 1]})
    cached_broken_data.to_csv(tmp_path / "test_broken.csv", index=False)

    flow_state = FlowRunner(flow=flow, task_runner_cls=DSTaskRunner).run(
        task_runner_state_handlers=[checkpoint_handler],
        return_tasks=[data, only_works_from_cache]
    )

    mapped_data_from_cache = [
        pd.read_csv(tmp_path / "test_{offset}.csv".format(offset=offset))
        for offset in [0, 2]
    ]
    assert len(mapped_data_from_cache) == len(flow_state.result[data].result)
    for cached_data, flow_data in zip(mapped_data_from_cache, flow_state.result[data].result):
        pd.testing.assert_frame_equal(cached_data, flow_data)

    pd.testing.assert_frame_equal(cached_broken_data, flow_state.result[only_works_from_cache].result)