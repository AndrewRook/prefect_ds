import pandas as pd
import pathlib
import pytest
from prefect import Flow, Parameter, task

from prefect_ds.flow_runner import DSFlowRunner
from prefect_ds.result import PurgedResult

THIS_DIR = pathlib.Path(__file__).parent.absolute()

@task()
def create_offsets(num_offsets):
    return list(range(num_offsets))

@task()
def create_data(offset):
    data = pd.DataFrame({
        "one": [1, 2, 3],
        "two": [4, 5, 6]
    })
    return data

@task()
def merge_data(data_list):
    return pd.concat(data_list, ignore_index=True)

@task()
def merge_two_dataframes(dataframe_1, dataframe_2):
    return pd.concat([dataframe_1, dataframe_2], ignore_index=True)

@task()
def modify_data(input_data):
    return input_data * 2


@pytest.fixture(scope="module")
def test_flow():
    with Flow("test") as flow:
        num_offsets = Parameter("num_offsets")
        offsets = create_offsets(num_offsets)
        initial_data_list = create_data.map(offsets)
        initial_data_list_2 = create_data.map(offsets)
        merged_data = merge_data(initial_data_list)
        merged_data_2 = merge_data(initial_data_list_2)
        modified_merged_data = modify_data(merged_data)
        fully_combined_data = merge_two_dataframes(merged_data, merged_data_2)
        combined_modified_data = merge_two_dataframes(merged_data, modified_merged_data)

    tasks = {
        "num_offsets": num_offsets,
        "offsets": offsets,
        "initial_data_list": initial_data_list,
        "initial_data_list_2": initial_data_list_2,
        "merged_data": merged_data,
        "merged_data_2": merged_data_2,
        "modified_merged_data": modified_merged_data,
        "fully_combined_data": fully_combined_data,
        "combined_modified_data": combined_modified_data
    }

    return DSFlowRunner(flow=flow), tasks


def test_runs_at_all(test_flow):
    flow, tasks = test_flow
    state = flow.run(parameters={"num_offsets": 2}, return_tasks=flow.flow.tasks)
    print(state.result)


def test_gets_expected_answers(test_flow):
    flow, tasks = test_flow
    state = flow.run(
        parameters={"num_offsets": 2},
        return_tasks=[tasks["fully_combined_data"], tasks["combined_modified_data"]]
    )

    expected_fully_combined_data = pd.DataFrame({
        "one": [1, 2, 3, 1, 2, 3, 1, 2, 3, 1, 2, 3],
        "two": [4, 5, 6, 4, 5, 6, 4, 5, 6, 4, 5, 6]
    })
    pd.testing.assert_frame_equal(
        expected_fully_combined_data,
        state.result[tasks["fully_combined_data"]].result
    )

    expected_combined_modified_data = pd.DataFrame({
        "one": [1, 2, 3, 1, 2, 3, 2, 4, 6, 2, 4, 6],
        "two": [4, 5, 6, 4, 5, 6, 8, 10, 12, 8, 10, 12]
    })
    pd.testing.assert_frame_equal(
        expected_combined_modified_data,
        state.result[tasks["combined_modified_data"]].result
    )


def test_purges_upstream_tasks(test_flow):
    flow, tasks = test_flow
    state = flow.run(parameters={"num_offsets": 2}, return_tasks=flow.flow.tasks)
    for task_name, task_object in tasks.items():
        if task_name not in ("fully_combined_data", "combined_modified_data"):
            #breakpoint()
            assert state.result[task_object]._result is PurgedResult
