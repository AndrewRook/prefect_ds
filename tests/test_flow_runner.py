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
def create_data(offset=0):
    data = pd.DataFrame({
        "one": [1, 2, 3],
        "two": [4, 5, 6]
    })
    return data + offset

@task()
def merge_data(data_list):
    return pd.concat(data_list, ignore_index=True)

@task()
def merge_two_dataframes(dataframe_1, dataframe_2):
    return pd.concat([dataframe_1, dataframe_2], ignore_index=True)

@task()
def modify_data(input_data):
    return input_data * 2


def test_purges_upstream_tasks():
    with Flow("test") as flow:
        initial_data = create_data()
        modified_data = modify_data(initial_data)
        modified_data_2 = modify_data(modified_data)

    state = DSFlowRunner(flow=flow).run(return_tasks=flow.tasks)
    expected_result = pd.DataFrame({
        "one": [4, 8, 12],
        "two": [16, 20, 24]
    })
    pd.testing.assert_frame_equal(expected_result, state.result[modified_data_2].result)
    assert state.result[initial_data]._result is PurgedResult
    assert state.result[modified_data]._result is PurgedResult


def test_purges_properly_when_non_tasks_are_given_as_task_inputs():
    with Flow("test") as flow:
        initial_data = create_data(5)
        modified_data = modify_data(initial_data)
        modified_data_2 = modify_data(modified_data)
    state = DSFlowRunner(flow=flow).run(return_tasks=flow.tasks)
    expected_result = pd.DataFrame({
        "one": [24, 28, 32],
        "two": [36, 40, 44]
    })
    pd.testing.assert_frame_equal(expected_result, state.result[modified_data_2].result)
    assert state.result[initial_data]._result is PurgedResult
    assert state.result[modified_data]._result is PurgedResult


def test_purges_parameters():
    with Flow("test") as flow:
        offset = Parameter("offset")
        initial_data = create_data(offset)
    state = DSFlowRunner(flow=flow).run(
        parameters={"offset": 5}, return_tasks=flow.tasks
    )
    expected_result = pd.DataFrame({
        "one": [6, 7, 8],
        "two": [9, 10, 11]
    })
    pd.testing.assert_frame_equal(expected_result, state.result[initial_data].result)
    assert state.result[offset]._result is PurgedResult


def test_purges_repeated_maps():
    with Flow("test") as flow:
        offsets = create_offsets(2)
        initial_data = create_data.map(offsets)
        modified_data = modify_data.map(initial_data)
        merged_data = merge_data(modified_data)
    state = DSFlowRunner(flow=flow).run(
        return_tasks=flow.tasks
    )
    expected_result = pd.DataFrame({
        "one": [2, 4, 6, 4, 6, 8],
        "two": [8, 10, 12, 10, 12, 14]
    })
    pd.testing.assert_frame_equal(expected_result, state.result[merged_data].result)
    assert all(result is None for result in state.result[initial_data].result)
    assert all(result is None for result in state.result[modified_data].result)


def test_purges_properly_when_upstream_has_multiple_downstream():
    with Flow("test") as flow:
        initial_data = create_data()
        modified_data_1 = initial_data * 3
        modified_data_2 = modify_data(initial_data)

    state = DSFlowRunner(flow=flow).run(
        return_tasks=flow.tasks
    )
    expected_result_1 = pd.DataFrame({
        "one": [3, 6, 9],
        "two": [12, 15, 18]
    })
    expected_result_2 = pd.DataFrame({
        "one": [2, 4, 6],
        "two": [8, 10, 12]
    })

    pd.testing.assert_frame_equal(expected_result_1, state.result[modified_data_1].result)
    pd.testing.assert_frame_equal(expected_result_2, state.result[modified_data_2].result)
    assert state.result[initial_data]._result is PurgedResult
    