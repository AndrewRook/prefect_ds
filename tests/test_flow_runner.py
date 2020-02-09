import pandas as pd
import pytest
from prefect import Flow, Parameter, task

from prefect_ds.flow_runner import DSFlowRunner

@task()
def create_offsets(num_offsets):
    return list(range(num_offsets))

@task()
def create_data(offset):
    data = pd.DataFrame({
        "one": [1, 2, 3],
        "two": [4, 5, 6]
    })

@task()
def merge_data(data_list):
    return pd.concat(data_list, ignore_index=True)

@task()
def merge_two_dataframes(dataframe_1, dataframe_2):
    return pd.concat([dataframe_1, dataframe_2])

@task()
def modify_data(input_data):
    return input_data * 2


@pytest.fixture(scope="function")
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

    return DSFlowRunner(flow=flow)

def test_runs_at_all(test_flow):
    #breakpoint()
    test_flow.run(parameters={"num_offsets": 2}, return_tasks=test_flow.flow.tasks)