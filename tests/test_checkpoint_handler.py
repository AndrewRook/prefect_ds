import pandas as pd
import pytest

from prefect.core.edge import Edge
from prefect.core.task import Task
from prefect.engine.state import State, Failed, Pending, Running, Success
from prefect.engine.task_runner import TaskRunner
from prefect.engine.result_handlers.local_result_handler import LocalResultHandler

import prefect_ds.checkpoint_handler as dsh

from prefect_ds.task_runner import DSTaskRunner
from prefect_ds.pandas_result_handler import PandasResultHandler


class TestCheckPointHandler:
    def test_errors_when_regular_runner_is_used(self):
        task = Task(name="Task", result_handler=PandasResultHandler("dummy.csv"))
        task_runner = TaskRunner(task)
        old_state = Pending()
        new_state = Running()

        with pytest.raises(TypeError):
            dsh.checkpoint_handler(task_runner, old_state, new_state)

    def test_does_not_look_for_file_when_no_result_handler_given(self):
        task = Task(name="Task")
        task_runner = DSTaskRunner(task) # Does not get upstream_states by
        # default, so normally would throw an error if a result handler was passed
        old_state = Pending()
        new_state = Running()
        dsh.checkpoint_handler(task_runner, old_state, new_state)

    def test_raises_appropriate_error_when_incompatible_handler_given(self):
        task = Task(name="Task", result_handler=LocalResultHandler())
        task_runner = DSTaskRunner(task)
        task_runner.upstream_states = {}
        old_state = Pending()
        new_state = Running()
        with pytest.raises(TypeError):
            dsh.checkpoint_handler(task_runner, old_state, new_state)

    def test_errors_if_regular_checkpointing_is_set_to_be_used(self, monkeypatch):
        monkeypatch.setenv("PREFECT__FLOWS__CHECKPOINTING", "true")

        task = Task(name="Task", result_handler=PandasResultHandler("dummy.csv"))
        task_runner = DSTaskRunner(task)
        old_state = Pending()

        with pytest.raises(AttributeError):
            dsh.checkpoint_handler(task_runner, old_state, old_state)

    def test_writes_checkpointed_file_to_disk_on_success(self, tmp_path):
        result_handler = PandasResultHandler(
            tmp_path / "dummy.csv",
            write_kwargs={"index":False}
        )
        task = Task(name="Task", result_handler=result_handler)
        expected_result = pd.DataFrame({"one": [1, 2, 3], "two": [4, 5, 6]})
        task_runner = DSTaskRunner(task)
        task_runner.upstream_states = {}
        old_state = Running()
        new_state = Success(result=expected_result)

        dsh.checkpoint_handler(task_runner, old_state, new_state)

        actual_result = pd.read_csv(tmp_path / "dummy.csv")
        pd.testing.assert_frame_equal(expected_result, actual_result)

    def test_moves_on_gracefully_if_checkpointed_file_does_not_exist_yet(self, tmp_path):
        result_handler = PandasResultHandler(tmp_path / "dummy.csv")
        task = Task(name="Task", result_handler=result_handler)
        task_runner = TaskRunner(task)
        task_runner.upstream_states = {}
        old_state = Pending()
        new_state = Running()

        new_state = dsh.checkpoint_handler(task_runner, old_state, new_state)

        assert new_state.is_running()

    def test_reads_checkpointed_file_from_disk_if_exists(self, tmp_path):
        result_handler = PandasResultHandler(tmp_path / "dummy.csv")
        task = Task(name="Task", result_handler=result_handler)
        expected_result = pd.DataFrame({"one": [1, 2, 3], "two": [4, 5, 6]})
        expected_result.to_csv(tmp_path / "dummy.csv", index=False)
        task_runner = TaskRunner(task)
        task_runner.upstream_states = {}
        old_state = Pending()
        new_state = Running()

        new_state = dsh.checkpoint_handler(task_runner, old_state, new_state)

        assert new_state.is_successful()
        pd.testing.assert_frame_equal(expected_result, new_state.result)




    def test_does_not_write_checkpoint_file_to_disk_on_failure(self, tmp_path):
        result_handler = PandasResultHandler(
            tmp_path / "dummy.csv",
            write_kwargs={"index":False}
        )
        task = Task(name="Task", result_handler=result_handler)
        result = pd.DataFrame({"one": [1, 2, 3], "two": [4, 5, 6]})
        task_runner = DSTaskRunner(task)
        task_runner.upstream_states = {}
        old_state = Running()
        new_state = Failed(result=result)

        dsh.checkpoint_handler(task_runner, old_state, new_state)

        with pytest.raises(IOError):
            pd.read_csv(tmp_path / "dummy.csv")


class TestCreateInputMapping:
    def test_returns_empty_dict_when_no_upstream_states_given(self):
        mapping = dsh._create_input_mapping({})
        assert mapping == {}

    def test_works_with_multiple_upstream_states(self):
        upstream_task_1 = Task(name="upstream_task_one")
        upstream_state_1 = State(result=1)
        upstream_task_2 = Task(name="upstream_task_two")
        upstream_state_2 = State(result=2)
        downstream_task = Task(name="downstream_task")
        upstream_states = {
            Edge(upstream_task_1, downstream_task, key="var_1"): upstream_state_1,
            Edge(upstream_task_2, downstream_task, key="var_2"): upstream_state_2
        }
        mapping = dsh._create_input_mapping(upstream_states)
        assert mapping == {"var_1": 1, "var_2": 2}
