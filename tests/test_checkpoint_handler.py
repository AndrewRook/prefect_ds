import pytest

from prefect.core.edge import Edge
from prefect.core.task import Task
from prefect.engine.state import State, Pending, Running
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

    @pytest.mark.xfail()
    def test_does_not_look_for_file_when_no_result_handler_given(self):
        assert False

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

    @pytest.mark.xfail()
    def test_writes_checkpointed_file_to_disk_on_success(self):
        assert False

    @pytest.mark.xfail()
    def test_does_not_write_checkpoint_file_to_disk_on_failure(self):
        assert False

    @pytest.mark.xfail()
    def test_does_not_write_checkpoint_file_to_disk_when_no_handler_given(self):
        assert False


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
