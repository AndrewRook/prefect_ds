from prefect.core.edge import Edge
from prefect.engine.state import State
from prefect.core.task import Task

import prefect_ds.checkpoint_handler as dsh


class TestDiskStateHandler:
    pass

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
