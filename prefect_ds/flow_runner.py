from prefect.engine.flow_runner import FlowRunner
from prefect.engine.state import State
from prefect.core.edge import Edge
from prefect.core.task import Task
from typing import Any, Callable, Dict, Iterable, Set


class DSFlowRunner(FlowRunner):
    def get_flow_run_state(
        self,
        state: State,
        task_states: Dict[Task, State],
        task_contexts: Dict[Task, Dict[str, Any]],
        return_tasks: Set[Task],
        task_runner_state_handlers: Iterable[Callable],
        executor: "prefect.engine.executors.base.Executor",
    ) -> State:
        self.task_states = task_states
        return super().get_flow_run_state(
            state=state,
            task_states=task_states,
            task_contexts=task_contexts,
            return_tasks=return_tasks,
            task_runner_state_handlers=task_runner_state_handlers,
            executor=executor
        )

    def run_task(
        self,
        task: Task,
        state: State,
        upstream_states: Dict[Edge, State],
        context: Dict[str, Any],
        task_runner_state_handlers: Iterable[Callable],
        executor: "prefect.engine.executors.Executor",
    ) -> State:
        task_output = super().run_task(
            task=task,
            state=state,
            upstream_states=upstream_states,
            context=context,
            task_runner_state_handlers=task_runner_state_handlers,
            executor=executor
        )
        breakpoint()
        return task_output