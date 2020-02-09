from prefect.engine.flow_runner import FlowRunner
from prefect.engine.state import State
from prefect.core.edge import Edge
from prefect.core.task import Task
from typing import Any, Callable, Dict, Iterable, Set

from prefect_ds.result import PurgedResult


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
        self._purge_unnecessary_tasks(task, upstream_states)
        return task_output

    def _purge_unnecessary_tasks(self, task, upstream_state_edges):
        for state_edge in upstream_state_edges:
            upstream_task = state_edge.upstream_task
            edges_from_upstream_task = self.flow.edges_from(upstream_task)
            is_safely_purgeable = True
            for edge in edges_from_upstream_task:
                if edge.downstream_task == task:
                    # Downstream task is the current task
                    continue
                if (
                        edge.downstream_task in self.task_states and
                        self.task_states[edge.downstream_task].is_successful() == True
                ):
                    # Downstream edge has been successfully completed
                    continue
                # If we're still in this iteration of the loop, it means that this
                # downstream task hasn't successfully completed yet
                is_safely_purgeable = False
                break
            if is_safely_purgeable:
                self.task_states[upstream_task].result = PurgedResult
