import typing

from prefect.engine.state import State, Success
from prefect.engine.result import Result
from prefect.core.edge import Edge

from prefect_ds.task_runner import DSTaskRunner


def checkpoint_handler(task_runner: DSTaskRunner, old_state: State, new_state: State) -> State:
    if task_runner.result_handler is not None and old_state.is_pending() and new_state.is_running():
        if not hasattr(task_runner, "upstream_states"):
            raise TypeError(
                "upstream_states not found in task runner. Make sure to use "
                "prefect_ds.task_runner.DSTaskRunner."
            )
        input_mapping = _create_input_mapping(task_runner.upstream_states)
        try:
            data = task_runner.task.result_handler.read(input_mapping=input_mapping)
        except FileNotFoundError:
            return new_state
        except TypeError: # unexpected argument input_mapping
            raise TypeError(
                "Result handler could not accept input_mapping argument. "
                "Please ensure that you are using a handler from prefect_ds."
            )
        result = Result(value=data, result_handler=task_runner.task.result_handler)
        state = Success(result=result, message="Task loaded from disk.")
        return state

    if task_runner.result_handler is not None and old_state.is_running() and new_state.is_successful():
        input_mapping = _create_input_mapping(task_runner.upstream_states)
        task_runner.task.result_handler.write(new_state.result, input_mapping=input_mapping)

    return new_state


def _create_input_mapping(upstream_states: typing.Dict[Edge, State]) -> typing.Dict[str, typing.Any]:
    mapping = {}
    for edge, state in upstream_states.items():
        input_variable_name = edge.key
        input_task_result = state.result
        mapping[input_variable_name] = input_task_result
    return mapping