from typing import Any

from prefect.engine.result import SafeResult
from prefect.engine.result_handlers import ResultHandler


class PurgedResultType(SafeResult):
    """
    A `SafeResult` subclass representing a Result that has been purged.  A `PurgedResult` object
    returns itself for its `value` and its `safe_value`.
    """

    def __init__(self) -> None:
        super().__init__(value=None, result_handler=ResultHandler())

    def __eq__(self, other: Any) -> bool:
        if type(self) == type(other):
            return True
        else:
            return False

    def __repr__(self) -> str:
        return "<Purged result>"

    def __str__(self) -> str:
        return "PurgedResult"

    def to_result(self, result_handler: ResultHandler = None) -> "ResultInterface":
        """
        Performs no computation and returns self.

        Args:
            - result_handler (optional): a passthrough for interface compatibility
        """
        return self


PurgedResult = PurgedResultType()
