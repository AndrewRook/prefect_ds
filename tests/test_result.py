"""Modified from https://github.com/PrefectHQ/prefect/blob/master/tests/engine/test_result.py"""

import cloudpickle
import pytest

from prefect.engine.result import Result, SafeResult

from prefect_ds.result import PurgedResult, PurgedResultType
from prefect.engine.result_handlers import ResultHandler


class TestInitialization:
    def test_purgedresult_is_already_init(self):
        n = PurgedResult
        assert isinstance(n, PurgedResultType)
        with pytest.raises(TypeError):
            n()


def test_purgedresult_is_safe():
    assert isinstance(PurgedResult, SafeResult)


def test_basic_purgedresult_repr():
    assert repr(PurgedResult) == "<Purged result>"


def test_basic_purgedresult_str():
    assert str(PurgedResult) == "PurgedResult"


def test_purgedresult_has_base_handler():
    n = PurgedResult
    n.result_handler == ResultHandler()


def test_purgedresult_returns_itself_for_safe_value():
    n = PurgedResult
    assert n is n.safe_value


def test_purgedresult_returns_none_for_value():
    n = PurgedResult
    assert n.value is None


def test_purged_results_are_all_the_same():
    n = PurgedResult
    q = PurgedResultType()
    assert n == q
    q.new_attr = 99
    assert n == q


def test_purged_results_are_not_the_same_as_result():
    n = PurgedResult
    r = Result(None)
    assert n != r


class TestStoreSafeValue:
    def test_store_safe_value_for_purged_results(self):
        output = PurgedResult.store_safe_value()
        assert output is None


class TestToResult:
    def test_to_result_returns_self_for_purged_results(self):
        assert PurgedResult.to_result() is PurgedResult


@pytest.mark.parametrize(
    "obj",
    [
        PurgedResult
    ],
)
def test_everything_is_pickleable_after_init(obj):
    assert cloudpickle.loads(cloudpickle.dumps(obj)) == obj
