import pandas as pd
import pathlib
import typing

from prefect.engine.result_handlers.result_handler import ResultHandler


def _generate_pandas_io_methods() -> typing.Tuple[typing.Dict[str, typing.Callable], typing.Dict[str, str]]:
    all_read_ops = [method for method in dir(pd) if method.lower().startswith("read_")]
    all_write_ops = [method for method in dir(pd.DataFrame()) if method.lower().startswith("to_")]
    read_suffixes = [method.lower().split("_")[1] for method in all_read_ops]
    write_suffixes = [method.lower().split("_")[1] for method in all_write_ops]

    read_io_ops = {
        read_method.split("_")[1].lower(): getattr(pd, read_method)
        for read_method in all_read_ops
        if read_method.split("_")[1].lower() in write_suffixes
    }
    write_io_ops = {
        write_method.split("_")[1].lower(): write_method
        for write_method in all_write_ops
        if write_method.split("_")[1].lower() in read_suffixes
    }

    return read_io_ops, write_io_ops


class PandasResultHandler(ResultHandler):
    
    _READ_OPS_MAPPING, _WRITE_OPS_MAPPING = _generate_pandas_io_methods()

    def __init__(
            self,
            path: typing.Union[str, pathlib.Path],
            read_kwargs: dict = None,
            write_kwargs: dict = None
    ):
        self.path = pathlib.Path(path)

        self.extension = self.path.suffix.replace(".", "").lower()
        assert self.extension in self._READ_OPS_MAPPING and self.extension in self._WRITE_OPS_MAPPING
        self.read_kwargs = read_kwargs if read_kwargs is not None else {}
        self.write_kwargs = write_kwargs if write_kwargs is not None else {}
        super().__init__()

    def read(self, *, input_mapping=None) -> pd.DataFrame:
        input_mapping = {} if input_mapping is None else input_mapping
        path_string = str(self.path).format(**input_mapping)
        self.logger.debug("Starting to read result from {}...".format(path_string))
        data = self._READ_OPS_MAPPING[self.extension](
            path_string,
            **self.read_kwargs
        )
        self.logger.debug("Finished reading result from {}...".format(path_string))
        return data

    def write(self, result: pd.DataFrame, input_mapping=None):
        input_mapping = {} if input_mapping is None else input_mapping
        path_string = str(self.path).format(**input_mapping)
        self.logger.debug("Starting to write result to {}...".format(path_string))
        write_function = getattr(result, self._WRITE_OPS_MAPPING[self.extension])
        write_function(path_string, **self.write_kwargs)
        self.logger.debug("Finished writing result to {}...".format(path_string))
