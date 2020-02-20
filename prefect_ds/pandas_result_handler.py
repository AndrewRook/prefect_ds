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
    """
    Hook for storing and retrieving task results in Pandas DataFrames.
    Task results are written/read via the standard Pandas
    ``to_[FILETYPE]``/``read_[FILETYPE]`` methods, and additionally the filename
    can be fully specified at task instantiation.

    Parameters
    ----------
    path : str or pathlib.Path
        Filepath to be read from or written to, including file name and extension.
    file_type : str
        The type of file to write to, e.g. "csv" or "parquet". Must match the name
        of the appropriate ``to_[FILETYPE]``/``read_[FILETYPE]`` method.
    read_kwargs : dict or None
        If present, passed as **kwargs to the ``read_[FILETYPE]`` method.
    write_kwargs : dict or None
        If present, passed as **kwargs tot he ``to_[FILETYPE]`` method.

    .. note::
        Because the filepath is fully specified, when using this handler in a ``map``
        can lead to the same file being read to or written from by every iteration of
        the map. To deal with this case this handler allows for Python's ``str.format``
        to be used with the names of arguments to the task instance. For instance, if
        your task has an argument named ``sample_name`` that you intend to map over,
        supplying a ``file_type`` like ``"output_{sample_name}.csv"`` will fill in the
        values of that argument for each iteration of the map.
    """
    _READ_OPS_MAPPING, _WRITE_OPS_MAPPING = _generate_pandas_io_methods()
    assert set(_READ_OPS_MAPPING.keys()) == set(_WRITE_OPS_MAPPING.keys())

    def __init__(
            self,
            path: typing.Union[str, pathlib.Path],
            file_type: str,
            read_kwargs: dict = None,
            write_kwargs: dict = None
    ):
        self.path = pathlib.Path(path)
        self.file_type = file_type

        if self.file_type.lower() not in self._READ_OPS_MAPPING:
            raise ValueError(
                f"{self.file_type} not available. "
                f"Known file extensions are {list(self._READ_OPS_MAPPING.keys())}"
            )
        self.read_kwargs = read_kwargs if read_kwargs is not None else {}
        self.write_kwargs = write_kwargs if write_kwargs is not None else {}
        super().__init__()

    def read(self, *, input_mapping=None) -> pd.DataFrame:
        """
        Read a result from the specified ``path`` using the appropriate ``read_[FILETYPE]`` method.

        Parameters
        ----------
        input_mapping : dict
            If present, passed to ``path.format()`` to set the final filename. This is necessary
            for mapped tasks, to prevent the same file from being read from for each map sub-step.
        """

        input_mapping = {} if input_mapping is None else input_mapping
        path_string = str(self.path).format(**input_mapping)
        self.logger.debug("Starting to read result from {}...".format(path_string))
        data = self._READ_OPS_MAPPING[self.file_type.lower()](
            path_string,
            **self.read_kwargs
        )
        self.logger.debug("Finished reading result from {}...".format(path_string))
        return data

    def write(self, result: pd.DataFrame, input_mapping=None):
        input_mapping = {} if input_mapping is None else input_mapping
        path_string = str(self.path).format(**input_mapping)
        self.logger.debug("Starting to write result to {}...".format(path_string))
        write_function = getattr(result, self._WRITE_OPS_MAPPING[self.file_type.lower()])
        write_function(path_string, **self.write_kwargs)
        self.logger.debug("Finished writing result to {}...".format(path_string))
