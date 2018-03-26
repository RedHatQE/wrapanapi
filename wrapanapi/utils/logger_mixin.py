"""
Contains utils used by the base classes

"""
import logging


class LoggerMixin(object):
    @property
    def logger(self):
        """
        Returns a logger for the instance

        Default logger is named "{module}.{class}"

        The logger can be overriden with the logger setter.
        """
        if not hasattr(self, "_logger"):
            self._logger = logging.getLogger(
                "{}.{}".format(
                    self.__class__.__module__,
                    self.__class__.__name__
                )
            )
        return self._logger

    @logger.setter
    def logger(self, value):
        """
        Sets logger for this class

        If value is None, a 'catch-all' lambda is used that silences
        the log output.
        """
        if value is None:
            self._logger = lambda *args, **kwargs: None
        else:
            # Basic check to make sure 'value' is some kind of logger
            # (not necessarily a logging.Logger)
            expected_attrs = [
                'info', 'warning', 'critical', 'error',
                'trace', 'debug', 'exception'
            ]
            callable_attrs_present = (
                hasattr(value, a) and callable(value.a) for a in expected_attrs
            )

            if not all(callable_attrs_present):
                raise ValueError(
                    "missing one of expected logger methods: {}"
                    .format(expected_attrs)
                )
            self._logger = value
