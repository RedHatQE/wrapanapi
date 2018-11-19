# coding: utf-8
"""Backend management system classes

Used to communicate with providers without using CFME facilities
"""
from __future__ import absolute_import
import six
from abc import ABCMeta, abstractmethod, abstractproperty

from wrapanapi.utils import LoggerMixin


class System(six.with_metaclass(ABCMeta, LoggerMixin)):
    """Represents any system that wrapanapi interacts with."""
    # This should be defined by implementors of System
    _stats_available = {}

    def __init__(self, *args, **kwargs):
        """
        Constructor for base System.

        System constructor is not doing much right now, but since System child classes
        may call __super__ in their __init__, we'll store any args that have bubbled up
        through these super calls
        """
        self._base_system_args = args
        self._base_system_kwargs = kwargs

    @abstractproperty
    def _identifying_attrs(self):
        """
        Return a dict with key, value pairs for each kwarg that is used to
        uniquely identify this system.

        Used for __eq__() to assert that two System instances are communicating
        to the same underlying system.
        """

    def __eq__(self, other):
        """
        Compares two Systems to check if they are equal.

        Uses kwargs/attributes of the system that prove that 'self'
        and 'other' are both "communicating with the same thing" and that
        a unique entity obtained using 'self' would be the same as that entity
        obtained using 'other'
        """
        if not isinstance(other, self.__class__):
            return False
        try:
            return self._identifying_attrs == other._identifying_attrs
        except AttributeError:
            return False

    @abstractmethod
    def info(self):
        """Returns basic information about the mgmt system.

        Returns: string representation of name/version of mgmt system.
        """

    def stats(self, *requested_stats):
        """Returns all available stats, if none are explicitly requested

        Args:
            *requested_stats: A list giving the name of the stats to return. Stats are defined
                in the _stats_available attibute of the specific class.
        Returns: A dict of stats.
        """
        if not self._stats_available:
            raise Exception('{} has empty self._stats_available dictionary'.format(
                self.__class__.__name__))

        requested_stats = requested_stats or self._stats_available.keys()
        return {stat: self._stats_available[stat](self) for stat in requested_stats}

    def disconnect(self):
        """Disconnects the API from mgmt system"""
        pass

    def usage_and_quota(self):
        raise NotImplementedError(
            'Provider {} does not implement usage_and_quota'.format(type(self).__name__))
