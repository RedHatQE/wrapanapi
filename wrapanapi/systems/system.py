# coding: utf-8
"""Backend management system classes

Used to communicate with providers without using CFME facilities
"""
from __future__ import absolute_import
from abc import ABCMeta, abstractmethod, abstractproperty

from wrapanapi.utils import LoggerMixin


class System(LoggerMixin):
    """Represents any system that wrapanapi interacts with."""
    __metaclass__ = ABCMeta

    def __init__(self, *args, **kwargs):
        self._stats_available = {}

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
