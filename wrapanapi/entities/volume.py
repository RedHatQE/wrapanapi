"""
wrapanapi.entities.volume

Volumes
"""
from __future__ import absolute_import
import six

from abc import ABCMeta, abstractmethod

from wrapanapi.entities.base import Entity, EntityMixin
from wrapanapi.exceptions import MultipleItemsError, NotFoundError


class Volume(six.with_metaclass(ABCMeta, Entity)):
    """
    Defines methods/properties pertaining to volume
    """
    @abstractmethod
    def get_details(self):
        """
        Return a dict with detailed info about this object

        There's no specific prescription for how this dict should be formatted--
        it will vary based on entity and provider type. It is recommended
        that the values contain simple python data types instead of
        complex classes so the data can be parsed easily.

        Returns: dict
        """


class VolumeMixin(six.with_metaclass(ABCMeta, EntityMixin)):
    """
    Defines methods for systems that support volumes
    """
    @abstractmethod
    def create_volume(self, **kwargs):
        """
        Creates volume

        Returns: wrapanapi.entities.Volume for newly created Volume
        """

    @abstractmethod
    def list_volumes(self, **kwargs):
        """
        Return a list of Volume entities.

        Returns: list of Volume objects
        """

    @abstractmethod
    def find_volumes(self, name, **kwargs):
        """
        Find a volume based on 'name' or other kwargs

        Returns an empty list if no matches found

        Returns: implementation of wrapanapi.network.Volume
        """

    @abstractmethod
    def get_volume(self, name, **kwargs):
        """
        Get a volume based on name or other kwargs

        Returns: Volume object
        Raises:
            MultipleItemsError if multiple matches found
            NotFoundError if unable to find volume
        """

    def does_volume_exist(self, name):
        """
        Checks if a volume with 'name' exists on the system

        If multiple volume with the same name exists, this still returns 'True'
        """
        try:
            return bool(self.get_volume(name))
        except MultipleItemsError:
            return True
        except NotFoundError:
            return False
