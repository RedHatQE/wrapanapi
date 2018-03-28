# coding: utf-8
"""Backend management system classes

Used to communicate with providers without using CFME facilities
"""
from __future__ import absolute_import
from abc import ABCMeta, abstractmethod, abstractproperty

from .util import LoggerMixin
from .entity import Entity


class BaseSystem(LoggerMixin):
    """Represents any system that wrapanapi interacts with."""
    @classmethod
    def supported_entities(cls):
        """
        Return all entity types implemented for this system.
        """
        supported_entities = []
        for subclass in Entity.get_all_subclasses():
            if subclass.system_cls == cls:
                supported_entities.append(subclass)

    @classmethod
    def _get_subclass_for_type(cls, entity_cls):
        if not isinstance(entity_cls, Entity):
            raise ValueError("entity_cls must be derived from wrapanapi.base.entity.Entity")

        for subclass in entity_cls.get_all_subclasses():
            if subclass.system_type == cls:
                return subclass
        raise ValueError(
            "Unable to find any {} defined for system {}".format(entity_cls, cls)
        )

    def entity(self, entity_cls, *args, **kwargs):
        """
        Return a new entity instance of class 'entity_cls' for this system.

        Does not check to see if entity exists on the provider. Simply builds
        an instance of the class. You must use 'entity.exists' property to verify
        it exists on the provider backend.

        args and kwargs are passed along to the entity's factory() method
        """
        subclass = self._get_subclass_for_type(entity_cls)
        return subclass.factory(system=self, *args, **kwargs)

    @abstractmethod
    def list_entities(self, entity_cls):
        """
        Return a list of instances of type 'entity_cls' for this system.

        args and kwargs are passed along to the entity's __init__ method
        """
        subclass = self._get_subclass_for_type(entity_cls)
        subclass.list_all_on_system(system=self)

    @abstractmethod
    def find_entity(self, entity_cls, *args, **kwargs):
        """
        Return an entity instance of type 'entity_type' which exists on system.

        Verifies the entity exists on the system before returning.

        args and kwargs are passed along to the entity's __init__ method
        """
        subclass = self._get_subclass_for_type(entity_cls)
        return subclass.find_on_system(system=self, *args, **kwargs)

    def stats(self, *requested_stats):
        """Returns all available stats, if none are explicitly requested

        Args:
            *requested_stats: A list giving the name of the stats to return. Stats are defined
                in the _stats_available attibute of the specific class.
        Returns: A dict of stats.
        """
        if not hasattr(self, '_stats_available'):
            raise Exception('{} is missing self._stats_available dictionary'.format(
                self.__class__.__name__))

        requested_stats = requested_stats or self._stats_available
        return {stat: self._stats_available[stat](self) for stat in requested_stats}

    def disconnect(self):
        """Disconnects the API from mgmt system"""
        pass


class BaseVMSystem(BaseSystem):
    """
    Base interface class for Management Systems that manage VMs/instances

    Interface notes:

    * Initializers of subclasses must support \*\*kwargs in their
      signtures

    """
    __metaclass__ = ABCMeta

    @classmethod
    @abstractproperty
    def can_suspend(cls):
        """Indicates whether this system can suspend VM's/instances."""
        return False

    @classmethod
    @abstractproperty
    def can_pause(cls):
        """Indicates whether this system can pause VM's/instances."""
        return False

    @classmethod
    @abstractproperty
    def steady_wait_time(cls):
        """Returns default secs to wait for VM to move into a steady state."""
        return 120

    def list_flavors(self):
        """Returns a list of flavors.

        Only valid for some systems.

        Returns: list of flavor names
        """
        raise NotImplementedError('list_flavors not implemented.')

    def list_networks(self):
        """Returns a list of networks.

        Only valid for some systems.

        Returns: list of network names
        """
        raise NotImplementedError('list_networks not implemented.')

    @abstractmethod
    def info(self):
        """Returns basic information about the mgmt system.

        Returns: string representation of name/version of mgmt system.
        """
        raise NotImplementedError('info not implemented.')

    def remove_host_from_cluster(self, hostname):
        """Remove a host from it's cluster

        Args:
            hostname (str): The hostname of the system
        Returns:
            True if successful, False if failed
        """
        raise NotImplementedError('remove_host_from_cluster not implemented.')

    def usage_and_quota(self):
        raise NotImplementedError(
            'Provider {} does not implement usage_and_quota'.format(type(self).__name__))