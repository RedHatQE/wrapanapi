# coding: utf-8
"""Backend management system classes

Used to communicate with providers without using CFME facilities
"""
from __future__ import absolute_import
from abc import ABCMeta, abstractmethod, abstractproperty
from contextlib import contextmanager


class BaseSystem(object):
    """Represents any system that wrapanapi interacts with."""
    @property
    def logger(self):
        return logging.getLogger(self.__class__.__module__ + "." + self.__class__.__name__)

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
        pass


class BaseVMSystem(BaseSystem):
    """
    Base interface class for Management Systems that manage VMs/instances

    Interface notes:

    * Initializers of subclasses must support \*\*kwargs in their
      signtures

    """
    __metaclass__ = ABCMeta
    STEADY_WAIT_MINS = 3

    @classmethod
    @abstractproperty
    def can_suspend(cls):
        """Indicates whether this system can suspend VM's/instances."""
        raise NotImplementedError

    @classmethod
    @abstractproperty
    def can_pause(cls):
        """Indicates whether this system can pause VM's/instances."""
        raise NotImplementedError

    @classmethod
    @abstractproperty
    def steady_wait_time(cls):
        """Returns default secs to wait for VM to move into a steady state."""
        return 120

    @abstractmethod
    def list_vms(self, **kwargs):
        """Returns a list of VM objects

        Returns: list of BaseVM
        """
        raise NotImplementedError('list_vm not implemented.')

    @abstractmethod
    def create_vm(self, vm_name, *args, **kwargs):
        """Creates a vm.

        Args:
            vm_name: name of the vm to be created
        Returns: BaseVM if creation successful
        """
        raise NotImplementedError('create_vm not implemented.')

    @abstractmethod
    def list_templates(self):
        """Returns a list of templates/images.

        Returns: list of template/image names
        """
        raise NotImplementedError('list_template not implemented.')

    @abstractmethod
    def list_flavors(self):
        """Returns a list of flavors.

        Only valid for OpenStack and Amazon

        Returns: list of flavor names
        """
        raise NotImplementedError('list_flavor not implemented.')

    def list_networks(self):
        """Returns a list of networks.

        Only valid for OpenStack

        Returns: list of network names
        """
        raise NotImplementedError('list_network not implemented.')

    @abstractmethod
    def info(self):
        """Returns basic information about the mgmt system.

        Returns: string representation of name/version of mgmt system.
        """
        raise NotImplementedError('info not implemented.')

    @abstractmethod
    def disconnect(self):
        """Disconnects the API from mgmt system"""
        raise NotImplementedError('disconnect not implemented.')

    @abstractmethod
    def remove_host_from_cluster(self, hostname):
        """remove a host from it's cluster

        :param hostname: The hostname of the system
        :type  hostname: str
        :return: True if successful, False if failed
        :rtype: boolean

        """
        raise NotImplementedError('remove_host_from_cluster not implemented.')

    @contextmanager
    def steady_wait(self, minutes):
        """Overrides original STEADY_WAIT_MINS variable in the object.

        This is useful eg. when creating templates in RHEV as it has long Image Locked period

        Args:
            minutes: How many minutes to wait
        """
        original = None
        if "STEADY_WAIT_MINS" in self.__dict__:
            original = self.__dict__["STEADY_WAIT_MINS"]
        self.__dict__["STEADY_WAIT_MINS"] = minutes
        yield
        if original is None:
            del self.__dict__["STEADY_WAIT_MINS"]
        else:
            self.__dict__["STEADY_WAIT_MINS"] = original

    def set_meta_value(self, instance, key, value):
        raise NotImplementedError(
            'Provider {} does not implement set_meta_value'.format(type(self).__name__))

    def get_meta_value(self, instance, key):
        raise NotImplementedError(
            'Provider {} does not implement get_meta_value'.format(type(self).__name__))

    def usage_and_quota(self):
        raise NotImplementedError(
            'Provider {} does not implement usage_and_quota'.format(type(self).__name__))