"""
wrapanapi.entities.network

Networks
"""

from abc import ABCMeta, abstractmethod

from wrapanapi.entities.base import Entity, EntityMixin
from wrapanapi.exceptions import MultipleItemsError, NotFoundError


class Network(Entity, metaclass=ABCMeta):
    """
    Defines methods/properties pertaining to networks
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


class NetworkMixin(EntityMixin, metaclass=ABCMeta):
    """
    Defines methods for systems that support networks
    """
    @abstractmethod
    def create_network(self, **kwargs):
        """
        Creates network

        Returns: wrapanapi.entities.Network for newly created Network
        """

    @abstractmethod
    def list_networks(self, **kwargs):
        """
        Return a list of Network entities.

        Returns: list of Network objects
        """

    @abstractmethod
    def find_networks(self, name, **kwargs):
        """
        Find a network based on 'name' or other kwargs

        Returns an empty list if no matches found

        Returns: implementation of wrapanapi.network.Network
        """

    @abstractmethod
    def get_network(self, name, **kwargs):
        """
        Get a network based on name or other kwargs

        Returns: Network object
        Raises:
            MultipleItemsError if multiple matches found
            NotFoundError if unable to find network
        """

    def does_network_exist(self, name):
        """
        Checks if a network with 'name' exists on the system

        If multiple networks with the same name exists, this still returns 'True'
        """
        try:
            return bool(self.get_network(name))
        except MultipleItemsError:
            return True
        except NotFoundError:
            return False
