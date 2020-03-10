"""
wrapanapi.entities.security_group

SecurityGroups
"""

from abc import ABCMeta, abstractmethod

from wrapanapi.entities.base import Entity, EntityMixin
from wrapanapi.exceptions import MultipleItemsError, NotFoundError


class SecurityGroup(Entity, metaclass=ABCMeta):
    """
    Defines methods/properties pertaining to security groups
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


class SecurityGroupMixin(EntityMixin, metaclass=ABCMeta):
    """
    Defines methods for systems that support security groups
    """
    @abstractmethod
    def create_sec_group(self, **kwargs):
        """
        Creates security group

        Returns: wrapanapi.entities.SecurityGroup for newly created SecurityGroup
        """

    @abstractmethod
    def list_sec_groups(self, **kwargs):
        """
        Return a list of SecurityGroup entities.

        Returns: list of SecurityGroup objects
        """

    @abstractmethod
    def find_sec_groups(self, name, **kwargs):
        """
        Find a security group based on 'name' or other kwargs

        Returns an empty list if no matches found

        Returns: implementation of wrapanapi.network.SecurityGroup
        """

    @abstractmethod
    def get_sec_group(self, name, **kwargs):
        """
        Get a security group based on name or other kwargs

        Returns: SecurityGroup object
        Raises:
            MultipleItemsError if multiple matches found
            NotFoundError if unable to find security group
        """

    def does_sec_group_exist(self, name):
        """
        Checks if a security group with 'name' exists on the system

        If multiple security groups with the same name exists, this still returns 'True'
        """
        try:
            return bool(self.get_sec_group(name))
        except MultipleItemsError:
            return True
        except NotFoundError:
            return False
