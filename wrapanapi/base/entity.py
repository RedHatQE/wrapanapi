"""
Provides method/class definitions for handling any named entity on a provider

"""
from abc import ABCMeta, abstractmethod, abstractproperty

from .util import LoggerMixin

class Entity(LoggerMixin):
    """Represents any named object on a provider system.

    Provides properties/methods that should be applicable
    across all entities on all systems.
    """
    __metaclass__ = ABCMeta

    @classmethod
    def get_all_subclasses(cls):
        """
        Return all subclasses that inherit from this class
        """
        for subclass in cls.__subclasses__():
            for c in subclass.get_all_subclasses():
                yield c
            yield subclass

    @classmethod
    @abstractproperty
    def system_cls(cls):
        """
        Returns the class of the system this Entity is associated with

        For example, an Entity implemented for azure should return AzureSystem
        """

    @classmethod
    @abstractmethod
    def list_all(cls, system):
        """
        Returns list of all entities of this type on 'system'

        Args:
            system -- implementation of BaseSystem
        Returns:
            list of entities
        """

    @classmethod
    @abstractmethod
    def factory(cls, system, *args, **kwargs):
        """
        Return a new entity instance of this type for 'system'.

        Does not check to see if entity exists on the provider. Simply builds
        an instance of the class with the given args. You must use 'entity.exists'
        property to verify it exists on the provider backend.

        Args:
            system -- implementation of BaseSystem
        Returns:
            entity instance
        """

    @classmethod
    @abstractmethod
    def get(cls, system, *args, **kwargs):
        """
        Return entity of this type on 'system' if it exists.

        Verifies the entity exists on the provider system.

        Args:
            system -- implementation of BaseSystem
        Returns:
            entity instance
        Raises:
            wrapanapi.exceptions.NotFoundError if entity does not exist
        """

    def __init__(self, system, *args, **kwargs):
        """
        Constructor for BaseEntity

        Implementors will likely define more args

        Args:
            system (BaseSystem) -- instance of BaseSystem entity is tied to
        """
        self.system = system

    @abstractproperty
    def exists(self):
        """Checks if this entity exists on the provider"""

    @abstractmethod
    def create(self, *args, **kwargs):
        """Creates the entity on the provider system.

        Returns: True if successful, False if not
        """

    def rename(self, new_name):
        """Rename the entity on the provider

        Args:
            new_name: new name for the entity
        Returns: Updated entity object
        """
        raise NotImplementedError('rename not implemented')

    @abstractmethod
    def delete(self):
        """Removes the entity on the provider"""

    def cleanup(self):
        """
        Removes the entity on the provider and any of its associated resources

        Can be overriden by implementors to do more than just a delete
        """
        return self.delete()
