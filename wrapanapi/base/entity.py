"""
Provides method/class definitions for handling any named entity on a provider

"""
from abc import ABCMeta, abstractmethod, abstractproperty

class BaseEntity(LogMixin):
    """Represents any named object on a provider system.

    Provides properties/methods that should be applicable
    across all entities on all systems.
    """
    __metaclass__ = ABCMeta

    @property
    def logger(self):
        return logging.getLogger(self.__class__.__module__ + "." + self.__class__.__name__)

    def __init__(self, name, system, *args, **kwargs):
        """Constructor for BaseEntity

        Args:
          name (str) -- name of the entity on the system
          system (BaseSystem) -- an instance of BaseSystem
        """
        self.name = name
        self.system = system

    @abstractproperty
    def uuid(self):
        """Returns the uuid of the entity on the provider"""
        raise NotImplementedError

    @abstractproperty
    def exists(self):
        """Checks if the entity of this name/type exists on the provider"""
        raise NotImplementedError

    def rename(self, new_name):
        """Rename the entity on the provider

        Args:
            new_name: new name for the entity
        Returns: Updated entity object
        """
        raise NotImplementedError('rename not implemented')

    @property
    def can_rename(self):
        return hasattr(self, "rename")

    @abstractmethod
    def delete(self):
        """Removes the entity on the provider"""
        raise NotImplementedError