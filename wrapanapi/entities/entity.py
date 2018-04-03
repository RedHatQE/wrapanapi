"""
Provides method/class definitions for handling any named entity on a provider

"""
from abc import ABCMeta, abstractmethod, abstractproperty

from wrapanapi.utils import LoggerMixin


class Entity(LoggerMixin):
    """
    Base class to represent any object on a provider system as well
    as methods for manipulating that entity (deleting, renaming, etc.)

    Provides properties/methods that should be applicable
    across all entities on all systems.
    """
    __metaclass__ = ABCMeta

    def __init__(self, system, *args, **kwargs):
        """
        Constructor for an entity

        An entity is always tied to a system, other args pertaining to an Entity
        may be defined by implementations of Entity
        """
        self.system = system

    @classmethod
    def get_all_subclasses(cls):
        """
        Return all subclasses that inherit from this class
        """
        for subclass in cls.__subclasses__():
            for c in subclass.get_all_subclasses():
                yield c
            yield subclass

    @abstractproperty
    def exists(self):
        """
        Checks if this entity exists on the system
        """

    @abstractmethod
    def refresh(self):
        """
        Re-pull the data for this entity and update this instance's attributes
        """

    @abstractmethod
    def delete(self):
        """
        Removes the entity on the provider
        """

    @abstractmethod
    def cleanup(self):
        """
        Removes the entity on the provider and any of its associated resources

        This should be more than a simple delete, though if that takes care of
        the job and cleans up everything, simply calling "self.delete()" works
        """


class EntityMixin(object):
    """
    Usually an Entity also provides a mixin which defines methods/properties that should
    be defined by a wrapanapi.systems.System that manages that type of entity

    For example, for a 'Vm' entity, example abstract methods would be:
    get_vm, list_vm, find_vm, create_vm

    These methods should return instances (or a list of instances) which describe the entity

    However, methods for operating on a retrieved entity should be defined in the Entity class

    """
    # There may be some common methods/properties that apply at the base level in future...
    pass
