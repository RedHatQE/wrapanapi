from abc import ABCMeta, abstractmethod

from .entity import Entity, EntityMixin


class Stack(Entity):
    """
    Defines methods/properties pertaining to stacks
    """
    __metaclass__ = ABCMeta

    def __init__(self, system, name, *args, **kwargs):
        super(Stack, self).__init__(system)
        self.name = name

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


class StackMixin(EntityMixin):
    """
    Defines methods for systems that support stacks
    """
    __metaclass__ = ABCMeta

    @abstractmethod
    def list_stacks(self, *args, **kwargs):
        """
        Return a list of Stack entities.

        Returns: list of Stack objects
        """

    @abstractmethod
    def find_stack(self, *args, **kwargs):
        """
        Find a stack based on given args

        Returns: Stack object
        """

    @abstractmethod
    def get_stack(self, name, *args, **kwargs):
        """
        Get stack based on name

        Returns: Stack object
        """
