from abc import ABCMeta, abstractmethod

from .entity import EntityMixin

class FlavorMixin(EntityMixin):
    """
    Defines methods for systems that support flavors
    """
    __metaclass__ = ABCMeta

    @abstractmethod
    def list_flavors(self):
        """Returns a list of flavors.

        Only valid for some systems.

        Returns: list of flavor names
        """
