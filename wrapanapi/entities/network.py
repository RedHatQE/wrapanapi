from abc import ABCMeta, abstractmethod

from .entity import EntityMixin

class NetworkMixin(EntityMixin):
    """
    Defines methods for systems that support networks
    """
    __metaclass__ = ABCMeta

    @abstractmethod
    def list_networks(self):
        """Returns a list of networks.

        Only valid for some systems.

        Returns: list of network names
        """
