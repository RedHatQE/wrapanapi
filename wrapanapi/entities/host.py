from abc import ABCMeta, abstractmethod

from .entity import EntityMixin

class HostsMixin(EntityMixin):
    """
    Defines methods for systems that handle hosts
    """
    __metaclass__ = ABCMeta

    @abstractmethod
    def remove_host_from_cluster(self, hostname):
        """Remove a host from it's cluster

        Args:
            hostname (str): The hostname of the system
        Returns:
            True if successful, False if failed
        """
