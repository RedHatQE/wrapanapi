"""
Methods/classes pertaining to performing actions on a template

"""
from abc import ABCMeta, abstractmethod, abstractproperty

from .entity import BaseEntity


class BaseTemplate(BaseEntity):
    __metaclass__ = ABCMeta

    @property
    def exists(self):
        """Checks if a template of this name exists

        Returns:
            True if it exists
            False if not
            False if system's list_templates() method is not implemented.
        """
        try:
            return (self.name in t.name for t in self.system.list_templates())
        except NotImplementedError:
            return False

    @abstractmethod
    def deploy(self, *args, **kwargs):
        """Deploy a VM/instance from a template

        Args:
            template: The name of the template to deploy
        Returns: a BaseVM object
        """
        raise NotImplementedError('deploy not implemented.')