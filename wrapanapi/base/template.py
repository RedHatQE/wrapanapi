"""
Methods/classes pertaining to performing actions on a template

"""
from abc import ABCMeta, abstractmethod, abstractproperty

from .entity import Entity


class Template(Entity):
    __metaclass__ = ABCMeta

    def __init__(self, system, name, *args, **kwargs):
        super(Template, self).__init__(system)
        self.name = name

    @property
    def exists(self):
        """Checks if a template of this name exists

        Returns:
            True if it exists
            False if not
        Raises:
            NotImplementedError if system's list_templates() method undefined.
        """
        for template in self.system.list_templates():
            if self.name == template.name:
                return True
        return False

    @abstractmethod
    def deploy(self, *args, **kwargs):
        """Deploy a VM/instance from a template

        Args:
            template: The name of the template to deploy
        Returns: a BaseVM object
        """
        raise NotImplementedError('deploy not implemented.')