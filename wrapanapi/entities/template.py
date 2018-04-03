"""
Methods/classes pertaining to performing actions on a template

"""
from abc import ABCMeta, abstractmethod, abstractproperty

from .entity import Entity, EntityMixin


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


class TemplateMixin(EntityMixin):
    """
    Defines methods a wrapanapi.systems.System that manages Templates should have
    """
    __metaclass__ = ABCMeta

    @abstractmethod
    def get_template(self, *args, **kwargs):
        """
        Get template from system
    
        Returns:
            wrapanapi.entities.Template if it exists
        """

    @abstractmethod
    def create_template(self, *args, **kwargs):
        """
        Create template on system

        Returns:
            wrapanapi.entities.Template for newly created templated
        """

    @abstractmethod
    def list_template(self, *args, **kwargs):
        """
        List templates on system

        Returns:
            list of wrapanapi.entities.Template
        """

    @abstractmethod
    def find_template(self, *args, **kwargs):
        """
        Find a template on system based on args/kwargs

        Returns:
            list of wrapanapi.entities.Template for matches found
        """
