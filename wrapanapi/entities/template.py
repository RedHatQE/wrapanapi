"""
Methods/classes pertaining to performing actions on a template

"""
from abc import ABCMeta, abstractmethod, abstractproperty

from .entity import Entity, EntityMixin


class Template(Entity):
    __metaclass__ = ABCMeta

    @abstractmethod
    def deploy(self, *args, **kwargs):
        """Deploy a VM/instance from a template

        Args:
            template: The name of the template to deploy
        Returns: an implementation of a BaseVM object
        """


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
    def list_templates(self, *args, **kwargs):
        """
        List templates on system

        Returns:
            list of wrapanapi.entities.Template
        """

    @abstractmethod
    def find_templates(self, *args, **kwargs):
        """
        Find a template on system based on args/kwargs

        Returns:
            list of wrapanapi.entities.Template for matches found
        """
