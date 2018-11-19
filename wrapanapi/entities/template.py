"""
wrapanapi.entities.template

Methods/classes pertaining to performing actions on a template
"""
import six

from abc import ABCMeta, abstractmethod

from wrapanapi.entities.base import Entity, EntityMixin
from wrapanapi.exceptions import MultipleItemsError, NotFoundError


class Template(six.with_metaclass(ABCMeta, Entity)):
    """
    Represents a template on a system
    """
    @abstractmethod
    def deploy(self, vm_name, timeout, **kwargs):
        """
        Deploy a VM/instance with name 'vm_name' using this template

        Returns: an implementation of a BaseVM object
        """


class TemplateMixin(six.with_metaclass(ABCMeta, EntityMixin)):
    """
    Defines methods a wrapanapi.systems.System that manages Templates should have
    """
    @abstractmethod
    def get_template(self, name, **kwargs):
        """
        Get template from system with name 'name'

        This should return only ONE matching entity. If multiple entities match
        the criteria, a MultipleItemsError should be raised

        Returns:
            wrapanapi.entities.Template if it exists
        Raises:
            wrapanapi.exceptions.MultipleItemsError if multiple matches are found
        """

    @abstractmethod
    def create_template(self, name, **kwargs):
        """
        Create template on system with name 'name'

        Returns:
            wrapanapi.entities.Template for newly created templated
        """

    @abstractmethod
    def list_templates(self, **kwargs):
        """
        List templates on system

        Returns:
            list of wrapanapi.entities.Template
        """

    @abstractmethod
    def find_templates(self, name, **kwargs):
        """
        Find templates on system based on name or other filters in kwargs

        Should return an empty list if no matches were found

        Returns:
            list of wrapanapi.entities.Template for matches found
        """

    def does_template_exist(self, name):
        """
        Checks if a template with 'name' exists on the system

        If multiple templates with the same name exists, this still returns 'True'
        """
        try:
            return bool(self.get_template(name))
        except MultipleItemsError:
            return True
        except NotFoundError:
            return False
