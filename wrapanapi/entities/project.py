"""
wrapanapi.entities.project

Methods/classes pertaining to performing actions on a project
"""
import six

from abc import ABCMeta, abstractmethod

from wrapanapi.entities.base import Entity, EntityMixin
from wrapanapi.exceptions import MultipleItemsError, NotFoundError


class Project(six.with_metaclass(ABCMeta, Entity)):
    """
    Represents a project on a system
    """
    @abstractmethod
    def get_quota(self):
        """
        Deploy a VM/instance with name 'vm_name' using this template

        Returns: an implementation of a BaseVM object
        """


class ProjectMixin(six.with_metaclass(ABCMeta, EntityMixin)):
    """
    Defines methods a wrapanapi.systems.System that manages Projects should have
    """
    @abstractmethod
    def get_project(self, name, **kwargs):
        """
        Get project from system with name 'name'

        This should return only ONE matching entity. If multiple entities match
        the criteria, a MultipleItemsError should be raised

        Returns:
            wrapanapi.entities.Project if it exists
        Raises:
            wrapanapi.exceptions.MultipleItemsError if multiple matches are found
        """

    @abstractmethod
    def create_project(self, name, **kwargs):
        """
        Create project on system with name 'name'

        Returns:
            wrapanapi.entities.Project for newly created project
        """

    @abstractmethod
    def list_project(self, **kwargs):
        """
        List projects on system

        Returns:
            list of wrapanapi.entities.Template
        """

    @abstractmethod
    def find_projects(self, name, **kwargs):
        """
        Find project on system based on name or other filters in kwargs

        Should return an empty list if no matches were found

        Returns:
            list of wrapanapi.entities.Project for matches found
        """

    def does_project_exist(self, name):
        """
        Checks if a project with 'name' exists on the system

        If multiple projects with the same name exists, this still returns 'True'
        """
        try:
            return bool(self.get_project(name))
        except MultipleItemsError:
            return True
        except NotFoundError:
            return False
