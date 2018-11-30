"""
wrapanapi.entities.stack

Orchestration stacks
"""
from __future__ import absolute_import
import six

from abc import ABCMeta, abstractmethod

from wrapanapi.entities.base import Entity, EntityMixin
from wrapanapi.exceptions import MultipleItemsError, NotFoundError


class Stack(six.with_metaclass(ABCMeta, Entity)):
    """
    Defines methods/properties pertaining to stacks
    """
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


class StackMixin(six.with_metaclass(ABCMeta, EntityMixin)):
    """
    Defines methods for systems that support stacks
    """
    @abstractmethod
    def list_stacks(self, **kwargs):
        """
        Return a list of Stack entities.

        Returns: list of Stack objects
        """

    @abstractmethod
    def find_stacks(self, name, **kwargs):
        """
        Find a stacks based on 'name' or other kwargs

        Returns an empty list if no matches found

        Returns: implementation of wrapanapi.stack.Stack
        """

    @abstractmethod
    def get_stack(self, name, **kwargs):
        """
        Get stack based on name or other kwargs

        Returns: Stack object
        Raises:
            MultipleItemsError if multiple matches found
            NotFoundError if unable to find stack
        """

    def does_stack_exist(self, name):
        """
        Checks if a stack with 'name' exists on the system

        If multiple stacks with the same name exists, this still returns 'True'
        """
        try:
            return bool(self.get_stack(name))
        except MultipleItemsError:
            return True
        except NotFoundError:
            return False
