"""
wrapanapi.entities.base

Provides method/class definitions for handling any entity on a provider
"""
from __future__ import absolute_import

from abc import ABCMeta, abstractmethod, abstractproperty
from six.moves import reprlib

from wrapanapi.utils import LoggerMixin
from wrapanapi.exceptions import NotFoundError


class Entity(LoggerMixin):
    """
    Base class to represent any object on a provider system as well
    as methods for manipulating that entity (deleting, renaming, etc.)

    Provides properties/methods that should be applicable
    across all entities on all systems.
    """
    __metaclass__ = ABCMeta

    def __init__(self, system, raw=None, **kwargs):
        """
        Constructor for an entity

        An entity is always tied to a specific system

        Args:
            system -- the implementation of wrapanapi.systems.System this entity "resides on"
            raw -- the raw representation of this entity, if already known. This can be an instance
                object as returned by the underlying API/library we use to communicate with
                'system', or it may simply be a dict of JSON data.
            kwargs -- kwargs that are required to uniquely identify this entity

        An entity can be instantiated in two ways:
            1) passing in the 'raw' data
            2) passing in the 'minimal params' (via the kwargs) needed to be able to get the
               correct 'raw' data from the API

        Sometimes kwargs may be required even with method #1 if the 'raw' data which represents
        this entity doesn't provide all the info necessary to look it up (for example, the
        'azure.storage.models.Blob' class does not contain info on 'container', which is needed
        to look up the blob)

        'kwargs' should be the smallest set of args we can use to pull the right info for this
        entity from the system using self.refresh(). These 'unique kwargs' correlate to the
        self._identifying_attrs property below. For many systems, this may be just a uuid, or in
        cases of systems on which names cannot be duplicated, this may be just the 'name' itself.

        'raw' may optionally be passed in at instantiation in cases where we already have obtained
        the raw data for an entity. If this is the case, instance variables will need to be set for
        the 'unique kwargs' based on the given raw data and 'unique kwargs' are not required.

        Whether an instance is created using 'raw', or created using 'kwargs', if it is the same
        entity, the self._identifying_attrs property MUST be equal.
        """
        self.system = system
        self._raw = raw
        self._kwargs = kwargs

    @abstractproperty
    def _identifying_attrs(self):
        """
        Return the list of attributes that make this instance uniquely identifiable without
        needing to query the API for updated data. This should be a dict of kwarg_name, kwarg_value
        for the **kwargs that self.__init__() requires.
        """

    @property
    def _log_id(self):
        """
        Return an str which identifies this VM quickly in logs. Uses _identifying_attrs so that
        API doesn't need to be queried repeatedly.
        """
        string = ""
        for key, val in self._identifying_attrs.items():
            string = "{}{}={} ".format(string, key, val)
        return "<{}>".format(string.strip())

    def __eq__(self, other):
        """
        Define a method for asserting if this instance is equal to another instances of
        the same type.

        This should validate that the system and 'unique identifiers' are equal.

        The unique identifying attributes that are passed in at init such as uuid or name
        are used to assert equality, not raw data, since certain params of the raw data
        are subject to change.
        """
        if not isinstance(other, self.__class__):
            return False
        try:
            return (self.system == other.system and
                    self._identifying_attrs == other._identifying_attrs)
        except AttributeError:
            return False

    def __repr__(self):
        """Represent object.

        Example:
        <wrapanapi.systems.msazure.AzureInstance system=<AzureSystem>
         raw=<azure.mgmt.compute.v2017_03_30.models.virtual_machine.VirtualMachine>,
         kwargs['name']=u'ansinha_test', kwargs['resource_group']=u'Automation'
        >
        """
        # Show object type for system and raw
        params_repr = (
            "system=<{sys_obj_cls}> raw=<{raw_obj_mod}.{raw_obj_cls}>"
            .format(
                sys_obj_cls=self.system.__class__.__name__,
                raw_obj_mod=self._raw.__class__.__module__,
                raw_obj_cls=self._raw.__class__.__name__
            )
        )

        # Show kwarg key/value for each unique kwarg
        a_repr = reprlib.aRepr
        a_repr.maxstring = 100
        a_repr.maxother = 100
        for key, val in self._identifying_attrs.items():
            params_repr = (
                "{existing_params_repr}, kwargs['{kwarg_key}']={kwarg_val}"
                .format(
                    existing_params_repr=params_repr,
                    kwarg_key=key,
                    kwarg_val=a_repr.repr(val),
                )
            )

        return "<{mod_name}.{class_name} {params_repr}>".format(
            mod_name=self.__class__.__module__,
            class_name=self.__class__.__name__,
            params_repr=params_repr,
        )

    def __str__(self):
        try:
            return self.name
        except Exception:
            return self.uuid

    @abstractproperty
    def name(self):
        """
        Returns name from most recent raw data.

        If you need the most up-to-date name, you must call self.refresh() before accessing
        this property.
        """

    @abstractproperty
    def uuid(self):
        """
        Returns uuid from most recent raw data.

        If you need the most up-to-date uuid, you must call self.refresh() before accessing
        this property.

        If the system has no concept of a 'uuid' then some other string value can be used here
        that guarantees uniqueness. This should not return 'None'
        """

    @classmethod
    def get_all_subclasses(cls):
        """
        Return all subclasses that inherit from this class
        """
        for subclass in cls.__subclasses__():
            for nested_subclass in subclass.get_all_subclasses():
                yield nested_subclass
            yield subclass

    @abstractmethod
    def refresh(self):
        """
        Re-pull the data for this entity using the system's API and update
        this instance's attributes.

        This method should be called any time the most up-to-date info needs to be
        returned

        This method should re-set self.raw with fresh data for this entity

        Returns:
            New value of self.raw
        Raises:
            NotFoundError if this entity is not found on the system
        """

    @abstractmethod
    def delete(self):
        """
        Removes the entity on the provider
        """

    @abstractmethod
    def cleanup(self):
        """
        Removes the entity on the provider and any of its associated resources

        This should be more than a simple delete, though if that takes care of
        the job and cleans up everything, simply calling "self.delete()" works
        """

    def rename(self):
        """
        Rename entity.

        May not be implemented for all entities.

        This should update self.raw (via self.refresh() or other) to ensure that
        the self.name property is correct after a successful rename.
        """
        raise NotImplementedError

    @property
    def exists(self):
        """
        Checks if this entity exists on the system

        Catches NotFoundError to return False if the entity does not exist
        """
        try:
            self.refresh()
        except NotFoundError:
            return False
        return True

    @property
    def raw(self):
        """
        Returns the raw data returned by this system's underlying API/library

        Can be an object instance, or a dict
        """
        if not self._raw:
            self.refresh()
        return self._raw

    @raw.setter
    def raw(self, value):
        """
        Sets the raw data
        """
        self._raw = value


class EntityMixin(object):
    """
    Usually an Entity also provides a mixin which defines methods/properties that should
    be defined by a wrapanapi.systems.System that manages that type of entity

    For example, for a 'Vm' entity, example abstract methods would be:
    get_vm, list_vm, find_vm, create_vm

    These methods should return instances (or a list of instances) which describe the entity

    However, methods for operating on a retrieved entity should be defined in the Entity class

    """
    # There may be some common methods/properties that apply at the base level in future...
    pass
