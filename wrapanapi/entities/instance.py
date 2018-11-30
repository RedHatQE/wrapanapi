"""
wrapanapi.entities.instance

Instances which run on cloud providers
"""
from __future__ import absolute_import
import six

from abc import ABCMeta, abstractproperty

from .vm import Vm


class Instance(six.with_metaclass(ABCMeta, Vm)):
    """
    Adds a few additional properties/methods pertaining to VMs hosted
    on a cloud platform.
    """
    @abstractproperty
    def type(self):
        """
        Return type or flavor of the Instance

        E.g. 'm1.micro' in ec2
        """
