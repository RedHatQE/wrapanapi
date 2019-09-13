"""
wrapanapi.entities.instance

Instances which run on cloud providers
"""

from abc import ABCMeta, abstractproperty

from .vm import Vm


class Instance(Vm, metaclass=ABCMeta):
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
