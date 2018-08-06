# Imports for convenience
from __future__ import absolute_import

from .systems.ec2 import EC2System
from .systems.google import GoogleCloudSystem
from .systems.hawkular import HawkularSystem
from .systems.lenovo import LenovoSystem
from .systems.msazure import AzureSystem
from .systems.nuage import NuageSystem
from .systems.openstack import OpenstackSystem
from .systems.openstack_infra import OpenstackInfraSystem
from .systems.redfish import RedfishSystem
from .systems.rhevm import RHEVMSystem
from .systems.scvmm import SCVMMSystem
from .systems.vcloud import VmwareCloudSystem
from .systems.virtualcenter import VMWareSystem
from .systems.container.rhopenshift import Openshift

from .entities.vm import VmState

__all__ = [
    'EC2System', 'GoogleCloudSystem', 'HawkularSystem',
    'LenovoSystem', 'AzureSystem', 'NuageSystem', 'OpenstackSystem',
    'OpenstackInfraSystem', 'RedfishSystem', 'RHEVMSystem', 'SCVMMSystem',
    'VmwareCloudSystem', 'VMWareSystem', 'Openshift', 'VmState'
]
