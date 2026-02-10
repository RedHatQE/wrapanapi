# Imports for convenience
from .entities.vm import VmState

__all__ = [
    "EC2System",
    "GoogleCloudSystem",
    "HawkularSystem",
    "LenovoSystem",
    "AzureSystem",
    "NuageSystem",
    "OpenstackSystem",
    "OpenstackInfraSystem",
    "RedfishSystem",
    "RHEVMSystem",
    "SCVMMSystem",
    "VmwareCloudSystem",
    "VMWareSystem",
    "Openshift",
    "Podman",
    "VmState",
]


def __getattr__(name):
    """Lazy import system classes to avoid loading dependencies for unused providers."""
    if name == "EC2System":
        from .systems.ec2 import EC2System

        return EC2System
    elif name == "GoogleCloudSystem":
        from .systems.google import GoogleCloudSystem

        return GoogleCloudSystem
    elif name == "HawkularSystem":
        from .systems.hawkular import HawkularSystem

        return HawkularSystem
    elif name == "LenovoSystem":
        from .systems.lenovo import LenovoSystem

        return LenovoSystem
    elif name == "AzureSystem":
        from .systems.msazure import AzureSystem

        return AzureSystem
    elif name == "NuageSystem":
        from .systems.nuage import NuageSystem

        return NuageSystem
    elif name == "OpenstackSystem":
        from .systems.openstack import OpenstackSystem

        return OpenstackSystem
    elif name == "OpenstackInfraSystem":
        from .systems.openstack_infra import OpenstackInfraSystem

        return OpenstackInfraSystem
    elif name == "RedfishSystem":
        from .systems.redfish import RedfishSystem

        return RedfishSystem
    elif name == "RHEVMSystem":
        from .systems.rhevm import RHEVMSystem

        return RHEVMSystem
    elif name == "SCVMMSystem":
        from .systems.scvmm import SCVMMSystem

        return SCVMMSystem
    elif name == "VmwareCloudSystem":
        from .systems.vcloud import VmwareCloudSystem

        return VmwareCloudSystem
    elif name == "VMWareSystem":
        from .systems.virtualcenter import VMWareSystem

        return VMWareSystem
    elif name == "Openshift":
        from .systems.container.rhopenshift import Openshift

        return Openshift
    elif name == "Podman":
        from .systems.container.podman import Podman

        return Podman
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
