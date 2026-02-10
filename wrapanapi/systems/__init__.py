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
]


def __getattr__(name):
    """Lazy import system classes to avoid loading dependencies for unused providers."""
    if name == "EC2System":
        from .ec2 import EC2System

        return EC2System
    elif name == "GoogleCloudSystem":
        from .google import GoogleCloudSystem

        return GoogleCloudSystem
    elif name == "HawkularSystem":
        from .hawkular import HawkularSystem

        return HawkularSystem
    elif name == "LenovoSystem":
        from .lenovo import LenovoSystem

        return LenovoSystem
    elif name == "AzureSystem":
        from .msazure import AzureSystem

        return AzureSystem
    elif name == "NuageSystem":
        from .nuage import NuageSystem

        return NuageSystem
    elif name == "OpenstackSystem":
        from .openstack import OpenstackSystem

        return OpenstackSystem
    elif name == "OpenstackInfraSystem":
        from .openstack_infra import OpenstackInfraSystem

        return OpenstackInfraSystem
    elif name == "RedfishSystem":
        from .redfish import RedfishSystem

        return RedfishSystem
    elif name == "RHEVMSystem":
        from .rhevm import RHEVMSystem

        return RHEVMSystem
    elif name == "SCVMMSystem":
        from .scvmm import SCVMMSystem

        return SCVMMSystem
    elif name == "VmwareCloudSystem":
        from .vcloud import VmwareCloudSystem

        return VmwareCloudSystem
    elif name == "VMWareSystem":
        from .virtualcenter import VMWareSystem

        return VMWareSystem
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
