"""
wrapanapi.entities.physical_container

Implements classes and methods related to physical entities such as racks,
blocks, sleds, chassis or enclosures that contain other physical entities such
as physical servers.
"""

from abc import ABCMeta

from wrapanapi.entities.base import Entity


class PhysicalContainer(Entity, metaclass=ABCMeta):
    """Represents a single physical container."""

    def delete(self):
        """Remove the entity on the provider. Not supported on physical containers."""
        raise NotImplementedError("Deleting not supported for physical containers")

    def cleanup(self):
        """
        Remove the entity on the provider and any of its associated resources.

        Not supported on physical containers.
        """
        raise NotImplementedError("Cleanup not supported for physical containers")
