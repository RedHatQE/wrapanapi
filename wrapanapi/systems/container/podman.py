from abc import ABCMeta
from datetime import datetime

from podman import PodmanClient
from proto.utils import cached_property

from wrapanapi.entities.base import Entity
from wrapanapi.systems.base import System


class PodmanContainer(Entity, metaclass=ABCMeta):
    def __init__(self, system, key=None, raw=None, **kwargs):
        """
        Constructor for an PodmanContainer tied to a specific Container.

        Args:
            system: a Podman system object
            key: An unique identifier for the container, could be name or id
            raw: Raw container object if already obtained, or None
        """
        self.key = key
        if not self.key:
            raise ValueError("missing required kwargs identifier: 'key'")
        self._name = kwargs.get("name")
        self._id = kwargs.get("id")
        self._image = kwargs.get("image")

        super().__init__(system, raw, **kwargs)
        self._api = self.system.containers_collection

    @property
    def _identifying_attrs(self):
        return {"name": self.name, "id": self.id}

    @property
    def name(self):
        if not self._name:
            self._name = self.raw.name
        return self._name

    @property
    def id(self):
        if not self._id:
            self._id = self.raw.id
        return self._id

    @property
    def image(self):
        return self._image

    @property
    def uuid(self):
        return self.id

    @property
    def creation_time(self):
        return datetime.fromisoformat(self.raw.attrs["Created"])

    def delete(self, force=False):
        return self.raw.remove(force=force)

    def stop(self):
        return self.raw.stop()

    def cleanup(self, force=False):
        return self.delete(force=force)

    def refresh(self):
        container = self._api.get(self.key)
        self.raw = container
        return self.raw


class Podman(System):
    def __init__(
        self, hostname, username, protocol="http+ssh", port=22, verify_ssl=False, **kwargs
    ):
        super().__init__(hostname, username, protocol, port, verify_ssl, **kwargs)
        self.username = username
        self.hostname = hostname
        self.protocol = protocol
        self.port = port
        self.verify_ssl = verify_ssl

        self._connect()

    def _identifying_attrs(self):
        """
        Return a dict with key, value pairs for each kwarg that is used to
        uniquely identify this system.
        """
        return {"hostname": self.hostname, "port": self.port}

    def _connect(self):
        self.url = "{proto}://{username}@{host}:{port}/run/podman/podman.sock".format(
            proto=self.protocol, username=self.username, host=self.hostname, port=self.port
        )

        self.podmanclient = PodmanClient(base_url=self.url)

    def info(self):
        url = "{proto}://{username}@{host}:{port}".format(
            proto=self.protocol, username=self.username, host=self.hostname, port=self.port
        )
        return f"podman {url}"

    @property
    def containers_collection(self):
        return self.podmanclient.containers

    @cached_property
    def containers(self):
        """Returns list of containers"""
        conInstance = []
        for container in self.containers_collection.list():
            conInstance.append(
                PodmanContainer(
                    system=self,
                    key=container.id,
                    raw=container,
                    name=container.name,
                    image=container.image,
                )
            )
        return conInstance

    def get_container(self, key):
        container = self.containers_collection.get(key)
        return PodmanContainer(
            system=self, key=container.id, raw=container, name=container.name, image=container.image
        )
