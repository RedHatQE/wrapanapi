from __future__ import absolute_import
from wrapanapi.containers import ContainersResourceBase


class Replicator(ContainersResourceBase):
    RESOURCE_TYPE = 'replicationcontroller'
    KIND = 'ReplicationController'
    CREATABLE = True

    @property
    def replicas(self):
        return self.spec['replicas']

    @property
    def current_replicas(self):
        return self.status['replicas']
