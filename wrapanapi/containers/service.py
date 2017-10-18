from wrapanapi.containers import ContainersResourceBase


class Service(ContainersResourceBase):
    RESOURCE_TYPE = 'service'
    CREATABLE = True

    @property
    def portal_ip(self):
        return self.spec['clusterIP']

    @property
    def session_affinity(self):
        return self.spec['sessionAffinity']
