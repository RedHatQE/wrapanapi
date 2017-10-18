from wrapanapi.containers import ContainersResourceBase


class DeploymentConfig(ContainersResourceBase):
    RESOURCE_TYPE = 'deploymentconfig'
    CREATABLE = True
    API = 'o_api'
    VALID_NAME_PATTERN = '^[a-zA-Z0-9][a-zA-Z0-9\-]+$'

    def __init__(self, provider, name, namespace):
        ContainersResourceBase.__init__(self, provider, name, namespace)

    @property
    def replicas(self):
        return self.spec['replicas']
