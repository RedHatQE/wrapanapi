from cached_property import cached_property
from wrapanapi.containers import ContainersResourceBase


class DeploymentConfig(ContainersResourceBase):
    RESOURCE_TYPE = 'deploymentconfig'

    def __init__(self, provider, name, namespace, template_data, replicas):
        ContainersResourceBase.__init__(self, provider, name, namespace)
        self.template_data = template_data
        self.replicas = replicas

    @cached_property
    def api(self):
        return self.provider.o_api
