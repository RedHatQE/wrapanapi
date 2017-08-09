from cached_property import cached_property
from wrapanapi.containers import ContainersResourceBase


class ImageRegistry(ContainersResourceBase):
    RESOURCE_TYPE = 'imagestream'

    def __init__(self, provider, name, registry, namespace):
        ContainersResourceBase.__init__(self, provider, name, namespace)
        self.registry = registry
        full_host = registry.split('/')[0]
        self.host, self.port = full_host.split(':') if ':' in full_host else (full_host, '')

    def __repr__(self):
        return '<{} name="{}" host="{}" namespace="{}">'.format(
            self.__class__.__name__, self.name, self.host, self.namespace)

    @cached_property
    def api(self):
        return self.provider.o_api
