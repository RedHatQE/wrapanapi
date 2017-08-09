from cached_property import cached_property

from wrapanapi.containers import ContainersResourceBase


class Route(ContainersResourceBase):
    RESOURCE_TYPE = 'route'

    @cached_property
    def api(self):
        return self.provider.o_api
