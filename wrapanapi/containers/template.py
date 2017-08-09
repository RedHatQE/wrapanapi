from cached_property import cached_property

from wrapanapi.containers import ContainersResourceBase


class Template(ContainersResourceBase):
    RESOURCE_TYPE = 'template'

    @cached_property
    def api(self):
        return self.provider.o_api
