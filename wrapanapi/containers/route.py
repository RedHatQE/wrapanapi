from wrapanapi.containers import ContainersResourceBase


class Route(ContainersResourceBase):
    RESOURCE_TYPE = 'route'
    CREATABLE = True
    API = 'o_api'
