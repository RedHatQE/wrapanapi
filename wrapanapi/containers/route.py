from wrapanapi.containers import ContainersResourceBase


class Route(ContainersResourceBase):
    RESOURCE_TYPE = 'route'
    KIND = 'Route'
    CREATABLE = True
    API = 'o_api'
