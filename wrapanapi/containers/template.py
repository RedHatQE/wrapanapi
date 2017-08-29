from wrapanapi.containers import ContainersResourceBase


class Template(ContainersResourceBase):
    RESOURCE_TYPE = 'template'
    KIND = 'Template'
    CREATABLE = True
    API = 'o_api'
