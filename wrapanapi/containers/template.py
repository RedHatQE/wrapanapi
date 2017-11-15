from __future__ import absolute_import
from wrapanapi.containers import ContainersResourceBase


class Template(ContainersResourceBase):
    RESOURCE_TYPE = 'template'
    CREATABLE = True
    API = 'o_api'
