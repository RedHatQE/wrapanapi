from wrapanapi.containers import ContainersResourceBase


class Project(ContainersResourceBase):
    RESOURCE_TYPE = 'namespace'
    CREATABLE = True
    VALID_NAME_PATTERN = r'^[a-z0-9][a-z0-9\-]+$'

    def __init__(self, provider, name):
        ContainersResourceBase.__init__(self, provider, name, None)

    def __repr__(self):
        return '<{} name="{}">'.format(
            self.__class__.__name__, self.name)
