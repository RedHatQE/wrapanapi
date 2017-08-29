from wrapanapi.containers import ContainersResourceBase


class Volume(ContainersResourceBase):
    RESOURCE_TYPE = 'persistentvolume'
    KIND = 'PersistentVolume'
    CREATABLE = True

    def __init__(self, provider, name):
        ContainersResourceBase.__init__(self, provider, name, None)

    def __repr__(self):
        return '<{} name="{}" capacity="{}">'.format(
            self.__class__.__name__, self.name, self.capacity)

    @property
    def capacity(self):
        return self.spec['capacity']['storage']

    @property
    def accessmodes(self):
        self.spec['accessModes']
