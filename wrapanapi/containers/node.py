from wrapanapi.containers import ContainersResourceBase


class Node(ContainersResourceBase):
    RESOURCE_TYPE = 'node'

    def __init__(self, provider, name):
        ContainersResourceBase.__init__(self, provider, name, None)

    @property
    def cpu(self):
        return int(self.status['capacity']['cpu'])

    @property
    def ready(self):
        return self.status['conditions'][0]['status']

    @property
    def memory(self):
        return int(round(int(
            self.status['capacity']['memory'][:-2]) * 0.00000102400))  # KiB to GB
