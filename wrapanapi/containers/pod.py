from wrapanapi.containers import ContainersResourceBase


class Pod(ContainersResourceBase):
    RESOURCE_TYPE = 'pod'

    @property
    def restart_policy(self):
        return self.spec['restartPolicy']

    @property
    def dns_policy(self):
        return self.spec['dnsPolicy']
