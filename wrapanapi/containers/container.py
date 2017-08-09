from pod import Pod


class Container(object):

    def __init__(self, provider, name, pod, image):
        if not isinstance(pod, Pod):
            raise TypeError('pod argument should be an Pod instance')
        self.provider = provider
        self.name = name
        self.pod = pod
        self.image = image

    @property
    def cg_name(self):
        # For backward compatibility
        return self.pod.name

    @property
    def namespace(self):
        return self.pod.namespace
