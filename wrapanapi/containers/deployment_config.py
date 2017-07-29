from cached_property import cached_property
from wrapanapi.containers import ContainersResourceBase


class DeploymentConfig(ContainersResourceBase):
    RESOURCE_TYPE = 'deploymentconfig'
    VALID_NAME_PATTERN = '^[a-zA-Z0-9][a-zA-Z0-9\-]+$'

    def __init__(self, provider, name, namespace, image, replicas, json_override_params={}):
        ContainersResourceBase.__init__(self, provider, name, namespace)
        self.image = image
        self.replicas = replicas
        self._json_override_params = json_override_params

    @cached_property
    def api(self):
        return self.provider.o_api

    def create(self):
        self._payload = {
            'kind': 'DeploymentConfig',
            'metadata': {
                'name': self.name,
                'namespace': self.namespace
            },
            'spec': {
                'replicas': self.replicas,
                'test': False,
                'triggers': [
                    {
                        'type': 'ConfigChange'
                    }
                ],
                'strategy': {
                    'activeDeadlineSeconds': 21600,
                    'resources': {},
                    'rollingParams': {
                        'intervalSeconds': 1,
                        'maxSurge': '25%',
                        'maxUnavailable': '25%',
                        'timeoutSeconds': 600,
                        'updatePeriodSeconds': 1
                    },
                    'type': 'Rolling'
                },
                'template': {
                    'metadata': {
                        'labels': {
                            'run': self.name
                        }
                    },
                    'spec': {
                        'containers': [
                            {
                                'image': self.image,
                                'imagePullPolicy': 'Always',
                                'name': self.name,
                                'ports': [
                                    {
                                        'containerPort': 8080,
                                        'protocol': 'TCP'
                                    }
                                ],
                                'resources': {},
                                'terminationMessagePath': '/dev/termination-log'
                            }
                        ],
                        'dnsPolicy': 'ClusterFirst',
                        'restartPolicy': 'Always',
                        'securityContext': {},
                        'terminationGracePeriodSeconds': 30
                    }
                }
            },
            'status': {
                'replicas': self.replicas,
                'latestVersion': 1,
                'observedGeneration': 2,
                'updatedReplicas': self.replicas,
                'availableReplicas': self.replicas,
                'unavailableReplicas': 0
            }
        }
        self._payload.update(self._json_override_params)
        return self.provider.o_api.post(self.RESOURCE_TYPE, self._payload, namespace=self.namespace)
