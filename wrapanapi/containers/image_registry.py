from wrapanapi.containers import ContainersResourceBase
from wrapanapi.exceptions import RequestFailedException
from wrapanapi.containers.image import Image


class ImageRegistry(ContainersResourceBase):
    RESOURCE_TYPE = 'imagestream'
    KIND = 'ImageStream'
    API = 'o_api'
    VALID_NAME_PATTERN = '^[a-zA-Z0-9][a-zA-Z0-9\-_\.]+$'

    def __init__(self, provider, name, registry, namespace):
        ContainersResourceBase.__init__(self, provider, name, namespace)
        self.registry = registry
        full_host = registry.split('/')[0]
        self.host, self.port = full_host.split(':') if ':' in full_host else (full_host, '')

    def __repr__(self):
        return '<{} name="{}" host="{}" namespace="{}">'.format(
            self.__class__.__name__, self.name, self.host, self.namespace)

    def import_image(self):
        """Import the image from the docker registry. Returns instance of image"""
        status_code, json_content = self.provider.o_api.post('imagestreamimport', {
            'apiVersion': 'v1',
            'kind': 'ImageStreamImport',
            'metadata': {
                'name': self.name,
                'namespace': self.namespace
            },
            'spec': {
                'import': True,
                'images': [{
                    'from': {
                        'kind': 'DockerImage',
                        'name': self.registry
                    },
                    'importPolicy': {},
                    'to': {'name': 'latest'}
                }]
            }
        }, namespace=self.namespace)
        if status_code not in (200, 201):
            raise RequestFailedException('Failed to import image. status_code: {};  '
                                         'json_content: {};'
                                         .format(status_code, json_content))
        _, image_name, image_id, _ = Image.parse_docker_image_info(
            json_content['status']['images'][-1]['image']['dockerImageReference'])

        return Image(self.provider, image_name, image_id)
