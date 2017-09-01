from cached_property import cached_property

from wrapanapi.containers import ContainersResourceBase


class Image(ContainersResourceBase):
    RESOURCE_TYPE = 'image'

    def __init__(self, provider, name, image_id):
        ContainersResourceBase.__init__(self, provider, name, None)
        self.id = image_id

    def __eq__(self, other):
        return self.id == getattr(other, 'id', None)

    def __repr__(self):
        return '<{} name="{}" id="{}">'.format(
            self.__class__.__name__, self.name, self.id)

    @staticmethod
    def parse_docker_image_info(image_str):
        """Splits full image name into registry, name, id and tag

        Registry and tag are optional, name and id are always present.

        Example:
            <registry>/jboss-fuse-6/fis-karaf-openshift:<tag>@sha256:<sha256> =>
            <registry>, jboss-fuse-6/fis-karaf-openshift, sha256:<sha256>, <tag>
        """
        registry, image_str = image_str.split('/', 1) if '/' in image_str else ('', image_str)
        name, image_id = image_str.split('@')
        tag = name.split(':')[-1] if ':' in image_str else (image_str, '')
        return registry, name, image_id, tag

    @cached_property
    def docker_image_reference(self):
        return self.get().get('dockerImageReference', '')

    @cached_property
    def docker_image_info(self):
        return self.parse_docker_image_info(self.docker_image_reference)

    @property
    def name_for_api(self):
        return self.id

    @cached_property
    def api(self):
        return self.provider.o_api

    @cached_property
    def registry(self):
        return self.docker_image_info[0]

    @cached_property
    def tag(self):
        return self.docker_image_info[3]
