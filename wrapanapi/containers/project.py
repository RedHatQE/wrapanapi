from wrapanapi.containers import ContainersResourceBase


class Project(ContainersResourceBase):
    RESOURCE_TYPE = 'namespace'
    VALID_NAME_PATTERN = r'^[a-z0-9][a-z0-9\-]+$'

    def __init__(self, provider, name):
        ContainersResourceBase.__init__(self, provider, name, None)

    def create(self):
        status_code, json_content = self.provider.api.post(
            'namespace',
            {"apiVersion": "v1", "kind": "Project", "metadata": {"name": self.name}}
        )
        if status_code not in (200, 201):
            raise Exception('Failed to create project "{}". status_code: {}; json_content: {};'
                            .format(self.name, status_code, json_content))
        return status_code, json_content
