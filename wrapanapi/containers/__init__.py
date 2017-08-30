import re
from cached_property import cached_property

from wrapanapi.exceptions import (RequestFailedException, InvalidValueException,
    LabelNotFoundException)


class ContainersResourceBase(object):
    """
    A container resource base class. This class includes the base functions of (almost)
    all container resources. Each resource has its own API entry and use different API
    (Kubernetes or OpenShift). Each resource has get, post, patch and delete methods which
    directed to the path of the resource.
    The following parameters should be statically defined:
        * RESOURCE_TYPE: (str) the resource type name in the API
        * (optional) VALID_NAME_PATTERN: (str) the regex pattern that match a valid object name
    """
    def __init__(self, provider, name, namespace):
        if hasattr(self, 'VALID_NAME_PATTERN') and not re.match(self.VALID_NAME_PATTERN, name):
            raise InvalidValueException('{0} name "{1}" is invalid. {0} name must '
                                        'match the regex "{2}"'
                                        .format(self.RESOURCE_TYPE, name, self.VALID_NAME_PATTERN))
        self.provider = provider
        self.name = name
        self.namespace = namespace

    def __eq__(self, other):
        return (self.namespace == getattr(other, 'namespace', None) and
                self.name == getattr(other, 'name', None))

    def __repr__(self):
        return '<{} name="{}" namespace="{}">'.format(
            self.__class__.__name__, self.name, self.namespace)

    def exists(self):
        """Return whether this object exists or not."""
        try:
            self.get()
            return True
        except RequestFailedException:
            return False

    @cached_property
    def api(self):
        """The API to use - the default is Kubernetes but some resources use the OpenShift API"""
        return self.provider.api

    @property
    def name_for_api(self):
        """The name used for the API (In Image the name for API is id)"""
        return self.name

    @property
    def project_name(self):
        # For backward compatibility
        return self.namespace

    @property
    def metadata(self):
        return self.get()['metadata']

    @property
    def spec(self):
        return self.get()['spec']

    @property
    def status(self):
        return self.get()['status']

    def get(self, convert=None):
        """Sends a GET request to the resource."""
        status_code, json_content = self.api.get(self.RESOURCE_TYPE, name=self.name_for_api,
                                                 namespace=self.namespace, convert=convert)
        if status_code != 200:
            raise RequestFailedException('GET request of {} "{}" returned status code {}. '
                                         'json content: {}'
                                         .format(self.RESOURCE_TYPE, self.name_for_api,
                                                 status_code, json_content))
        return json_content

    def post(self, data, convert=None):
        """Sends a POST request with the given data to the resource."""
        return self.api.post(self.RESOURCE_TYPE, data, name=self.name_for_api,
                             namespace=self.namespace, convert=convert)

    def patch(self, data, convert=None,
              headers={'Content-Type': 'application/strategic-merge-patch+json'}):
        """Sends a PATCH request with the given data/headers to the resource."""
        return self.api.patch(self.RESOURCE_TYPE, data, name=self.name_for_api,
                              namespace=self.namespace, convert=convert, headers=headers)

    def delete(self, convert=None):
        """Sends a DELETE request to the resource (delete the resource)."""
        return self.api.delete(self.RESOURCE_TYPE, self.name_for_api,
                               namespace=self.namespace, convert=convert)

    def list_labels(self):
        """List the labels of this resource"""
        return self.metadata.get('labels', {})

    def set_label(self, key, value):
        """Sets a label for this resource"""
        return self.patch({'metadata': {'labels': {key: str(value)}}})

    def delete_label(self, key):
        """Deletes a label from this resource"""
        original_labels = self.list_labels()
        if key not in original_labels:
            raise LabelNotFoundException(key)
        del original_labels[key]
        labels = {'$patch': 'replace'}
        labels.update(original_labels)
        return self.patch({'metadata': {'labels': labels}})
