from __future__ import absolute_import
import requests
import os
import json
import six
import logging

from wrapanapi.exceptions import RestClientException

requests.packages.urllib3.disable_warnings()


class BearerTokenAuth(requests.auth.AuthBase):
    """Attaches a bearer token to the given request object"""
    def __init__(self, token):
        self.token = token

    def __call__(self, r):
        r.headers['Authorization'] = 'Bearer {}'.format(self.token)
        return r


class ContainerClient(object):

    def __init__(self, hostname, auth, protocol="https", port=6443, entry='api/v1', verify=False):
        """Simple REST API client for container management systems

        Args:
            hostname: String with the hostname or IP address of the server (e.g. '10.11.12.13')
            auth: Either a (user, pass) sequence or a string with token
            protocol: Protocol to use for communication with the server
            port: Port to use
            entry: Entry point of the REST API
            verify: 'True' if we want to verify SSL, 'False' otherwise
        """
        self._logger = logging.getLogger(__name__)
        self.api_entry = "{}://{}:{}/{}".format(protocol, hostname, port, entry)
        self.verify = verify
        if type(auth) in (list, set, tuple):
            self.auth = auth
        elif isinstance(auth, six.string_types):
            self.auth = BearerTokenAuth(auth)
        else:
            raise RestClientException('Invalid auth object')

    def entity_path(self, entity_type, name=None, namespace=None):
        """Processing the entity path according to the type, name and namespace"""
        path = '{}s'.format(entity_type)
        if namespace is not None:
            path = os.path.join('namespaces/{}'.format(namespace), path)
        if name is not None:
            path = os.path.join(path, '{}'.format(name))
        return path

    def get(self, entity_type, name=None, namespace=None, convert=None):
        """Sends a request to fetch an entity of specific type

        Fetches a single entity if its name is provided or all of given type if name is ommited.

        Note:
            Some entities are tied to namespaces (projects).
            To fetch these by name, namespace has to be provided as well.

            convert: The convert method to use for the json content (e.g. eval_strings).

        Return:
            Tuple containing status code and json response with requested entity/entities.
        """
        path = self.entity_path(entity_type, name, namespace)
        r = self.raw_get(path)
        json_content = r.json()
        if json_content and convert:
            json_content = convert(json_content)
        return (r.status_code, json_content)

    def post(self, entity_type, data, name=None, namespace=None, convert=None):
        """Sends a POST request to an entity specified by the method parameters"""
        path = self.entity_path(entity_type, name, namespace)
        r = self.raw_post(path, data)
        json_content = r.json()
        if json_content and convert:
            json_content = convert(json_content)
        return (r.status_code, json_content)

    def patch(self, entity_type, data, name=None, namespace=None, convert=None,
              headers={'Content-Type': 'application/strategic-merge-patch+json'}):
        """Sends a PATCH request to an entity specified by the method parameters"""
        path = self.entity_path(entity_type, name, namespace)
        r = self.raw_patch(path, data, headers)
        json_content = r.json()
        if json_content and convert:
            json_content = convert(json_content)
        return (r.status_code, json_content)

    def delete(self, entity_type, name, namespace=None, convert=None):
        """Sends a DELETE request to an entity specified by the method parameters
        (In simple words - delete the entity)"""
        path = self.entity_path(entity_type, name, namespace)
        r = self.raw_delete(path)
        json_content = r.json()
        if json_content and convert:
            json_content = convert(json_content)
        return (r.status_code, json_content)

    def get_json(self, path, headers=None, params=None):
        return self.raw_get(path, headers, params).json()

    def put_status(self, path, data, headers=None):
        r = self.raw_put(path, data, headers)
        return r.ok

    def post_status(self, path, data, headers=None):
        r = self.raw_post(path, data, headers)
        return r.ok

    def delete_status(self, path, headers=None):
        r = self.raw_delete(path, headers)
        return r.ok

    def raw_get(self, path, headers=None, params=None):
        self._logger.debug('GET %s;', path)
        return requests.get(
            os.path.join(self.api_entry, path),
            auth=self.auth,
            verify=self.verify,
            headers=headers,
            params=params)

    def raw_put(self, path, data, headers=None):
        self._logger.debug('PUT %s; data=%s;', path, data)
        return requests.put(
            os.path.join(self.api_entry, path), auth=self.auth, verify=self.verify,
            headers=headers, data=json.dumps(data))

    def raw_post(self, path, data, headers=None):
        self._logger.debug('POST %s; data=%s;', path, data)
        return requests.post(
            os.path.join(self.api_entry, path), auth=self.auth, verify=self.verify,
            headers=headers, data=json.dumps(data))

    def raw_patch(self, path, data, headers=None):
        self._logger.debug('PATCH %s; data=%s;', path, data)
        return requests.patch(
            os.path.join(self.api_entry, path), auth=self.auth, verify=self.verify,
            headers=headers, data=json.dumps(data))

    def raw_delete(self, path, headers=None):
        self._logger.debug('DELETE %s;', path)
        return requests.delete(
            os.path.join(self.api_entry, path), auth=self.auth, verify=self.verify,
            headers=headers)
