from __future__ import absolute_import

import copy
import json
import string
import yaml
from collections import Iterable
from functools import partial, wraps
from random import choice

import inflection
import six
from kubernetes import client as kubeclient
from kubernetes.client.rest import ApiException
from miq_version import TemplateName, Version
from openshift import client as ociclient
from wait_for import TimedOutError, wait_for

from wrapanapi.systems.base import System


# this service allows to access db outside of openshift
common_service = """
{
  "api_version": "v1",
  "kind": "Service",
  "metadata": {
    "name": "common-service"
  },
  "spec": {
    "ports": [
      {
        "name": "postgresql",
        "port": "5432"
      }
    ],
    "type": "LoadBalancer",
    "selector": {
      "name": "postgresql"
    }
  }
}
"""

# since 5.10 CloudForms doesn't allow to override image repo url and tag in template
# so, this information has to be stored during template deployment somewhere in project
image_repo_cm_template = """
api_version: v1
kind: ConfigMap
metadata:
  name: "image-repo-data"
data:
  tags: |
    {tags}
"""


def reconnect(decorator):
    def decorate(cls):
        for attr in cls.__dict__:
            if callable(getattr(cls, attr)) and not attr.startswith('_'):
                setattr(cls, attr, decorator(getattr(cls, attr)))
        return cls
    return decorate


def unauthenticated_error_handler(method):
    """Fixes issue with 401 error by restoring connection.
        Sometimes connection to openshift api endpoint gets expired and openshift returns 401.
        As a result tasks in some applications like sprout fail.
    """
    @wraps(method)
    def wrap(*args, **kwargs):
        attempts = 3
        for _ in range(attempts):
            try:
                return method(*args, **kwargs)
            except ApiException as e:
                if e.reason == 'Unauthorized':
                    args[0]._connect()
                else:
                    raise e
        return method(*args, **kwargs)
    return wrap


@reconnect(unauthenticated_error_handler)
class Openshift(System):

    _stats_available = {
        'num_container': lambda self: len(self.list_container()),
        'num_pod': lambda self: len(self.list_pods()),
        'num_service': lambda self: len(self.list_service()),
        'num_replication_controller':
            lambda self: len(self.list_replication_controller()),
        'num_image': lambda self: len(self.list_image_id()),
        'num_node': lambda self: len(self.list_node()),
        'num_image_registry': lambda self: len(self.list_image_registry()),
        'num_project': lambda self: len(self.list_project()),
        'num_route': lambda self: len(self.list_route()),
        'num_template': lambda self: len(self.list_template())
    }

    stream2template_tags_mapping59 = {
        'cfme-openshift-httpd': {'tag': 'HTTPD_IMG_TAG', 'url': 'HTTPD_IMG_NAME'},
        'cfme-openshift-app': {'tag': 'BACKEND_APPLICATION_IMG_TAG',
                               'url': 'BACKEND_APPLICATION_IMG_NAME'},
        'cfme-openshift-app-ui': {'tag': 'FRONTEND_APPLICATION_IMG_TAG',
                                  'url': 'FRONTEND_APPLICATION_IMG_NAME'},
        'cfme-openshift-embedded-ansible': {'tag': 'ANSIBLE_IMG_TAG', 'url': 'ANSIBLE_IMG_NAME'},
        'cfme-openshift-memcached': {'tag': 'MEMCACHED_IMG_TAG', 'url': 'MEMCACHED_IMG_NAME'},
        'cfme-openshift-postgresql': {'tag': 'POSTGRESQL_IMG_TAG', 'url': 'POSTGRESQL_IMG_NAME'},
    }

    stream2template_tags_mapping58 = {
        'cfme58-openshift-app': {'tag': 'APPLICATION_IMG_TAG', 'url': 'APPLICATION_IMG_NAME'},
        'cfme58-openshift-memcached': {'tag': 'MEMCACHED_IMG_TAG', 'url': 'MEMCACHED_IMG_NAME'},
        'cfme58-openshift-postgresql': {'tag': 'POSTGRESQL_IMG_TAG', 'url': 'POSTGRESQL_IMG_NAME'},
    }

    scc_user_mapping59 = (
        {'scc': 'anyuid', 'user': 'cfme-anyuid'},
        {'scc': 'anyuid', 'user': 'cfme-orchestrator'},
        {'scc': 'anyuid', 'user': 'cfme-httpd'},
        {'scc': 'privileged', 'user': 'cfme-privileged'},
    )

    scc_user_mapping58 = (
        {'scc': 'anyuid', 'user': 'cfme-anyuid'},
        {'scc': 'privileged', 'user': 'default'},
    )

    default_namespace = 'openshift'
    required_project_pods = ('httpd', 'memcached', 'postgresql',
                             'cloudforms', 'cloudforms-backend')
    required_project_pods58 = ('memcached', 'postgresql', 'cloudforms')
    not_required_project_pods = ('cloudforms-backend', 'ansible')

    def __init__(self, hostname, protocol="https", port=8443, debug=False,
                 verify_ssl=False, **kwargs):
        super(Openshift, self).__init__(kwargs)
        self.hostname = hostname
        self.protocol = protocol
        self.port = port
        self.username = kwargs.get('username', '')
        self.password = kwargs.get('password', '')
        self.base_url = kwargs.get('base_url', None)
        self.token = kwargs.get('token', '')
        self.auth = self.token if self.token else (self.username, self.password)
        self.debug = debug
        self.verify_ssl = verify_ssl

        self._connect()

    def _identifying_attrs(self):
        """
        Return a dict with key, value pairs for each kwarg that is used to
        uniquely identify this system.
        """
        return {'hostname': self.hostname, 'port': self.port}

    def _connect(self):
        url = '{proto}://{host}:{port}'.format(proto=self.protocol, host=self.hostname,
                                               port=self.port)

        token = 'Bearer {token}'.format(token=self.token)
        config = ociclient.Configuration()
        config.host = url
        config.verify_ssl = self.verify_ssl
        config.debug = self.debug
        config.api_key['authorization'] = token

        self.ociclient = ociclient
        self.kclient = kubeclient
        self.oapi_client = ociclient.ApiClient(config=config)
        self.kapi_client = kubeclient.ApiClient(config=config)
        self.o_api = ociclient.OapiApi(api_client=self.oapi_client)
        self.k_api = kubeclient.CoreV1Api(api_client=self.kapi_client)
        self.security_api = self.ociclient.SecurityOpenshiftIoV1Api(api_client=self.oapi_client)
        self.batch_api = self.kclient.BatchV1Api(api_client=self.kapi_client)  # for job api

    def info(self):
        url = '{proto}://{host}:{port}'.format(proto=self.protocol, host=self.hostname,
                                               port=self.port)
        return "rhopenshift {}".format(url)

    def list_route(self, namespace=None):
        """Returns list of routes"""
        if namespace:
            routes = self.o_api.list_namespaced_route(namespace=namespace).items
        else:
            routes = self.o_api.list_route_for_all_namespaces().items
        return routes

    def list_image_streams(self, namespace=None):
        """Returns list of image streams"""
        if namespace:
            image_streams = self.o_api.list_namespaced_image_stream(namespace=namespace).items
        else:
            image_streams = self.o_api.list_image_stream_for_all_namespaces().items
        return image_streams

    def list_project(self):
        """Returns list of projects"""
        return self.o_api.list_project().items

    def list_template(self, namespace=None):
        """Returns list of templates"""
        if namespace:
            return [t.metadata.name for t in self.o_api.list_namespaced_template(namespace).items]
        else:
            return [t.metadata.name for t in self.o_api.list_template_for_all_namespaces().items]

    # fixme: get rid of this mapping
    list_templates = list_template

    def list_image_stream_images(self):
        """Returns list of images (Docker registry only)"""
        return [item for item in self.o_api.list_image().items
                if item.docker_image_reference is not None]

    def list_deployment_config(self, namespace=None):
        """Returns list of deployment configs"""
        if namespace:
            dc = self.o_api.list_namespaced_deployment_config(namespace=namespace).items
        else:
            dc = self.o_api.list_deployment_config_for_all_namespaces().items
        return dc

    def list_service(self, namespace=None):
        """Returns list of services."""
        if namespace:
            svc = self.k_api.list_namespaced_service(namespace=namespace).items
        else:
            svc = self.k_api.list_service_for_all_namespaces().items
        return svc

    def list_replication_controller(self, namespace=None):
        """Returns list of replication controllers"""
        if namespace:
            rc = self.k_api.list_namespaced_replication_controller(namespace=namespace).items
        else:
            rc = self.k_api.list_replication_controller_for_all_namespaces().items
        return rc

    def list_node(self):
        """Returns list of nodes"""
        nodes = self.k_api.list_node().items
        return nodes

    def cluster_info(self):
        """Returns information about the cluster - number of CPUs and memory in GB"""
        aggregate_cpu, aggregate_mem = 0, 0
        for node in self.list_node():
            aggregate_cpu += int(node.status.capacity['cpu'])
            # converting KiB to GB. 1KiB = 1.024E-6 GB
            aggregate_mem += int(round(int(node.status.capacity['memory'][:-2]) * 0.00000102400))

        return {'cpu': aggregate_cpu, 'memory': aggregate_mem}

    def list_persistent_volume(self):
        """Returns list of persistent volumes"""
        pv = self.k_api.list_persistent_volume().items
        return pv

    def list_pods(self, namespace=None):
        """Returns list of container groups (pods).
        If project_name is passed, only the pods under the selected project will be returned"""
        if namespace:
            pods = self.k_api.list_namespaced_pod(namespace=namespace).items
        else:
            pods = self.k_api.list_pod_for_all_namespaces().items
        return pods

    def list_container(self, namespace=None):
        """Returns list of containers (derived from pods)
        If project_name is passed, only the containers under the selected project will be returned
        """
        pods = self.list_pods(namespace=namespace)
        return [pod.spec.containers for pod in pods]

    def list_image_id(self, namespace=None):
        """Returns list of unique image ids (derived from pods)"""
        pods = self.list_pods(namespace=namespace)
        statuses = []
        for pod in pods:
            for status in pod.status.container_statuses:
                statuses.append(status)
        return sorted(set([status.image_id for status in statuses]))

    def list_image_registry(self, namespace=None):
        """Returns list of image registries (derived from pods)"""
        pods = self.list_pods(namespace=namespace)
        statuses = []
        for pod in pods:
            for status in pod.status.container_statuses:
                statuses.append(status)
        # returns only the image registry name, without the port number in case of local registry
        return sorted(set([status.image.split('/')[0].split(':')[0] for status in statuses]))

    def expose_db_ip(self, namespace):
        """Creates special service in appliance project (namespace) which makes internal appliance
           db be available outside.

        Args:
            namespace: (str) openshift namespace
        Returns: ip
        """
        # creating common service with external ip and extracting assigned ip
        service_obj = self.kclient.V1Service(**json.loads(common_service))
        self.k_api.create_namespaced_service(namespace=namespace, body=service_obj)
        # external ip isn't assigned immediately, so, we have to wait until it is assigned

        return self.get_ip_address(namespace)

    def deploy_template(self, template, tags=None, password='smartvm', **kwargs):
        """Deploy a VM from a template

        Args:
            template: (str) The name of the template to deploy
            tags: (dict) dict with tags if some tag isn't passed it is set to 'latest'
            vm_name: (str) is used as project name if passed. otherwise, name is generated (sprout)
            progress_callback: (func) function to return current progress (sprout)
            template_params: (dict) parameters to override during template deployment
            running_pods: (list) checks that passed pods are running instead of default set
            since input tags are image stream tags whereas template expects its own tags.
            So, input tags should match stream2template_tags_mapping.
            password: this password will be set as default everywhere
        Returns: dict with parameters necessary for appliance setup or None if deployment failed
        """
        self.logger.info("starting template %s deployment", template)
        self.wait_template_exist(namespace=self.default_namespace, name=template)

        if not self.base_url:
            raise ValueError("base url isn't provided")

        version = Version(TemplateName.parse_template(template).version)

        if version >= '5.9':
            tags_mapping = self.stream2template_tags_mapping59
        else:
            tags_mapping = self.stream2template_tags_mapping58

        prepared_tags = {tag['tag']: 'latest' for tag in tags_mapping.values()}
        if tags:
            not_found_tags = [t for t in tags.keys() if t not in tags_mapping.keys()]
            if not_found_tags:
                raise ValueError("Some passed tags {t} don't exist".format(t=not_found_tags))
            for tag, value in tags.items():
                prepared_tags[tags_mapping[tag]['url']] = value['url']
                prepared_tags[tags_mapping[tag]['tag']] = value['tag']

        # create project
        # assuming this is cfme installation and generating project name
        proj_id = "".join(choice(string.digits + string.lowercase) for _ in range(6))

        # for sprout
        if 'vm_name' in kwargs:
            proj_name = kwargs['vm_name']
        else:
            proj_name = "{t}-project-{proj_id}".format(t=template, proj_id=proj_id)

        template_params = kwargs.pop('template_params', {})
        running_pods = kwargs.pop('running_pods', ())
        proj_url = "{proj}.{base_url}".format(proj=proj_id, base_url=self.base_url)
        self.logger.info("unique id %s, project name %s", proj_id, proj_name)

        default_progress_callback = partial(self._progress_log_callback, self.logger, template,
                                            proj_name)
        progress_callback = kwargs.get('progress_callback', default_progress_callback)

        self.create_project(name=proj_name, description=template)
        progress_callback("Created Project `{}`".format(proj_name))

        # grant rights according to scc
        self.logger.info("granting rights to project %s sa", proj_name)
        scc_user_mapping = self.scc_user_mapping59 if version >= '5.9' else self.scc_user_mapping58

        self.logger.info("granting required rights to project's service accounts")
        for mapping in scc_user_mapping:
            self.append_sa_to_scc(scc_name=mapping['scc'], namespace=proj_name, sa=mapping['user'])
        progress_callback("Added service accounts to appropriate scc")

        # appliances prior 5.9 don't need such rights
        # and those rights are embedded into templates since 5.9.2.2
        if version >= '5.9' and version < '5.9.2.2':
            # grant roles to orchestrator
            self.logger.info("assigning additional roles to cfme-orchestrator")
            auth_api = self.ociclient.AuthorizationOpenshiftIoV1Api(api_client=self.oapi_client)
            orchestrator_sa = self.kclient.V1ObjectReference(name='cfme-orchestrator',
                                                             kind='ServiceAccount',
                                                             namespace=proj_name)

            view_role = self.kclient.V1ObjectReference(name='view')
            view_role_binding_name = self.kclient.V1ObjectMeta(name='view')
            view_role_binding = self.ociclient.V1RoleBinding(role_ref=view_role,
                                                             subjects=[orchestrator_sa],
                                                             metadata=view_role_binding_name)
            self.logger.debug("creating 'view' role binding "
                              "for cfme-orchestrator sa in project %s", proj_name)
            auth_api.create_namespaced_role_binding(namespace=proj_name, body=view_role_binding)

            edit_role = self.kclient.V1ObjectReference(name='edit')
            edit_role_binding_name = self.kclient.V1ObjectMeta(name='edit')
            edit_role_binding = self.ociclient.V1RoleBinding(role_ref=edit_role,
                                                             subjects=[orchestrator_sa],
                                                             metadata=edit_role_binding_name)
            self.logger.debug("creating 'edit' role binding "
                              "for cfme-orchestrator sa in project %s", proj_name)
            auth_api.create_namespaced_role_binding(namespace=proj_name, body=edit_role_binding)

        self.logger.info("project sa created via api have no some mandatory roles. adding them")
        self._restore_missing_project_role_bindings(namespace=proj_name)
        progress_callback("Added all necessary role bindings to project `{}`".format(proj_name))

        # creating common service with external ip
        ext_ip = self.expose_db_ip(proj_name)
        progress_callback("Common Service has been added")

        # adding config map with image stream urls and tags
        image_repo_cm = image_repo_cm_template.format(tags=json.dumps(tags))
        self.create_config_map(namespace=proj_name, **yaml.safe_load(image_repo_cm))

        # creating pods and etc
        processing_params = {'DATABASE_PASSWORD': password,
                             'APPLICATION_DOMAIN': proj_url}
        processing_params.update(prepared_tags)

        # updating template parameters
        processing_params.update(template_params)
        self.logger.info(("processing template and passed params in order to "
                          "prepare list of required project entities"))
        template_entities = self.process_template(name=template, namespace=self.default_namespace,
                                                  parameters=processing_params)
        self.logger.debug("template entities:\n %r", template_entities)
        progress_callback("Template has been processed")
        self.create_template_entities(namespace=proj_name, entities=template_entities)
        progress_callback("All template entities have been created")

        self.logger.info("verifying that all created entities are up and running")
        progress_callback("Waiting for all pods to be ready and running")
        try:
            wait_for(self.is_vm_running, num_sec=600,
                     func_kwargs={'vm_name': proj_name, 'running_pods': running_pods})
            self.logger.info("all pods look up and running")
            progress_callback("Everything has been deployed w/o errors")
            return {'url': proj_url,
                    'external_ip': ext_ip,
                    'project': proj_name,
                    }
        except TimedOutError:
            self.logger.error("deployment failed. Please check failed pods details")
            # todo: return and print all failed pod details
            raise

    def create_template_entities(self, namespace, entities):
        """Creates entities from openshift template.

        Since there is no methods in openshift/kubernetes rest api for app deployment from template,
        it is necessary to create template entities one by one using respective entity api.

        Args:
            namespace: (str) openshift namespace
            entities: (list) openshift entities

        Returns: None
        """
        self.logger.debug("passed template entities:\n %r", entities)
        kinds = set([e['kind'] for e in entities])
        entity_names = {e: inflection.underscore(e) for e in kinds}
        proc_names = {k: 'create_{e}'.format(e=p) for k, p in entity_names.items()}

        for entity in entities:
            if entity['kind'] in kinds:
                procedure = getattr(self, proc_names[entity['kind']], None)
                obtained_entity = procedure(namespace=namespace, **entity)
                self.logger.debug(obtained_entity)
            else:
                self.logger.error("some entity %s isn't present in entity creation list", entity)

    def start_vm(self, vm_name):
        """Starts a vm.

        Args:
            vm_name: name of the vm to be started
        Returns: whether vm action has been initiated properly
        """
        self.logger.info("starting vm/project %s", vm_name)
        if self.does_project_exist(vm_name):
            for pod in self.get_required_pods(vm_name):
                self.scale_entity(name=pod, namespace=vm_name, replicas=1)
        else:
            raise ValueError("Project with name {n} doesn't exist".format(n=vm_name))

    def stop_vm(self, vm_name):
        """Stops a vm.

        Args:
            vm_name: name of the vm to be stopped
        Returns: whether vm action has been initiated properly
        """
        self.logger.info("stopping vm/project %s", vm_name)
        if self.does_project_exist(vm_name):
            for pod in self.get_required_pods(vm_name):
                self.scale_entity(name=pod, namespace=vm_name, replicas=0)
        else:
            raise ValueError("Project with name {n} doesn't exist".format(n=vm_name))

    def delete_vm(self, vm_name):
        """Deletes a vm.

        Args:
            vm_name: name of the vm to be deleted
        Returns: whether vm action has been initiated properly
        """
        self.logger.info("removing vm/project %s", vm_name)
        self.delete_project(name=vm_name)
        return True

    def does_vm_exist(self, vm_name):
        """Does VM exist?

        Args:
            vm_name: The name of the VM
        Returns: whether vm exists
        """
        return self.does_project_exist(vm_name)

    @staticmethod
    def _update_template_parameters(template, **params):
        """Updates openshift template parameters.
        Since Openshift REST API doesn't provide any api to change default parameter values as
        it is implemented in `oc process`. This method implements such a parameter replacement.

        Args:
            template: Openshift's template object
            params: bunch of key=value parameters
        Returns: updated template
        """
        template = copy.deepcopy(template)
        if template.parameters:
            new_parameters = template.parameters
            for new_param, new_value in params.items():
                for index, old_param in enumerate(new_parameters):
                    if old_param['name'] == new_param:
                        old_param = new_parameters.pop(index)
                        if 'generate' in old_param:
                            old_param['generate'] = None
                            old_param['_from'] = None

                        old_param['value'] = new_value
                        new_parameters.append(old_param)
                        template.parameters = new_parameters
        return template

    def process_template(self, name, namespace, parameters=None):
        """Implements template processing mechanism similar to `oc process`.

        Args:
            name: (str) template name
            namespace: (str) openshift namespace
            parameters: parameters and values to replace default ones
        Return: list of objects stored in template
        """
        # workaround for bug https://github.com/openshift/openshift-restclient-python/issues/60
        raw_response = self.o_api.read_namespaced_template(name=name, namespace=namespace,
                                                           _preload_content=False)
        raw_data = json.loads(raw_response.data)

        return self.process_raw_template(body=raw_data, namespace=namespace, parameters=parameters)

    def process_raw_template(self, body, namespace, parameters=None):
        """Implements template processing mechanism similar to `oc process`.
        It does two functions
          1. parametrized templates have to be processed in order to replace parameters with values.
          2. templates consist of list of objects. Those objects have to be extracted
          before creation accordingly.

        Args:
            body: (dict) template body
            namespace: (str) openshift namespace
            parameters: parameters and values to replace default ones
        Return: list of objects stored in template
        """
        updated_data = self.rename_structure(body)
        read_template = self.ociclient.V1Template(**updated_data)
        if parameters:
            updated_template = self._update_template_parameters(template=read_template,
                                                                **parameters)
        else:
            updated_template = read_template
        raw_response = self.o_api.create_namespaced_processed_template(namespace=namespace,
                                                                       body=updated_template,
                                                                       _preload_content=False)
        raw_data = json.loads(raw_response.data)
        updated_data = self.rename_structure(raw_data)
        processed_template = self.ociclient.V1Template(**updated_data)
        return processed_template.objects

    def rename_structure(self, struct):
        """Fixes inconsistency in input/output data of openshift python client methods

        Args:
            struct: data to process and rename
        Return: updated data
        """
        if not isinstance(struct, six.string_types) and isinstance(struct, Iterable):
            if isinstance(struct, dict):
                for key in struct.keys():
                    # we shouldn't rename something under data or spec
                    if key == 'stringData':
                        # this key has to be renamed but its contents should be left intact
                        struct[inflection.underscore(key)] = struct.pop(key)
                    elif key in ('spec', 'data', 'string_data', 'annotations'):
                        # these keys and data should be left intact
                        pass
                    else:
                        # all this data should be processed and updated
                        val = self.rename_structure(struct.pop(key))
                        struct[inflection.underscore(key)] = val
                return struct
            else:
                for index, item in enumerate(struct):
                    struct[index] = self.rename_structure(item)
                return struct
        else:
            return struct

    def create_config_map(self, namespace, **kwargs):
        """Creates ConfigMap entity using REST API.

        Args:
            namespace: openshift namespace where entity has to be created
            kwargs: ConfigMap data
        Return: data if entity was created w/o errors
        """
        conf_map = self.kclient.V1ConfigMap(**kwargs)
        conf_map_name = conf_map.to_dict()['metadata']['name']
        self.logger.info("creating config map %s", conf_map_name)
        output = self.k_api.create_namespaced_config_map(namespace=namespace, body=conf_map)
        self.wait_config_map_exist(namespace=namespace, name=conf_map_name)
        return output

    def replace_config_map(self, namespace, **kwargs):
        """Replace ConfigMap entity using REST API.

        Args:
            namespace: openshift namespace where entity has to be created
            kwargs: ConfigMap data
        Return: data if entity was created w/o errors
        """
        conf_map = self.kclient.V1ConfigMap(**kwargs)
        conf_map_name = conf_map.to_dict()['metadata']['name']
        self.logger.info("replacing config map %s", conf_map_name)
        output = self.k_api.replace_namespaced_config_map(namespace=namespace,
                                                          name=conf_map_name,
                                                          body=conf_map)
        return output

    def create_stateful_set(self, namespace, **kwargs):
        """Creates StatefulSet entity using REST API.

        Args:
            namespace: openshift namespace where entity has to be created
            kwargs: StatefulSet data
        Return: data if entity was created w/o errors
        """
        st = self.kclient.V1beta1StatefulSet(**kwargs)
        st_name = st.to_dict()['metadata']['name']
        self.logger.info("creating stateful set %s", st_name)
        api = self.kclient.AppsV1beta1Api(api_client=self.kapi_client)
        output = api.create_namespaced_stateful_set(namespace=namespace, body=st)
        self.wait_stateful_set_exist(namespace=namespace, name=st_name)
        return output

    def create_service(self, namespace, **kwargs):
        """Creates Service entity using REST API.

        Args:
            namespace: openshift namespace where entity has to be created
            kwargs: Service data
        Return: data if entity was created w/o errors
        """
        service = self.kclient.V1Service(**kwargs)
        service_name = service.to_dict()['metadata']['name']
        self.logger.info("creating service %s", service_name)
        output = self.k_api.create_namespaced_service(namespace=namespace, body=service)
        self.wait_service_exist(namespace=namespace, name=service_name)
        return output

    def create_endpoints(self, namespace, **kwargs):
        """Creates Endpoints entity using REST API.

        Args:
            namespace: openshift namespace where entity has to be created
            kwargs: Endpoints data
        Return: data if entity was created w/o errors
        """
        endpoints = self.kclient.V1Endpoints(**kwargs)
        endpoints_name = endpoints.to_dict()['metadata']['name']
        self.logger.info("creating endpoints %s", endpoints_name)
        output = self.k_api.create_namespaced_endpoints(namespace=namespace, body=endpoints)
        self.wait_endpoints_exist(namespace=namespace, name=endpoints_name)
        return output

    def create_route(self, namespace, **kwargs):
        """Creates Route entity using REST API.

        Args:
            namespace: openshift namespace where entity has to be created
            kwargs: Route data
        Return: data if entity was created w/o errors
        """
        route = self.ociclient.V1Route(**kwargs)
        route_name = route.to_dict()['metadata']['name']
        self.logger.info("creating route %s", route_name)
        output = self.o_api.create_namespaced_route(namespace=namespace, body=route)
        self.wait_route_exist(namespace=namespace, name=route_name)
        return output

    def create_service_account(self, namespace, **kwargs):
        """Creates Service Account entity using REST API.

        Args:
            namespace: openshift namespace where entity has to be created
            kwargs: Service Account data
        Return: data if entity was created w/o errors
        """
        sa = self.kclient.V1ServiceAccount(**kwargs)
        sa_name = sa.to_dict()['metadata']['name']
        self.logger.info("creating service account %s", sa_name)
        output = self.k_api.create_namespaced_service_account(namespace=namespace, body=sa)
        self.wait_service_account_exist(namespace=namespace, name=sa_name)
        return output

    def create_role_binding(self, namespace, **kwargs):
        """Creates RoleBinding entity using REST API.

        Args:
            namespace: openshift namespace where entity has to be created
            kwargs: RoleBinding data
        Return: data if entity was created w/o errors
        """
        ObjectRef = self.kclient.V1ObjectReference  # noqa
        auth_api = self.ociclient.AuthorizationOpenshiftIoV1Api(api_client=self.oapi_client)
        # there is some version mismatch in api. so, it would be better to remove version
        kwargs.pop('api_version', None)
        role_binding_name = kwargs['metadata']['name']

        # role and subjects data should be turned into objects before passing them to RoleBinding
        role_name = kwargs.pop('role_ref')['name']
        role = ObjectRef(name=role_name)
        subjects = [ObjectRef(namespace=namespace, **subj) for subj in kwargs.pop('subjects')]
        role_binding = self.ociclient.V1RoleBinding(role_ref=role, subjects=subjects, **kwargs)
        self.logger.debug("creating role binding %s in project %s", role_binding_name, namespace)
        output = auth_api.create_namespaced_role_binding(namespace=namespace,
                                                         body=role_binding)
        self.wait_role_binding_exist(namespace=namespace, name=role_binding_name)
        return output

    def create_image_stream(self, namespace, **kwargs):
        """Creates Image Stream entity using REST API.

        Args:
            namespace: openshift namespace where entity has to be created
            kwargs: Image Stream data
        Return: data if entity was created w/o errors
        """
        image_stream = self.ociclient.V1ImageStream(**kwargs)
        is_name = image_stream.to_dict()['metadata']['name']
        self.logger.info("creating image stream %s", is_name)
        output = self.o_api.create_namespaced_image_stream(namespace=namespace, body=image_stream)
        self.wait_image_stream_exist(namespace=namespace, name=is_name)
        return output

    def create_secret(self, namespace, **kwargs):
        """Creates Secret entity using REST API.

        Args:
            namespace: openshift namespace where entity has to be created
            kwargs: Secret data
        Return: data if entity was created w/o errors
        """
        secret = self.kclient.V1Secret(**kwargs)
        secret_name = secret.to_dict()['metadata']['name']
        self.logger.info("creating secret %s", secret_name)
        output = self.k_api.create_namespaced_secret(namespace=namespace, body=secret)
        self.wait_secret_exist(namespace=namespace, name=secret_name)
        return output

    def create_deployment_config(self, namespace, **kwargs):
        """Creates Deployment Config entity using REST API.

        Args:
            namespace: openshift namespace where entity has to be created
            kwargs: Deployment Config data
        Return: data if entity was created w/o errors
        """
        dc = self.ociclient.V1DeploymentConfig(**kwargs)
        dc_name = dc.to_dict()['metadata']['name']
        self.logger.info("creating deployment config %s", dc_name)
        output = self.o_api.create_namespaced_deployment_config(namespace=namespace, body=dc)
        self.wait_deployment_config_exist(namespace=namespace,
                                          name=dc_name)
        return output

    def create_persistent_volume_claim(self, namespace, **kwargs):
        """Creates Persistent Volume Claim entity using REST API.

        Args:
            namespace: openshift namespace where entity has to be created
            kwargs: Persistent Volume Claim data
        Return: data if entity was created w/o errors
        """
        pv_claim = self.kclient.V1PersistentVolumeClaim(**kwargs)
        pv_claim_name = pv_claim.to_dict()['metadata']['name']
        self.logger.info("creating persistent volume claim %s", pv_claim_name)
        output = self.k_api.create_namespaced_persistent_volume_claim(namespace=namespace,
                                                                      body=pv_claim)
        self.wait_persistent_volume_claim_exist(namespace=namespace,
                                                name=pv_claim_name)
        return output

    def create_project(self, name, description=None):
        """Creates Project(namespace) using REST API.

        Args:
            name: openshift namespace name
            description: project description. it is necessary to store appliance version
        Return: data if entity was created w/o errors
        """
        proj = self.ociclient.V1Project()
        proj.metadata = {'name': name, 'annotations': {}}
        if description:
            proj.metadata['annotations'] = {'openshift.io/description': description}
        self.logger.info("creating new project with name %s", name)
        output = self.o_api.create_project(body=proj)
        self.wait_project_exist(name=name)
        return output

    def run_job(self, namespace, body):
        """Creates job from passed template, runs it and waits for the job to be accomplished

        Args:
            namespace: openshift namespace name
            body: yaml job template
        Return: True/False
        """
        body = self.rename_structure(body)
        job_name = body['metadata']['name']
        self.batch_api.create_namespaced_job(namespace=namespace, body=body)

        return self.wait_job_finished(namespace, job_name)

    def wait_job_finished(self, namespace, name, wait='15m'):
        """Waits for job to accomplish

        Args:
            namespace: openshift namespace name
            name: job name
            wait: stop waiting after "wait" time
        Return: True/False
        """
        def job_wait_accomplished():
            try:
                job = self.batch_api.read_namespaced_job_status(name=name,
                                                                namespace=namespace)
                # todo: replace with checking final statuses
                return bool(job.status.succeeded)
            except KeyError:
                return False
        return wait_for(job_wait_accomplished, num_sec=wait)[0]

    def wait_persistent_volume_claim_status(self, namespace, name, status, wait='1m'):
        """Waits until pvc gets some particular status.
           For example: Bound.

        Args:
            namespace: openshift namespace name
            name: job name
            status: pvc status
            wait: stop waiting after "wait" time
        Return: True/False
        """
        def pvc_wait_status():
            try:
                pvc = self.k_api.read_namespaced_persistent_volume_claim(name=name,
                                                                         namespace=namespace)
                return pvc.status.phase == status
            except KeyError:
                return False

        return wait_for(pvc_wait_status, num_sec=wait)[0]

    def wait_project_exist(self, name, wait=60):
        """Checks whether Project exists within some time.

        Args:
            name: openshift namespace name
            wait: entity should appear for this time then - True, otherwise False
        Return: True/False
        """
        return wait_for(self._does_exist, num_sec=wait,
                        func_kwargs={'func': self.o_api.read_project, 'name': name})[0]

    def wait_config_map_exist(self, namespace, name, wait=60):
        """Checks whether Config Map exists within some time.

        Args:
            name: entity name
            namespace: openshift namespace where entity should exist
            wait: entity should appear for this time then - True, otherwise False
        Return: True/False
        """
        return wait_for(self._does_exist, num_sec=wait,
                        func_kwargs={'func': self.k_api.read_namespaced_config_map,
                                     'name': name,
                                     'namespace': namespace})[0]

    def wait_stateful_set_exist(self, namespace, name, wait=900):
        """Checks whether StatefulSet exists within some time.

        Args:
            name: entity name
            namespace: openshift namespace where entity should exist
            wait: entity should appear for this time then - True, otherwise False
        Return: True/False
        """
        api = self.kclient.AppsV1beta1Api(api_client=self.kapi_client)
        read_st = api.read_namespaced_stateful_set
        return wait_for(self._does_exist, num_sec=wait,
                        func_kwargs={'func': read_st,
                                     'name': name,
                                     'namespace': namespace})[0]

    def wait_service_exist(self, namespace, name, wait=60):
        """Checks whether Service exists within some time.

        Args:
            name: entity name
            namespace: openshift namespace where entity should exist
            wait: entity should appear for this time then - True, otherwise False
        Return: True/False
        """
        return wait_for(self._does_exist, num_sec=wait,
                        func_kwargs={'func': self.k_api.read_namespaced_service,
                                     'name': name,
                                     'namespace': namespace})[0]

    def wait_endpoints_exist(self, namespace, name, wait=60):
        """Checks whether Endpoints exists within some time.

        Args:
            name: entity name
            namespace: openshift namespace where entity should exist
            wait: entity should appear for this time then - True, otherwise False
        Return: True/False
        """
        return wait_for(self._does_exist, num_sec=wait,
                        func_kwargs={'func': self.k_api.read_namespaced_endpoints,
                                     'name': name,
                                     'namespace': namespace})[0]

    def wait_route_exist(self, namespace, name, wait=60):
        """Checks whether Route exists within some time.

        Args:
            name: entity name
            namespace: openshift namespace where entity should exist
            wait: entity should appear for this time then - True, otherwise False
        Return: True/False
        """
        return wait_for(self._does_exist, num_sec=wait,
                        func_kwargs={'func': self.o_api.read_namespaced_route,
                                     'name': name,
                                     'namespace': namespace})[0]

    def wait_service_account_exist(self, namespace, name, wait=60):
        """Checks whether Service Account exists within some time.

        Args:
            name: entity name
            namespace: openshift namespace where entity should exist
            wait: entity should appear for this time then - True, otherwise False
        Return: True/False
        """
        return wait_for(self._does_exist, num_sec=wait,
                        func_kwargs={'func': self.k_api.read_namespaced_service_account,
                                     'name': name,
                                     'namespace': namespace})[0]

    def wait_image_stream_exist(self, namespace, name, wait=60):
        """Checks whether Image Stream exists within some time.

        Args:
            name: entity name
            namespace: openshift namespace where entity should exist
            wait: entity should appear for this time then - True, otherwise False
        Return: True/False
        """
        return wait_for(self._does_exist, num_sec=wait,
                        func_kwargs={'func': self.o_api.read_namespaced_image_stream,
                                     'name': name,
                                     'namespace': namespace})[0]

    def wait_role_binding_exist(self, namespace, name, wait=60):
        """Checks whether RoleBinding exists within some time.

        Args:
            name: entity name
            namespace: openshift namespace where entity should exist
            wait: entity should appear for this time then - True, otherwise False
        Return: True/False
        """
        auth_api = self.ociclient.AuthorizationOpenshiftIoV1Api(api_client=self.oapi_client)
        return wait_for(self._does_exist, num_sec=wait,
                        func_kwargs={'func': auth_api.read_namespaced_role_binding,
                                     'name': name,
                                     'namespace': namespace})[0]

    def wait_secret_exist(self, namespace, name, wait=90):
        """Checks whether Secret exists within some time.

        Args:
            name: entity name
            namespace: openshift namespace where entity should exist
            wait: entity should appear for this time then - True, otherwise False
        Return: True/False
        """
        return wait_for(self._does_exist, num_sec=wait,
                        func_kwargs={'func': self.k_api.read_namespaced_secret,
                                     'name': name,
                                     'namespace': namespace})[0]

    def wait_persistent_volume_claim_exist(self, namespace, name, wait=60):
        """Checks whether Persistent Volume Claim exists within some time.

        Args:
            name: entity name
            namespace: openshift namespace where entity should exist
            wait: entity should appear for this time then - True, otherwise False
        Return: True/False
        """
        return wait_for(self._does_exist, num_sec=wait,
                        func_kwargs={'func': self.k_api.read_namespaced_persistent_volume_claim,
                                     'name': name,
                                     'namespace': namespace})[0]

    def wait_deployment_config_exist(self, namespace, name, wait=600):
        """Checks whether Deployment Config exists within some time.

        Args:
            name: entity name
            namespace: openshift namespace where entity should exist
            wait: entity should appear for this time then - True, otherwise False
        Return: True/False
        """
        read_dc = self.o_api.read_namespaced_deployment_config
        return wait_for(self._does_exist, num_sec=wait,
                        func_kwargs={'func': read_dc,
                                     'name': name,
                                     'namespace': namespace})[0]

    def wait_template_exist(self, namespace, name, wait=60):
        """Checks whether Template exists within some time.

        Args:
            name: entity name
            namespace: openshift namespace where entity should exist
            wait: entity should appear for this time then - True, otherwise False
        Return: True/False
        """
        return wait_for(self._does_exist, num_sec=wait,
                        func_kwargs={'func': self.o_api.read_namespaced_template,
                                     'name': name,
                                     'namespace': namespace})[0]

    def _does_exist(self, func, **kwargs):
        try:
            func(**kwargs)
            return True
        except ApiException as e:
            self.logger.info("ApiException occurred %s, it looks like obj doesn't exist", e)
            return False

    def _restore_missing_project_role_bindings(self, namespace):
        """Fixes one of issues in Openshift REST API
          create project doesn't add necessary roles to default sa, probably bug, this is workaround

        Args:
            namespace: openshift namespace where roles are absent
        Return: None
        """
        # adding builder role binding
        auth_api = self.ociclient.AuthorizationOpenshiftIoV1Api(api_client=self.oapi_client)
        builder_role = self.kclient.V1ObjectReference(name='system:image-builder')
        builder_sa = self.kclient.V1ObjectReference(name='builder',
                                                    kind='ServiceAccount',
                                                    namespace=namespace)
        builder_role_binding_name = self.kclient.V1ObjectMeta(name='builder-binding')
        builder_role_binding = self.ociclient.V1RoleBinding(role_ref=builder_role,
                                                            subjects=[builder_sa],
                                                            metadata=builder_role_binding_name)
        auth_api.create_namespaced_role_binding(namespace=namespace, body=builder_role_binding)

        # adding deployer role binding
        deployer_role = self.kclient.V1ObjectReference(name='system:deployer')
        deployer_sa = self.kclient.V1ObjectReference(name='deployer',
                                                     kind='ServiceAccount',
                                                     namespace=namespace)
        deployer_role_binding_name = self.kclient.V1ObjectMeta(name='deployer-binding')
        deployer_role_binding = self.ociclient.V1RoleBinding(role_ref=deployer_role,
                                                             subjects=[deployer_sa],
                                                             metadata=deployer_role_binding_name)
        auth_api.create_namespaced_role_binding(namespace=namespace, body=deployer_role_binding)

        # adding admin role binding
        admin_role = self.kclient.V1ObjectReference(name='admin')
        admin_user = self.kclient.V1ObjectReference(name='admin',
                                                    kind='User',
                                                    namespace=namespace)
        admin_role_binding_name = self.kclient.V1ObjectMeta(name='admin-binding')
        admin_role_binding = self.ociclient.V1RoleBinding(role_ref=admin_role,
                                                          subjects=[admin_user],
                                                          metadata=admin_role_binding_name)
        auth_api.create_namespaced_role_binding(namespace=namespace, body=admin_role_binding)

        # adding image-puller role binding
        puller_role = self.kclient.V1ObjectReference(name='system:image-puller')
        group_name = 'system:serviceaccounts:{proj}'.format(proj=namespace)
        puller_group = self.kclient.V1ObjectReference(name=group_name,
                                                      kind='SystemGroup',
                                                      namespace=namespace)
        role_binding_name = self.kclient.V1ObjectMeta(name='image-puller-binding')
        puller_role_binding = self.ociclient.V1RoleBinding(role_ref=puller_role,
                                                           subjects=[puller_group],
                                                           metadata=role_binding_name)
        auth_api.create_namespaced_role_binding(namespace=namespace, body=puller_role_binding)

    def delete_project(self, name, wait=300):
        """Removes project(namespace) and all entities in it.

        Args:
            name: project name
            wait: within this time project should disappear
        Return: None
        """
        self.logger.info("removing project %s", name)
        if self.does_project_exist(name=name):
            self.o_api.delete_project(name=name)
            try:
                wait_for(lambda name: not self.does_project_exist(name=name), num_sec=wait,
                         func_kwargs={'name': name})
            except TimedOutError:
                raise TimedOutError('project {n} was not removed within {w} sec'.format(n=name,
                                                                                        w=wait))

    def scale_entity(self, namespace, name, replicas, wait=60):
        """Allows to scale up/down entities.
        One of cases when this is necessary is emulation of stopping/starting appliance

        Args:
            namespace: openshift namespace
            name: entity name. it can be either stateless Pod from DeploymentConfig or StatefulSet
            replicas: number of replicas 0..N
            wait: time to wait for scale up/down
        Return: None
        """
        # only dc and statefulsets can be scaled
        st_api = self.kclient.AppsV1beta1Api(api_client=self.kapi_client)

        scale_val = self.kclient.V1Scale(spec=self.kclient.V1ScaleSpec(replicas=replicas))
        if self.is_deployment_config(name=name, namespace=namespace):
            self.o_api.patch_namespaced_deployment_config_scale(name=name, namespace=namespace,
                                                                body=scale_val)

            def check_scale_value():
                got_scale = self.o_api.read_namespaced_deployment_config_scale(name=name,
                                                                               namespace=namespace)
                return int(got_scale.spec.replicas or 0)

        elif self.is_stateful_set(name=name, namespace=namespace):
            # replace this code with stateful_set_scale when kubernetes shipped with openshift
            # client gets upgraded
            st_spec = self.kclient.V1beta1StatefulSetSpec
            st = self.kclient.V1beta1StatefulSet(spec=st_spec(replicas=replicas))
            st_api.patch_namespaced_stateful_set(name=name, namespace=namespace, body=st)

            def check_scale_value():
                got_scale = st_api.read_namespaced_stateful_set(name=name, namespace=namespace)
                return int(got_scale.spec.replicas or 0)
        else:
            raise ValueError("This name %s is not found among "
                             "deployment configs or stateful sets", name)
        self.logger.info("scaling entity %s to %s replicas", name, replicas)
        wait_for(check_scale_value, num_sec=wait, fail_condition=lambda val: val != replicas)

    def get_project_by_name(self, project_name):
        """Returns only the selected Project object"""
        return next(proj for proj in self.list_project() if proj.metadata.name == project_name)

    def get_scc(self, name):
        """Returns Security Context Constraint by name

        Args:
          name: security context constraint name
        Returns: security context constraint object
        """
        return self.security_api.read_security_context_constraints(name)

    def create_scc(self, body):
        """Creates Security Context Constraint from passed structure.
        Main aim is to create scc from read and parsed yaml file.

        Args:
          body: security context constraint structure
        Returns: security context constraint object
        """
        raw_scc = self.rename_structure(body)
        if raw_scc.get('api_version') == 'v1':
            # there is inconsistency between api and some scc files. v1 is not accepted by api now
            raw_scc.pop('api_version')
        scc = self.ociclient.V1SecurityContextConstraints(**raw_scc)
        return self.security_api.create_security_context_constraints(body=scc)

    def append_sa_to_scc(self, scc_name, namespace, sa):
        """Appends Service Account to respective Security Constraint

        Args:
          scc_name: security context constraint name
          namespace: service account's namespace
          sa: service account's name
        Returns: updated security context constraint object
        """
        user = 'system:serviceaccount:{proj}:{usr}'.format(proj=namespace,
                                                           usr=sa)
        if self.get_scc(scc_name).users is None:
            # ocp 3.6 has None for users if there is no sa in it
            update_scc_cmd = [
                {"op": "add",
                 "path": "/users",
                 "value": [user]}]
        else:
            update_scc_cmd = [
                {"op": "add",
                 "path": "/users/-",
                 "value": user}]
        self.logger.debug("adding user %r to scc %r", user, scc_name)
        return self.security_api.patch_security_context_constraints(name=scc_name,
                                                                    body=update_scc_cmd)

    def remove_sa_from_scc(self, scc_name, namespace, sa):
        """Removes Service Account from respective Security Constraint

        Args:
          scc_name: security context constraint name
          namespace: service account's namespace
          sa: service account's name
        Returns: updated security context constraint object
        """
        user = 'system:serviceaccount:{proj}:{usr}'.format(proj=namespace,
                                                           usr=sa)
        # json patch's remove works only with indexes. so we have to figure out index
        try:
            index = next(val[0] for val in enumerate(self.get_scc(scc_name).users)
                         if val[1] == user)
        except StopIteration:
            raise ValueError("No such sa {} in scc {}".format(user, scc_name))
        update_scc_cmd = [
            {"op": "remove",
             "path": "/users/{}".format(index)}]
        self.logger.debug("removing user %r from scc %s with index %s", user, scc_name, index)
        return self.security_api.patch_security_context_constraints(name=scc_name,
                                                                    body=update_scc_cmd)

    def is_vm_running(self, vm_name, running_pods=()):
        """Emulates check is vm(appliance) up and running

        Args:
            vm_name: (str) project(namespace) name
            running_pods: (list) checks only passed number of pods. otherwise, default set.
        Return: True/False
        """
        if not self.does_vm_exist(vm_name):
            return False
        self.logger.info("checking all pod statuses for vm name %s", vm_name)

        for pod_name in running_pods or self.get_required_pods(vm_name):
            if self.is_pod_running(namespace=vm_name, name=pod_name):
                continue
            else:
                return False

        # todo: check url is available + db is accessable
        return True

    def list_deployment_config_names(self, namespace):
        """Extracts and returns list of Deployment Config names

        Args:
            namespace: project(namespace) name
        Return: (list) deployment config names
        """
        dcs = self.o_api.list_namespaced_deployment_config(namespace=namespace)
        return [dc.metadata.name for dc in dcs.items]

    def list_stateful_set_names(self, namespace):
        """Returns list of Stateful Set names

        Args:
            namespace: project(namespace) name
        Return: (list) stateful set names
        """
        st_api = self.kclient.AppsV1beta1Api(api_client=self.kapi_client)
        sts = st_api.list_namespaced_stateful_set(namespace=namespace)
        return [st.metadata.name for st in sts.items]

    def is_deployment_config(self, namespace, name):
        """Checks whether passed name belongs to deployment configs in appropriate namespace

        Args:
            namespace: project(namespace) name
            name: entity name
        Return: True/False
        """
        return name in self.list_deployment_config_names(namespace=namespace)

    def is_stateful_set(self, namespace, name):
        """Checks whether passed name belongs to Stateful Sets in appropriate namespace

        Args:
            namespace: project(namespace) name
            name: entity name
        Return: True/False
        """
        return name in self.list_stateful_set_names(namespace=namespace)

    def does_project_exist(self, name):
        """Checks whether Project exists.

        Args:
            name: openshift namespace name
        Return: True/False
        """
        return self._does_exist(func=self.o_api.read_project, name=name)

    def is_vm_stopped(self, vm_name):
        """Check whether vm isn't running.
        There is no such state stopped for vm in openshift therefore
        it just checks that vm isn't running

        Args:
            vm_name: project name
        Return: True/False
        """
        pods = self.k_api.list_namespaced_pod(namespace=vm_name).items
        if pods:
            self.logger.info(("some pods are still "
                              "running: {}").format([pod.metadata.name for pod in pods]))
        return not bool(pods)

    def wait_vm_running(self, vm_name, num_sec=900):
        """Checks whether all project pods are in ready state.

        Args:
            vm_name: project name
            num_sec: all pods should get ready for this time then - True, otherwise False
        Return: True/False
        """
        wait_for(self.is_vm_running, [vm_name], num_sec=num_sec)
        return True

    def wait_vm_stopped(self, vm_name, num_sec=600):
        """Checks whether all project pods are stopped.

        Args:
            vm_name: project name
            num_sec: all pods should not be ready for this time then - True, otherwise False
        Return: True/False
        """
        wait_for(self.is_vm_stopped, [vm_name], num_sec=num_sec)
        return True

    def current_ip_address(self, vm_name):
        """Tries to retrieve project's external ip

        Args:
            vm_name: project name
        Return: ip address or None
        """
        try:
            common_svc = self.k_api.read_namespaced_service(name='common-service',
                                                            namespace=vm_name)
            return common_svc.spec.external_i_ps[0]
        except Exception:
            return None

    def is_vm_suspended(self, vm_name):
        """There is no such state in openshift

        Args:
            vm_name: project name
        Return: False
        """
        return False

    def in_steady_state(self, vm_name):
        """Return whether the specified virtual machine is in steady state

        Args:
            vm_name: VM name
        Returns: True/False
        """
        return (self.is_vm_running(vm_name)
                or self.is_vm_stopped(vm_name)
                or self.is_vm_suspended(vm_name))

    @property
    def can_rename(self):
        return hasattr(self, "rename_vm")

    def list_project_names(self):
        """Obtains project names

        Returns: list of project names
        """
        projects = self.o_api.list_project().items
        return [proj.metadata.name for proj in projects]

    list_vms = list_vm = list_project_names

    def get_appliance_version(self, vm_name):
        """Returns appliance version if it is possible

         Args:
            vm_name: the openshift project name of the podified appliance
        Returns: version
        """
        try:
            proj = self.o_api.read_project(vm_name)
            description = proj.metadata.annotations['openshift.io/description']
            return Version(TemplateName.parse_template(description).version)
        except (ApiException, KeyError, ValueError):
            try:
                return Version(TemplateName.parse_template(vm_name).version)
            except ValueError:
                return None

    def delete_template(self, template_name, namespace='openshift'):
        """Deletes template

        Args:
            template_name: stored openshift template name
            namespace: project name
        Returns: result of delete operation
        """
        options = self.kclient.V1DeleteOptions()
        return self.o_api.delete_namespaced_template(name=template_name, namespace=namespace,
                                                     body=options)

    def get_meta_value(self, instance, key):
        raise NotImplementedError(
            'Provider {} does not implement get_meta_value'.format(type(self).__name__))

    def set_meta_value(self, instance, key):
        raise NotImplementedError(
            'Provider {} does not implement get_meta_value'.format(type(self).__name__))

    def vm_status(self, vm_name):
        """Returns current vm/appliance state

        Args:
          vm_name: the openshift project name of the podified appliance
        Returns: up/down or exception if vm doesn't exist
        """
        if not self.does_vm_exist(vm_name):
            raise ValueError("Vm {} doesn't exist".format(vm_name))
        return 'up' if self.is_vm_running(vm_name) else 'down'

    def vm_creation_time(self, vm_name):
        """Returns time when vm/appliance was created

        Args:
          vm_name:  the openshift project name of the podified appliance
        Return: datetime obj
        """
        if not self.does_vm_exist(vm_name):
            raise ValueError("Vm {} doesn't exist".format(vm_name))
        projects = self.o_api.list_project().items
        project = next(proj for proj in projects if proj.metadata.name == vm_name)
        return project.metadata.creation_timestamp

    @staticmethod
    def _progress_log_callback(logger, source, destination, progress):
        logger.info("Provisioning progress {}->{}: {}".format(
            source, destination, str(progress)))

    def vm_hardware_configuration(self, vm_name):
        """Collects project's cpu and ram usage

        Args:
            vm_name: openshift's data
        Returns: collected data
        """
        hw_config = {'ram': 0,
                     'cpu': 0}
        if not self.does_vm_exist(vm_name):
            return hw_config

        proj_pods = self.k_api.list_namespaced_pod(vm_name)
        for pod in proj_pods.items:
            for container in pod.spec.containers:
                cpu = container.resources.requests['cpu']
                hw_config['cpu'] += float(cpu[:-1]) / 1000 if cpu.endswith('m') else float(cpu)

                ram = container.resources.requests['memory']
                if ram.endswith('Mi'):
                    hw_config['ram'] += float(ram[:-2])
                elif ram.endswith('Gi'):
                    hw_config['ram'] += float(ram[:-2]) * 1024
                elif ram.endswith('Ki'):
                    hw_config['ram'] += float(ram[:-2]) / 1024
                else:
                    hw_config['ram'] += ram
        return hw_config

    def usage_and_quota(self):
        installed_ram = 0
        installed_cpu = 0
        used_ram = 0
        used_cpu = 0
        # todo: finish this method later
        return {
            # RAM
            'ram_used': used_ram,
            'ram_total': installed_ram,
            'ram_limit': None,
            # CPU
            'cpu_used': used_cpu,
            'cpu_total': installed_cpu,
            'cpu_limit': None,
        }

    def get_required_pods(self, vm_name):
        """Provides list of pods which should be present in appliance

        Args:
            vm_name: openshift project name
        Returns: list
        """
        version = self.get_appliance_version(vm_name)
        if version and version < '5.9':
            return self.required_project_pods58
        else:
            return self.required_project_pods

    def get_ip_address(self, vm_name, timeout=600):
        """ Returns the IP address for the selected appliance.

        Args:
            vm_name: The name of the vm to obtain the IP for.
            timeout: The IP address wait timeout.
        Returns: A string containing the first found IP that isn't the device.
        """
        try:
            ip_address, tc = wait_for(lambda: self.current_ip_address(vm_name),
                                      fail_condition=None,
                                      delay=5,
                                      num_sec=timeout,
                                      message="get_ip_address from openshift")
        except TimedOutError:
            ip_address = None
        return ip_address

    def disconnect(self):
        pass

    def get_appliance_tags(self, name):
        """Returns appliance tags stored in appropriate config map if it exists.

        Args:
            name: appliance project name
        Returns: dict with tags and urls
        """
        try:
            read_data = self.k_api.read_namespaced_config_map(name='image-repo-data',
                                                              namespace=name)
            return json.loads(read_data.data['tags'])
        except ApiException:
            return {}

    def get_appliance_url(self, name):
        """Returns appliance url assigned by Openshift

        Args:
            name: appliance project name
        Returns: url or None
        """
        try:
            route = self.o_api.list_namespaced_route(name)
            return route.items[0].spec.host
        except (ApiException, IndexError):
            return None

    def get_appliance_uuid(self, name):
        """Returns appliance uuid assigned by Openshift

        Args:
            name: appliance project name
        Returns: uuid
        """
        return self.get_project_by_name(name).metadata.uid

    def is_appliance(self, name):
        """Checks whether passed vm/project is appliance

        Args:
            name: appliance project name
        Returns: True/False
        """
        return bool(self.get_appliance_tags(name))

    def find_job_pods(self, namespace, name):
        """Finds and returns all remaining job pods

        Args:
            namespace: project(namespace) name
            name: job name
        Returns: list of pods
        """
        pods = []
        for pod in self.list_pods(namespace=namespace):
            if pod.metadata.labels.get('job-name', '') == name:
                pods.append(pod)
        return pods

    def read_pod_log(self, namespace, name):
        """Reads and returns pod log

        Args:
            namespace: project(namespace) name
            name: pod name
        Returns: list of pods
        """
        return self.k_api.read_namespaced_pod_log(name=name, namespace=namespace)

    def delete_pod(self, namespace, name, options=None):
        """Tries to remove passed pod

        Args:
            namespace: project(namespace) name
            name: pod name
            options: delete options like force delete and etc
        Returns: Pod
        """
        return self.k_api.delete_namespaced_pod(namespace=namespace, name=name,
                                                body=options or self.kclient.V1DeleteOptions())

    def is_pod_running(self, namespace, name):
        """Checks whether pod is running

        Args:
            namespace: (str) project(namespace) name
            name: (str) pod name
        Return: True/False
        """
        self.logger.info("checking pod status %s", name)

        if self.is_deployment_config(name=name, namespace=namespace):
            dc = self.o_api.read_namespaced_deployment_config(name=name, namespace=namespace)
            status = dc.status.ready_replicas
        elif self.is_stateful_set(name=name, namespace=namespace):
            pods = self.k_api.list_namespaced_pod(namespace=namespace,
                                                  label_selector='name={n}'.format(n=name))
            pod_stats = [pod.status.container_statuses[-1].ready for pod in pods.items]
            status = all(pod_stats)
        else:
            raise ValueError("No such pod name among StatefulSets or Stateless Pods")

        if status and int(status) > 0:
            self.logger.debug("pod %s looks up and running", name)
            return True
        else:
            self.logger.debug("pod %s isn't up yet", name)
            return False

    def wait_pod_running(self, namespace, name, num_sec=300):
        """Waits for pod to switch to ready state

        Args:
            namespace: project name
            name: pod name
            num_sec: all pods should get ready for this time then - True, otherwise TimeoutError
        Return: True/False
        """
        wait_for(self.is_pod_running, [namespace, name], fail_condition=False, num_sec=num_sec)
        return True

    def is_pod_stopped(self, namespace, name):
        """Check whether pod isn't running.

        Args:
            namespace: (str) project(namespace) name
            name: (str) pod name
        Return: True/False
        """
        pods = self.k_api.list_namespaced_pod(namespace=namespace).items
        return not bool([pod for pod in pods if name == pod.metadata.name])

    def wait_pod_stopped(self, namespace, name, num_sec=300):
        """Waits for pod to stop

        Args:
            namespace: project name
            name: pod name
            num_sec: all pods should disappear - True, otherwise TimeoutError
        Return: True/False
        """
        wait_for(self.is_pod_stopped, [namespace, name], num_sec=num_sec)
        return True

    def run_command(self, namespace, name, cmd, **kwargs):
        """Connects to pod and tries to run

        Args:
            namespace: (str) project name
            name: (str) pod name
            cmd: (list) command to run
        Return: command output
        """
        # there are some limitations and this code isn't robust enough due to
        # https://github.com/kubernetes-client/python/issues/58
        return self.k_api.connect_post_namespaced_pod_exec(namespace=namespace,
                                                           name=name,
                                                           command=cmd,
                                                           stdout=True,
                                                           stderr=True,
                                                           **kwargs)
