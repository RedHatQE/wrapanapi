from __future__ import absolute_import

import copy
import inflection
import json
import six
import string

from functools import partial, wraps
from random import choice

from collections import Iterable
from kubernetes import client as kubeclient
from kubernetes.client.rest import ApiException
from openshift import client as ociclient
from wait_for import wait_for, TimedOutError

from miq_version import TemplateName, Version
from wrapanapi.base import WrapanapiAPIBase


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
class Openshift(WrapanapiAPIBase):

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

    stream2template_tags_mapping = {
        'cfme-openshift-httpd': 'HTTPD_IMG_TAG',
        'cfme-openshift-app': 'BACKEND_APPLICATION_IMG_TAG',
        'cfme-openshift-app-ui': 'FRONTEND_APPLICATION_IMG_TAG',
        'cfme-openshift-embedded-ansible': 'ANSIBLE_IMG_TAG',
        'cfme-openshift-memcached': 'MEMCACHED_IMG_TAG',
        'cfme-openshift-postgresql': 'POSTGRESQL_IMG_TAG',
        'cfme58-openshift-app': 'APPLICATION_IMG_TAG',
        'cfme58-openshift-memcached': 'MEMCACHED_IMG_TAG',
        'cfme58-openshift-postgresql': 'POSTGRESQL_IMG_TAG',
    }

    template_tags = [tag for tag in stream2template_tags_mapping.values()]
    stream_tags = [tag for tag in stream2template_tags_mapping.keys()]

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
        """Returns list of image ids (derived from pods)"""
        pods = self.list_pods(namespace=namespace)
        statuses = []
        for pod in pods:
            for status in pod.status.container_statuses:
                statuses.append(status)
        return [status.image_id for status in statuses]

    def list_image_registry(self, namespace=None):
        """Returns list of image registries (derived from pods)"""
        pods = self.list_pods(namespace=namespace)
        statuses = []
        for pod in pods:
            for status in pod.status.container_statuses:
                statuses.append(status)
        return [status.image for status in statuses]

    def deploy_template(self, template, tags=None, password='smartvm', **kwargs):
        """Deploy a VM from a template

        Args:
            template: (str) The name of the template to deploy
            tags: (dict) dict with tags if some tag isn't passed it is set to 'latest'
            vm_name: (str) is used as project name if passed. otherwise, name is generated (sprout)
            progress_callback: (func) function to return current progress (sprout)
            since input tags are image stream tags whereas template expects its own tags.
            So, input tags should match stream2template_tags_mapping.
            password: this password will be set as default everywhere
        Returns: dict with parameters necessary for appliance setup or None if deployment failed
        """
        self.logger.info("starting template %s deployment", template)
        self.wait_template_exist(namespace=self.default_namespace, name=template)

        if not self.base_url:
            raise ValueError("base url isn't provided")

        prepared_tags = {key: 'latest' for key in self.template_tags}
        if tags:
            not_found_tags = [tag for tag in tags.keys() if tag not in self.stream_tags]
            if not_found_tags:
                raise ValueError("Some passed tags {t} don't exist".format(t=not_found_tags))
            for tag, value in tags.items():
                prepared_tags[self.stream2template_tags_mapping[tag]] = value

        # create project
        # assuming this is cfme installation and generating project name
        proj_id = "".join(choice(string.digits + string.lowercase) for _ in range(6))

        # for sprout
        if 'vm_name' in kwargs:
            proj_name = kwargs['vm_name']
        else:
            proj_name = "{t}-project-{proj_id}".format(t=template, proj_id=proj_id)

        proj_url = "{proj}.{base_url}".format(proj=proj_id, base_url=self.base_url)
        self.logger.info("unique id %s, project name %s", proj_id, proj_name)

        default_progress_callback = partial(self._progress_log_callback, self.logger, template,
                                            proj_name)
        progress_callback = kwargs.get('progress_callback', default_progress_callback)

        self.create_project(name=proj_name, description=template)
        progress_callback("Created Project `{}`".format(proj_name))

        version = Version(TemplateName.parse_template(template).version)

        # grant rights according to scc
        self.logger.info("granting rights to project %s sa", proj_name)
        if version >= '5.9':
            scc_user_mapping = (
                {'scc': 'anyuid', 'user': 'cfme-anyuid'},
                {'scc': 'anyuid', 'user': 'cfme-orchestrator'},
                {'scc': 'anyuid', 'user': 'cfme-httpd'},
                {'scc': 'privileged', 'user': 'cfme-privileged'},
            )
        else:
            scc_user_mapping = (
                {'scc': 'anyuid', 'user': 'cfme-anyuid'},
                {'scc': 'privileged', 'user': 'default'},
            )

        self.logger.info("granting required rights to project's service accounts")
        security_api = self.ociclient.SecurityOpenshiftIoV1Api(api_client=self.oapi_client)
        for mapping in scc_user_mapping:
            old_scc = security_api.read_security_context_constraints(name=mapping['scc'])
            got_users = old_scc.users if old_scc.users else []
            got_users.append('system:serviceaccount:{proj}:{usr}'.format(proj=proj_name,
                                                                         usr=mapping['user']))
            self.logger.debug("adding users %r to scc %r", got_users, mapping['scc'])
            security_api.patch_security_context_constraints(name=mapping['scc'],
                                                            body={'users': got_users})
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
        service_obj = self.kclient.V1Service(**json.loads(common_service))
        self.k_api.create_namespaced_service(namespace=proj_name, body=service_obj)
        progress_callback("Common Service has been added")

        # creating pods and etc
        processing_params = {'DATABASE_PASSWORD': password,
                             'APPLICATION_DOMAIN': proj_url}
        processing_params.update(prepared_tags)
        self.logger.info(("processing template and passed params in order to "
                          "prepare list of required project entities"))
        template_entities = self.process_template(name=template, namespace=self.default_namespace,
                                                  parameters=processing_params)
        self.logger.debug("template entities:\n %r", template_entities)
        kinds = set([e['kind'] for e in template_entities])
        entity_names = {e: inflection.underscore(e) for e in kinds}
        proc_names = {k: 'create_{e}'.format(e=p) for k, p in entity_names.items()}
        progress_callback("Template has been processed")
        for entity in template_entities:
            if entity['kind'] in kinds:
                procedure = getattr(self, proc_names[entity['kind']], None)
                # todo: this code should be paralleled
                obtained_entity = procedure(namespace=proj_name, **entity)
                self.logger.debug(obtained_entity)
            else:
                self.logger.error("some entity %s isn't present in entity creation list", entity)

        progress_callback("All template entities have been created")
        # creating and obtaining db ip
        common_svc = self.k_api.read_namespaced_service(name='common-service',
                                                        namespace=proj_name)
        ext_ip = common_svc.spec.external_i_ps[0]

        self.logger.info("verifying that all created entities are up and running")
        progress_callback("Waiting for all pods to be ready and running")
        try:
            wait_for(self.is_vm_running, num_sec=600,
                     func_kwargs={'vm_name': proj_name})
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
        """Implements template processing mechanizm similar to `oc process`.
        It does to functions
          1. parametrized templates have to be processed in order to replace parameters with values.
          2. templates consist of list of objects. Those objects have to be extracted
          before creation accordingly.

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
        updated_data = self.rename_structure(raw_data)
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

    def wait_project_exist(self, name, wait=5):
        """Checks whether Project exists within some time.

        Args:
            name: openshift namespace name
            wait: entity should appear for this time then - True, otherwise False
        Return: True/False
        """
        return wait_for(self._does_exist, num_sec=wait,
                        func_kwargs={'func': self.o_api.read_project, 'name': name})[0]

    def wait_config_map_exist(self, namespace, name, wait=30):
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

    def wait_stateful_set_exist(self, namespace, name, wait=600):
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

    def wait_service_exist(self, namespace, name, wait=30):
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

    def wait_route_exist(self, namespace, name, wait=30):
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

    def wait_service_account_exist(self, namespace, name, wait=30):
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

    def wait_image_stream_exist(self, namespace, name, wait=10):
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

    def wait_role_binding_exist(self, namespace, name, wait=10):
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

    def wait_secret_exist(self, namespace, name, wait=30):
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

    def wait_persistent_volume_claim_exist(self, namespace, name, wait=30):
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

    def wait_template_exist(self, namespace, name, wait=5):
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

    @staticmethod
    def _does_exist(func, **kwargs):
        try:
            func(**kwargs)
            return True
        except ApiException:
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

    def is_vm_running(self, vm_name):
        """Emulates check is vm(appliance) up and running

        Args:
            vm_name: (str) project(namespace) name
        Return: True/False
        """
        if not self.does_vm_exist(vm_name):
            return False
        self.logger.info("checking all pod statuses for vm name %s", vm_name)

        for pod_name in self.get_required_pods(vm_name):
            if self.is_deployment_config(name=pod_name, namespace=vm_name):
                dc = self.o_api.read_namespaced_deployment_config(name=pod_name, namespace=vm_name)
                status = dc.status.ready_replicas
            elif self.is_stateful_set(name=pod_name, namespace=vm_name):
                pods = self.k_api.list_namespaced_pod(namespace=vm_name,
                                                      label_selector='name={n}'.format(n=pod_name))
                pod_stats = [pod.status.container_statuses[-1].ready for pod in pods.items]
                status = all(pod_stats)
            else:
                raise ValueError("No such pod name among StatefulSets or Stateless Pods")

            if status and int(status) > 0:
                self.logger.debug("pod %s looks up and running", pod_name)
                continue
            else:
                self.logger.debug("pod %s isn't up yet", pod_name)
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
        return not self.is_vm_running(vm_name)

    def wait_vm_running(self, vm_name, num_sec=360):
        """Checks whether all project pods are in ready state.

        Args:
            vm_name: project name
            num_sec: all pods should get ready for this time then - True, otherwise False
        Return: True/False
        """
        wait_for(self.is_vm_running, [vm_name], num_sec=num_sec)
        return True

    def wait_vm_stopped(self, vm_name, num_sec=360):
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
        return (self.is_vm_running(vm_name) or self.is_vm_stopped(vm_name) or
                self.is_vm_suspended(vm_name))

    @property
    def can_rename(self):
        return hasattr(self, "rename_vm")

    def list_project_names(self):
        """Obtains project names

        Returns: list of project names
        """
        projects = self.o_api.list_project().items
        return [proj.metadata.name for proj in projects]

    list_vm = list_project_names

    def get_appliance_version(self, vm_name):
        """Returns appliance version if it is possible

            Args:
                vm_name: project name
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
        raise NotImplementedError('vm_status not implemented.')

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
