from __future__ import absolute_import

import re
import copy
import json
import string
import yaml
from collections import Iterable
from functools import partial, wraps
from random import choice

import inflection
import six
from cached_property import cached_property
from kubernetes import client as kubeclient
from kubernetes import config as kubeclientconfig
from openshift.dynamic import DynamicClient
from kubernetes.client.rest import ApiException
from miq_version import TemplateName, Version
from openshift import client as ociclient
from wait_for import TimedOutError, wait_for

from wrapanapi.entities import (Template, TemplateMixin, Vm, VmMixin, VmState, ProjectMixin,
                                Project)
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


class Project(Project):

    def __init__(self, system, raw=None, **kwargs):
        """
        Construct a VMWareVirtualMachine instance

        Args:
            system: instance of VMWareSystem
            raw: pyVmomi.vim.VirtualMachine object
            name: name of VM
        """
        super(Project, self).__init__(system, raw, **kwargs)
        self._name = raw.metadata.name if raw else kwargs.get('name')
        if not self._name:
            raise ValueError("missing required kwarg 'name'")
        self.v1_project = self.system.ocp_client.resources.get(
            api_version='project.openshift.io/v1', kind='Project')

    @property
    def get_quota(self):
        return self.system.ocp_client.resources.get(api_version='v1', kind='ResourceQuota').get(
            namespace=self.name)

    @property
    def _identifying_attrs(self):
        return {'name': self._name}

    @property
    def name(self):
        return self._name

    @property
    def uuid(self):
        try:
            return str(self.raw.metadata.uid)
        except AttributeError:
            return self.name

    @property
    def ip(self):
        raise NotImplementedError

    def start(self):
        raise NotImplementedError

    def stop(self):
        raise NotImplementedError

    def restart(self):
        raise NotImplementedError

    def delete(self):
        self.v1_project.delete(name=self.name)

    def refresh(self):
        self.raw = self.system.get_project(name=self.name).raw
        return self.raw

    def cleanup(self):
        return self.delete()


class Pod(Vm):
    state_map = {
        'pending': VmState.PENDING,
        'running': VmState.RUNNING,
        'succeeded': VmState.SUCCEEDED,
        'failed': VmState.FAILED,
        'unknown': VmState.UNKNOWN
    }

    def __init__(self, system, raw=None, **kwargs):
        """
        Construct a VMWareVirtualMachine instance

        Args:
            system: instance of VMWareSystem
            raw: pyVmomi.vim.VirtualMachine object
            name: name of VM
        """
        super(Pod, self).__init__(system, raw, **kwargs)
        self._name = raw.metadata.name if raw else kwargs.get('name')
        self._namespace = raw.metadata.namespace if raw else kwargs.get('namespace')
        if not self._name:
            raise ValueError("missing required kwarg 'name'")
        self.v1_pod = self.system.ocp_client.resources.get(api_version='v1', kind='Pod')

    @property
    def _identifying_attrs(self):
        return {'name': self._name}

    @property
    def name(self):
        return self._name

    @property
    def uuid(self):
        try:
            return str(self.raw.metadata.uid)
        except AttributeError:
            return self.name

    @property
    def namespace(self):
        return self._namespace

    @property
    def ip(self):
        ipv4_re = r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}'
        #self.refresh()
        try:
            return self.raw.status.podIP
        except (AttributeError):
            # AttributeError: vm doesn't have an ip address yet
            return None

    def _get_state(self):
        return self.raw.status.phase

    def is_stateful_set(self, namespace, name):
        """Checks whether passed name belongs to Stateful Sets in appropriate namespace

        Args:
            namespace: project(namespace) name
            name: entity name
        Return: True/False
        """
        return name in self.list_stateful_set_names(namespace=namespace)

    def is_deployment_config(self, namespace, name):
        """Checks whether passed name belongs to deployment configs in appropriate namespace

        Args:
            namespace: project(namespace) name
            name: entity name
        Return: True/False
        """
        return name in self.list_deployment_config_names(namespace=namespace)

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

    def start(self):
        self.logger.info("starting vm/project %s", self.name)
        if self.does_project_exist(self.name):
            for pod in self.get_required_pods(self.name):
                self.scale_entity(name=pod, namespace=self.name, replicas=1)
        else:
            raise ValueError("Project with name {n} doesn't exist".format(n=self.name))

    def stop(self):
        """Stops a vm.

                Args:
                    vm_name: name of the vm to be stopped
                Returns: whether vm action has been initiated properly
        """
        self.logger.info("stopping vm/project %s", self.name)
        if self.does_project_exist(self.name):
            for pod in self.get_required_pods(self.name):
                self.scale_entity(name=pod, namespace=self.name, replicas=0)
        else:
            raise ValueError("Project with name {n} doesn't exist".format(n=self.name))

    def restart(self):
        raise NotImplementedError

    def delete(self):
        self.v1_pod.delete(name=self.name, namespace=self.namespace)

    def refresh(self):
        self.raw = self.system.get_pod(name=self.name, namespace=self.namespace).raw
        return self.raw

    def cleanup(self):
        return self.delete()

    @property
    def creation_time(self):
        """Detect the vm_creation_time either via uptime if non-zero, or by last boot time

        The API provides no sensible way to actually get this value. The only way in which
        vcenter API MAY have this is by filtering through events

        Return tz-naive datetime object
        """
        raise NotImplementedError


@reconnect(unauthenticated_error_handler)
class Openshift(System, VmMixin, ProjectMixin):

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

    can_suspend = True
    can_pause = False

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
        self.ssl_ca_cert = kwargs.get('ssl_ca_cert', '')

        self.ociclient = ociclient

        self.k8s_client = self._k8s_client_connect()

        self.ocp_client = DynamicClient(self.k8s_client)

    def _k8s_client_connect(self):

        aToken = self.token

        url = '{proto}://{host}:{port}'.format(proto=self.protocol, host=self.hostname,
                                               port=self.port)

        aConfiguration = kubeclient.Configuration()

        aConfiguration.host = url

        # Security part.
        aConfiguration.verify_ssl = self.verify_ssl
        aConfiguration.ssl_ca_cert = self.ssl_ca_cert

        aConfiguration.api_key = {"authorization": "Bearer " + aToken}

        # Create a ApiClient with our config
        return kubeclient.ApiClient(aConfiguration)

    # def _connect(self):
    #
    #     self.dyn_client = DynamicClient(self.k8s_client)

        # self.ociclient = ociclient
        # self.kclient = kubeclient
        # self.oapi_client = ociclient.ApiClient(config=config)
        # self.kapi_client = kubeclient.ApiClient(config=config)
        # self.o_api = ociclient.OapiApi(api_client=self.oapi_client)
        # self.k_api = kubeclient.CoreV1Api(api_client=self.kapi_client)
        # self.security_api = self.ociclient.SecurityOpenshiftIoV1Api(api_client=self.oapi_client)
        # self.batch_api = self.kclient.BatchV1Api(api_client=self.kapi_client)  # for job api

    @property
    def _identifying_attrs(self):
        """
        Return a dict with key, value pairs for each kwarg that is used to
        uniquely identify this system.
        """
        return {'hostname': self.hostname, 'port': self.port}

    def info(self):
        url = '{proto}://{host}:{port}'.format(proto=self.protocol, host=self.hostname,
                                               port=self.port)
        return "rhopenshift {}".format(url)

    @cached_property
    def v1_project(self):
        return self.ocp_client.resources.get(api_version='project.openshift.io/v1', kind='Project')

    @cached_property
    def v1_pod(self):
        return self.ocp_client.resources.get(api_version='v1', kind='Pod')

    @cached_property
    def v1_route(self):
        return self.ocp_client.resources.get(api_version='route.openshift.io/v1', kind='Route')


    @property
    def can_suspend(self):
        return True

    @property
    def can_pause(self):
        return False

    def get_ocp_obj(self, resource_type, name, namespace=None):
        ocp_obj = None
        for item in getattr(self, resource_type).get(namespace=namespace).items:
            if item.metadata.name == name:
                ocp_obj = item
                break
        return ocp_obj

    def get_pod(self, name, namespace=None):
        """
        Get a VM based on name

        Passes args to find_vms to search for matches

        Args:
            name (str)
            namespace (str): Openshift namespace

        Returns:
            single PodInstance object

        Raises:
            ValueError -- no name provided
        """
        pod = self.get_ocp_obj(resource_type='Pod', name=name, namespace=namespace)

        return Pod(system=self, name=pod.metadata.name, namespace=pod.metadata.namespace, raw=pod)

    get_vm = get_pod

    def create_vm(self, name, **kwargs):
        raise NotImplementedError('This function has not yet been implemented.')

    def list_pods(self, namespace=None):
        """
         List the Pods on system. Pods are treated as 'VMs' .
         If project_name is passed, only the pods under the selected project will be returned

        Args:
            namespace (str): Openshift namespace

         Returns:
             list of wrapanapi.entities.Vm
        """
        return [
            Pod(system=self, name=pod.metadata.name, namespace=pod.metadata.namespace, raw=pod)
            for pod in self.v1_pod.get(namespace=namespace).items]

    list_vms = list_pods

    def create_project(self, name, description=None, **kwargs):

        proj = self.ociclient.V1Project()
        proj.metadata = {'name': name, 'annotations': {}}
        if description:
            proj.metadata['annotations'] = {'openshift.io/description': description}

        self.logger.info("creating new project with name %s", name)

        project = self.v1_project.create(body=proj)

        return Project(system=self, name=project.metadata.name, raw=project)

    def find_projects(self, *args, **kwargs):
        raise NotImplementedError

    def get_project(self, name):
        project = self.get_ocp_obj(resource_type='v1_project', name=name)

        return Project(system=self, name=project.metadata.name, raw=project)

    def list_project(self, namespace=None):

        return [
            Project(system=self, name=project.metadata.name, raw=project)
            for project in self.v1_project.get(namespace=namespace).items]

    def list_routes(self, namespace=None):

        return self.v1_route.get(namespace=namespace)

    # def list_image_streams(self, namespace=None):
    #
    #     return self.get_ocp_obj_list(resource_type='ImageStreamList', namespace=namespace)
    #
    # def list_image_stream_imagess(self, namespace=None):
    #
    #     return self.get_ocp_obj_list(resource_type='ImageStreamImageList', namespace=namespace)
    #
    # def list_templates(self, namespace=None):
    #     return self.get_ocp_obj_list(resource_type='Template', namespace=namespace)
    #
    # def list_deployment_config(self, namespace=None):
    #     return self.get_ocp_obj_list(resource_type='DeploymentConfig', namespace=namespace)
    #
    # def list_services(self, namespace=None):
    #     return self.get_ocp_obj_list(resource_type='Service', namespace=namespace)
    #
    # def list_replication_controller(self, namespace=None):
    #     return self.get_ocp_obj_list(resource_type='ReplicationController', namespace=namespace)
    #
    # def list_node(self, namespace=None):
    #     return self.get_ocp_obj_list(resource_type='Node', namespace=namespace)
    #
    # def list_persistent_volume(self, namespace=None):
    #     return self.get_ocp_obj_list(resource_type='PersistentVolume', namespace=namespace)
    #
    # def list_container(self, namespace=None):
    #     return self.get_ocp_obj_list(resource_type='', namespace=namespace)
    #
    # def list_image_registry(self, namespace=None):
    #     return self.get_ocp_obj_list(resource_type='', namespace=namespace)

    def find_vms(self, *args, **kwargs):
        raise NotImplementedError

