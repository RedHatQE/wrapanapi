import copy
import json
from random import choice
import string


import inflection
from collections import Iterable
from kubernetes import client as kubeclient
from kubernetes.client.rest import ApiException
from openshift import client as ociclient
from wait_for import wait_for, TimedOutError

from wrapanapi.containers.providers.rhkubernetes import Kubernetes
from wrapanapi.rest_client import ContainerClient
from wrapanapi.containers.route import Route
from wrapanapi.containers.image_registry import ImageRegistry
from wrapanapi.containers.project import Project
from wrapanapi.containers.template import Template
from wrapanapi.containers.image import Image
from wrapanapi.containers.deployment_config import DeploymentConfig

# --------------------
# TODO: remove logging when everything is done
import logging
import sys
formatter = logging.Formatter('%(asctime)s %(name)-12s %(levelname)-8s %(message)s')
stdout_handler = logging.StreamHandler(sys.stdout)
stdout_handler.setFormatter(formatter)
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
logger.addHandler(stdout_handler)
# -------------------------


class Openshift(Kubernetes):

    _stats_available = Kubernetes._stats_available.copy()
    _stats_available.update({
        'num_route': lambda self: len(self.list_route()),
        'num_template': lambda self: len(self.list_template())
    })

    existing_tags = ('HTTPD_IMG_TAG', 'ANSIBLE_IMG_TAG', 'BACKEND_APPLICATION_IMG_TAG',
                     'FRONTEND_APPLICATION_IMG_TAG', 'MEMCACHED_IMG_TAG', 'POSTGRESQL_IMG_TAG')
    default_namespace = 'openshift'
    required_project_pods = ('httpd', 'memcached', 'postgresql',
                             'cloudforms', 'cloudforms-backend')
    not_required_project_pods = ('cloudforms-backend', 'ansible')

    def __init__(self, hostname, protocol="https", port=8443, k_entry="api/v1", o_entry="oapi/v1",
                 logger=logger, debug=False, **kwargs):
        self.logger = logger
        self.hostname = hostname
        self.username = kwargs.get('username', '')
        self.password = kwargs.get('password', '')
        self.token = kwargs.get('token', '')
        self.auth = self.token if self.token else (self.username, self.password)
        self.old_k_api = self.k_api = ContainerClient(hostname, self.auth, protocol, port, k_entry)
        self.old_o_api = self.o_api = ContainerClient(hostname, self.auth, protocol, port, o_entry)
        if 'new_client' in kwargs:
            url = '{proto}://{host}:{port}'.format(proto=protocol, host=self.hostname, port=port)
            ociclient.configuration.host = url
            kubeclient.configuration.host = url

            ociclient.configuration.verify_ssl = False
            kubeclient.configuration.verify_ssl = False

            kubeclient.configuration.debug = debug
            ociclient.configuration.debug = debug

            token = 'Bearer {token}'.format(token=self.token)
            ociclient.configuration.api_key['authorization'] = token
            kubeclient.configuration.api_key['authorization'] = token
            self.ociclient = ociclient
            self.kclient = kubeclient
            self.o_api = ociclient.OapiApi()
            self.k_api = kubeclient.CoreV1Api()

        self.api = self.k_api  # default api is the kubernetes one for Kubernetes-class requests
        self.list_image_openshift = self.list_docker_image  # For backward compatibility

    def list_route(self):
        """Returns list of routes"""
        entities = []
        entities_j = self.o_api.get('route')[1]['items']
        for entity_j in entities_j:
            meta = entity_j['metadata']
            entity = Route(self, meta['name'], meta['namespace'])
            entities.append(entity)
        return entities

    def list_docker_registry(self):
        """Returns list of docker registries"""
        entities = []
        entities_j = self.o_api.get('imagestream')[1]['items']
        for entity_j in entities_j:
            if 'dockerImageRepository' not in entity_j['status']:
                continue
            meta = entity_j['metadata']
            entity = ImageRegistry(self, meta['name'],
                                   entity_j['status']['dockerImageRepository'],
                                   meta['namespace'])
            if entity not in entities:
                entities.append(entity)
        return entities

    def list_project(self):
        """Returns list of projects"""
        entities = []
        entities_j = self.o_api.get('project')[1]['items']
        for entity_j in entities_j:
            meta = entity_j['metadata']
            entity = Project(self, meta['name'])
            entities.append(entity)
        return entities

    def list_template(self):
        """Returns list of templates"""
        entities = []
        entities_j = self.o_api.get('template')[1]['items']
        for entity_j in entities_j:
            meta = entity_j['metadata']
            entity = Template(self, meta['name'], meta['namespace'])
            entities.append(entity)
        return entities

    def list_docker_image(self):
        """Returns list of images (Docker registry only)"""
        entities = []
        entities_j = self.o_api.get('image')[1]['items']
        for entity_j in entities_j:
            if 'dockerImageReference' not in entity_j:
                continue
            _, name, image_id, _ = Image.parse_docker_image_info(entity_j['dockerImageReference'])
            entities.append(Image(self, name, image_id))
        return entities

    def list_deployment_config(self):
        """Returns list of deployment configs"""
        entities = []
        entities_j = self.o_api.get('deploymentconfig')[1]['items']
        for entity_j in entities_j:
            meta = entity_j['metadata']
            entity = DeploymentConfig(self, meta['name'], meta['namespace'])
            entities.append(entity)
        return entities

    def deploy_template(self, template, base_url, tags=None, db_password='smartvm', **kwargs):
        """Deploy a VM from a template

        Args:
            template: (str) The name of the template to deploy
            tags: (dict) dict with tags if some tag isn't passed it is set to 'latest'
        Returns: dict with parameters necessary for appliance setup
        """
        # todo: move base_url to init
        self.logger.info("starting template {t} deployment".format(t=template))
        self.does_template_exist(namespace=self.default_namespace, name=template)

        db_password = db_password
        base_url = base_url

        prepared_tags = {key: 'latest' for key in self.existing_tags}
        if tags:
            not_found_tags = [tag for tag in tags.keys() if tag not in self.existing_tags]
            if not_found_tags:
                raise ValueError("Some passed tags {t} don't exist".format(t=not_found_tags))
            prepared_tags.update(tags)

        # create project
        # assuming this is cfme installation and generating project name
        proj_id = "".join(choice(string.digits + string.lowercase) for _ in range(6))
        proj_name = "{t}-project-{pid}".format(pid=proj_id, t=template)
        proj_url = "{proj_name}.{base_url}".format(proj_name=proj_name, base_url=base_url)
        self.logger.info("unique id {id}, project name {name}".format(id=proj_id,
                                                                      name=proj_name))
        self.create_project(name=proj_name)

        # grant rights according to scc
        self.logger.info("granting rights to project sa")
        scc_user_mapping = (
            {'scc': 'anyuid', 'user': 'cfme-anyuid'},
            {'scc': 'anyuid', 'user': 'cfme-orchestrator'},
            {'scc': 'anyuid', 'user': 'cfme-httpd'},
            {'scc': 'privileged', 'user': 'cfme-privileged'},
        )

        self.logger.info("granting required rights to project's service accounts")
        security_api = self.ociclient.SecurityOpenshiftIoV1Api()
        for mapping in scc_user_mapping:
            old_scc = security_api.read_security_context_constraints(name=mapping['scc'])
            got_users = old_scc.users if old_scc.users else []
            got_users.append('system:serviceaccount:{proj}:{usr}'.format(proj=proj_name,
                                                                         usr=mapping['user']))
            security_api.patch_security_context_constraints(name=mapping['scc'],
                                                            body={'users': got_users})

        # grant roles to orchestrator
        self.logger.info("assigning additional roles to cfme-orchestrator")
        auth_api = self.ociclient.AuthorizationOpenshiftIoV1Api()
        orchestrator_sa = self.kclient.V1ObjectReference(name='cfme-orchestrator',
                                                         kind='ServiceAccount',
                                                         namespace=proj_name)

        view_role = self.kclient.V1ObjectReference(name='view')
        view_role_binding_name = self.kclient.V1ObjectMeta(name='view')
        view_role_binding = self.ociclient.V1RoleBinding(role_ref=view_role,
                                                         subjects=[orchestrator_sa],
                                                         metadata=view_role_binding_name)
        auth_api.create_namespaced_role_binding(namespace=proj_name, body=view_role_binding)

        edit_role = self.kclient.V1ObjectReference(name='edit')
        edit_role_binding_name = self.kclient.V1ObjectMeta(name='edit')
        edit_role_binding = self.ociclient.V1RoleBinding(role_ref=edit_role,
                                                         subjects=[orchestrator_sa],
                                                         metadata=edit_role_binding_name)
        auth_api.create_namespaced_role_binding(namespace=proj_name, body=edit_role_binding)

        self.logger.info("project sa created via api have no some mandatory roles. adding them")
        self._restore_missing_project_role_bindings(proj_name=proj_name)

        # creating pods and etc
        processing_params = {'DATABASE_PASSWORD': db_password,
                             'APPLICATION_DOMAIN': proj_url}
        processing_params.update(prepared_tags)
        self.logger.info(("processing template and passed params in order to "
                          "prepare list of required project entities"))
        template_entities = self.process_template(name=template, namespace=self.default_namespace,
                                                  parameters=processing_params)
        self.logger.debug("template entities:\n {e}".format(e=template_entities))
        kinds = set([e['kind'] for e in template_entities])
        entity_names = {e: inflection.underscore(e) for e in kinds}
        proc_names = {k: 'create_{e}'.format(e=p) for k, p in entity_names.items()}
        for entity in template_entities:
            if entity['kind'] in kinds:
                procedure = getattr(self, proc_names[entity['kind']], None)
                # todo: this code should be parallelized
                obtained_entity = procedure(namespace=proj_name, **entity)
                self.logger.debug(obtained_entity)
            else:
                self.logger.error("some entity %s isn't present in entity creation list", entity)

        # todo: test that everything is running
        self.logger.info("verifying that all created entities are up and running")

        common_svc = self.k_api.read_namespaced_service(name='common-service',
                                                        namespace=proj_name)
        ext_ip = common_svc.spec.external_i_ps[0]

        return {'url': proj_url,
                'external_ip': ext_ip,
                'project': proj_name,
                }

    def start_vm(self, vm_name):
        """Starts a vm.

        Args:
            vm_name: name of the vm to be started
        Returns: whether vm action has been initiated properly
        """
        self.logger.info("starting vm/project %s", vm_name)
        if self.does_project_exist(vm_name):
            for pod in self.required_project_pods:
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
            for pod in self.required_project_pods:
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
    def update_template_parameters(template, **params):
        """
        :param template:
        :param params:
        :return:
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
        # workaround for bug https://github.com/openshift/openshift-restclient-python/issues/60
        raw_response = self.o_api.read_namespaced_template(name=name, namespace=namespace,
                                                           _preload_content=False)
        raw_data = json.loads(raw_response.data)
        updated_data = self.rename_structure(raw_data)
        read_template = self.ociclient.V1Template(**updated_data)
        if parameters:
            updated_template = self.update_template_parameters(template=read_template, **parameters)
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
        if not isinstance(struct, (str, unicode)) and isinstance(struct, Iterable):
            if isinstance(struct, dict):
                for key in struct.keys():
                    # we shouldn't rename something under data or spec
                    if key == 'stringData':
                        # this key has to be renamed but its contents should be left intact
                        struct[inflection.underscore(key)] = struct.pop(key)
                    elif key in ('spec', 'data', 'string_data'):
                        # these keys and data should be left intact
                        pass
                    else:
                        # all this data should be processed and updated
                        val = self.rename_structure(struct.pop(key))
                        struct[inflection.underscore(key)] = val
                return struct
            else:
                for item in struct:
                    self.rename_structure(item)
                return struct
        else:
            return struct

    def create_config_map(self, namespace, **kwargs):
        conf_map = self.kclient.V1ConfigMap(**kwargs)
        conf_map_name = conf_map.to_dict()['metadata']['name']
        self.logger.info("creating config map %s", conf_map_name)
        output = self.k_api.create_namespaced_config_map(namespace=namespace, body=conf_map)
        self.does_config_map_exist(namespace=namespace, name=conf_map_name)
        return output

    def create_stateful_set(self, namespace, **kwargs):
        st = self.kclient.V1beta1StatefulSet(**kwargs)
        st_name = st.to_dict()['metadata']['name']
        self.logger.info("creating stateful set %s", st_name)
        output = self.kclient.AppsV1beta1Api().create_namespaced_stateful_set(namespace=namespace,
                                                                              body=st)
        self.does_stateful_set_exist(namespace=namespace, name=st_name)
        return output

    def create_service(self, namespace, **kwargs):
        service = self.kclient.V1Service(**kwargs)
        service_name = service.to_dict()['metadata']['name']
        self.logger.info("creating service %s", service_name)
        output = self.k_api.create_namespaced_service(namespace=namespace, body=service)
        self.does_service_exist(namespace=namespace, name=service_name)
        return output

    def create_route(self, namespace, **kwargs):
        route = self.ociclient.V1Route(**kwargs)
        route_name = route.to_dict()['metadata']['name']
        self.logger.info("creating route %s", route_name)
        output = self.o_api.create_namespaced_route(namespace=namespace, body=route)
        self.does_route_exist(namespace=namespace, name=route_name)
        return output

    def create_service_account(self, namespace, **kwargs):
        sa = self.kclient.V1ServiceAccount(**kwargs)
        sa_name = sa.to_dict()['metadata']['name']
        self.logger.info("creating service account %s", sa_name)
        output = self.k_api.create_namespaced_service_account(namespace=namespace, body=sa)
        self.does_service_account_exist(namespace=namespace, name=sa_name)
        return output

    def create_secret(self, namespace, **kwargs):
        secret = self.kclient.V1Secret(**kwargs)
        secret_name = secret.to_dict()['metadata']['name']
        self.logger.info("creating secret %s", secret_name)
        output = self.k_api.create_namespaced_secret(namespace=namespace, body=secret)
        self.does_secret_exist(namespace=namespace, name=secret_name)
        return output

    def create_deployment_config(self, namespace, **kwargs):
        dc = self.ociclient.V1DeploymentConfig(**kwargs)
        dc_name = dc.to_dict()['metadata']['name']
        self.logger.info("creating deployment config %s", dc_name)
        output = self.o_api.create_namespaced_deployment_config(namespace=namespace, body=dc)
        self.does_deployment_config_exist_and_alive(namespace=namespace,
                                                    name=dc_name)
        return output

    def create_persistent_volume_claim(self, namespace, **kwargs):
        pv_claim = self.kclient.V1PersistentVolumeClaim(**kwargs)
        pv_claim_name = pv_claim.to_dict()['metadata']['name']
        self.logger.info("creating persistent volume claim %s", pv_claim_name)
        output = self.k_api.create_namespaced_persistent_volume_claim(namespace=namespace,
                                                                      body=pv_claim)
        self.does_persistent_volume_claim_exist(namespace=namespace,
                                                name=pv_claim_name)
        return output

    def create_project(self, name):
        proj = self.ociclient.V1Project()
        proj.metadata = {'name': name}
        self.logger.info("creating new project with name {n}".format(n=name))
        output = self.o_api.create_project(body=proj)
        self.does_project_exist(name=name)
        return output

    def does_project_exist(self, name, wait=5):
        return wait_for(self._does_exist, num_sec=wait,
                        func_kwargs={'func': self.o_api.read_project, 'name': name})[0]

    def does_config_map_exist(self, namespace, name, wait=30):
        return wait_for(self._does_exist, num_sec=wait,
                        func_kwargs={'func': self.k_api.read_namespaced_config_map,
                                     'name': name,
                                     'namespace': namespace})[0]

    def does_stateful_set_exist(self, namespace, name, wait=600):
        read_st = self.kclient.AppsV1beta1Api().read_namespaced_stateful_set
        return wait_for(self._does_exist, num_sec=wait,
                        func_kwargs={'func': read_st,
                                     'name': name,
                                     'namespace': namespace})[0]

    def does_service_exist(self, namespace, name, wait=30):
        return wait_for(self._does_exist, num_sec=wait,
                        func_kwargs={'func': self.k_api.read_namespaced_service,
                                     'name': name,
                                     'namespace': namespace})[0]

    def does_route_exist(self, namespace, name, wait=30):
        return wait_for(self._does_exist, num_sec=wait,
                        func_kwargs={'func': self.o_api.read_namespaced_route,
                                     'name': name,
                                     'namespace': namespace})[0]

    def does_service_account_exist(self, namespace, name, wait=30):
        return wait_for(self._does_exist, num_sec=wait,
                        func_kwargs={'func': self.k_api.read_namespaced_service_account,
                                     'name': name,
                                     'namespace': namespace})[0]

    def does_secret_exist(self, namespace, name, wait=30):
        return wait_for(self._does_exist, num_sec=wait,
                        func_kwargs={'func': self.k_api.read_namespaced_secret,
                                     'name': name,
                                     'namespace': namespace})[0]

    def does_persistent_volume_claim_exist(self, namespace, name, wait=30):
        return wait_for(self._does_exist, num_sec=wait,
                        func_kwargs={'func': self.k_api.read_namespaced_persistent_volume_claim,
                                     'name': name,
                                     'namespace': namespace})[0]

    def does_deployment_config_exist_and_alive(self, namespace, name, wait=600):
        read_dc = self.o_api.read_namespaced_deployment_config
        return wait_for(self._does_exist, num_sec=wait,
                        func_kwargs={'func': read_dc,
                                     'name': name,
                                     'namespace': namespace})[0]

    def does_template_exist(self, namespace, name, wait=5):
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

    def _restore_missing_project_role_bindings(self, proj_name):
        # create project doesn't add necessary roles to default sa, probably bug, this is workaround
        # adding builder role binding
        auth_api = self.ociclient.AuthorizationOpenshiftIoV1Api()
        builder_role = self.kclient.V1ObjectReference(name='system:image-builder')
        builder_sa = self.kclient.V1ObjectReference(name='builder',
                                                    kind='ServiceAccount',
                                                    namespace=proj_name)
        builder_role_binding_name = self.kclient.V1ObjectMeta(name='builder-binding')
        builder_role_binding = self.ociclient.V1RoleBinding(role_ref=builder_role,
                                                            subjects=[builder_sa],
                                                            metadata=builder_role_binding_name)
        auth_api.create_namespaced_role_binding(namespace=proj_name, body=builder_role_binding)

        # adding deployer role binding
        deployer_role = self.kclient.V1ObjectReference(name='system:deployer')
        deployer_sa = self.kclient.V1ObjectReference(name='deployer',
                                                     kind='ServiceAccount',
                                                     namespace=proj_name)
        deployer_role_binding_name = self.kclient.V1ObjectMeta(name='deployer-binding')
        deployer_role_binding = self.ociclient.V1RoleBinding(role_ref=deployer_role,
                                                             subjects=[deployer_sa],
                                                             metadata=deployer_role_binding_name)
        auth_api.create_namespaced_role_binding(namespace=proj_name, body=deployer_role_binding)

        # adding admin role binding
        admin_role = self.kclient.V1ObjectReference(name='admin')
        admin_user = self.kclient.V1ObjectReference(name='admin',
                                                    kind='User',
                                                    namespace=proj_name)
        admin_role_binding_name = self.kclient.V1ObjectMeta(name='admin-binding')
        admin_role_binding = self.ociclient.V1RoleBinding(role_ref=admin_role,
                                                          subjects=[admin_user],
                                                          metadata=admin_role_binding_name)
        auth_api.create_namespaced_role_binding(namespace=proj_name, body=admin_role_binding)

        # adding image-puller role binding
        puller_role = self.kclient.V1ObjectReference(name='system:image-puller')
        group_name = 'system:serviceaccounts:{proj}'.format(proj=proj_name)
        puller_group = self.kclient.V1ObjectReference(name=group_name,
                                                      kind='SystemGroup',
                                                      namespace=proj_name)
        role_binding_name = self.kclient.V1ObjectMeta(name='image-puller-binding')
        puller_role_binding = self.ociclient.V1RoleBinding(role_ref=puller_role,
                                                           subjects=[puller_group],
                                                           metadata=role_binding_name)
        auth_api.create_namespaced_role_binding(namespace=proj_name, body=puller_role_binding)

    def delete_project(self, name, wait=120):
        self.logger.info("removing project %s", name)
        if self.does_project_exist(name=name):
            self.o_api.delete_project(name=name)
            if not self.does_project_exist(name=name, wait=wait):
                raise TimedOutError('project {n} was not removed within {w} sec'.format(n=name,
                                                                                        w=wait))

    def scale_entity(self, namespace, name, replicas, wait=60):
        # only dc and statefulsets can be scaled
        dcs = self.o_api.list_namespaced_deployment_config(namespace=namespace)
        dc_names = [dc.metadata.name for dc in dcs.items]

        st_api = self.kclient.AppsV1beta1Api()
        sts = st_api.list_namespaced_stateful_set(namespace=namespace)
        st_names = [st.metadata.name for st in sts.items]

        scale_val = self.kclient.V1Scale(spec=self.kclient.V1ScaleSpec(replicas=replicas))
        if name in dc_names:
            self.o_api.patch_namespaced_deployment_config_scale(name=name, namespace=namespace,
                                                                body=scale_val)

            def check_scale_value():
                got_scale = self.o_api.read_namespaced_deployment_config_scale(name=name,
                                                                               namespace=namespace)
                return 0 if got_scale.spec.replicas is None else int(got_scale.spec.replicas)

        elif name in st_names:
            # replace this code with stateful_set_scale when kubernetes shipped with openshift
            # gets upgraded
            st_spec = self.kclient.V1beta1StatefulSetSpec
            st = self.kclient.V1beta1StatefulSet(spec=st_spec(replicas=replicas))
            st_api.patch_namespaced_stateful_set(name=name, namespace=namespace, body=st)

            def check_scale_value():
                got_scale = st_api.read_namespaced_stateful_set(name=name, namespace=namespace)
                return 0 if got_scale.status.replicas is None else int(got_scale.spec.replicas)
        else:
            raise ValueError("This name is not found among deployment configs or stateful sets")
        self.logger.info("scaling entity %s to %s replicas", name, replicas)
        wait_for(check_scale_value, num_sec=wait, fail_condition=lambda val: val != replicas)

    def get_project_by_name(self, project_name):
        """Returns only the selected Project object"""
        return Project(self, project_name)
