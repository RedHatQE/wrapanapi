# -*- coding: utf-8 -*-
"""Unit tests for Openshift client."""
import os
from random import choice

import pytest
import mock
import fauxfactory
from wait_for import wait_for

from wrapanapi.containers.providers import openshift
from wrapanapi.containers.project import Project
from wrapanapi.containers.deployment_config import DeploymentConfig
from wrapanapi.containers.image_registry import ImageRegistry
from wrapanapi.containers.image import Image
from wrapanapi.containers.pod import Pod
from wrapanapi.containers.service import Service
from wrapanapi.containers.node import Node
from wrapanapi.containers.replicator import Replicator
from wrapanapi.containers.route import Route
from wrapanapi.containers.template import Template
from wrapanapi.containers.volume import Volume

from wrapanapi.exceptions import InvalidValueException, RequestFailedException


# Specify whether to use a mock provider or real one.
MOCKED = os.environ.get('MOCKED', 'true').lower() == 'true'
# If you prefer to use a real provider, provide HOSTNAME, USERNAME and TOKEN
HOSTNAME = os.environ.get('HOSTNAME')
USERNAME = os.environ.get('USERNAME')
TOKEN = os.environ.get('TOKEN')

FIXTURES_SCOPES = ('function' if MOCKED else 'module')


@pytest.fixture(scope=FIXTURES_SCOPES)
def provider():
    if MOCKED:
        ocp = openshift.Openshift('openshift.test.com', username='default')
        with mock.patch('wrapanapi.rest_client.ContainerClient') as client:
            ocp.o_api = ocp.api = ocp.k_api = client
    else:
        return openshift.Openshift(HOSTNAME, username=USERNAME, token=TOKEN)
    return ocp


def gen_docker_image_reference():
    """Generating a docker image reference including image ID.
    returns the docker image reference and image ID"""
    image_id = 'sha256:some-long-fake-id-with-numbers-{}'
    docker_image_refrence = 'this.is.some.fake.{}/registry:{}@{}'.format(
        fauxfactory.gen_alpha().lower(), fauxfactory.gen_numeric_string(3), image_id)
    return docker_image_refrence, image_id


def mocked_image_data():
    out = [200, {'items': []}]
    for i in range(fauxfactory.gen_integer(2, 20)):
        dockerImageReference, imageID = gen_docker_image_reference()
        out[1]['items'].append({
            'metadata': {
                'name': 'mockedimage{}'.format(i),
                'namespace': choice(('default', 'openshift-infra', 'kube-system'))
            }
        })
        out[1]['items'][-1]['dockerImageReference'] = \
            dockerImageReference.format(fauxfactory.gen_numeric_string())
        out[1]['items'][-1]['status'] = {
            'dockerImageRepository': dockerImageReference,
            'containerStatuses': [
                {
                    'image': out[1]['items'][-1]['dockerImageReference'],
                    'imageID': imageID.format(fauxfactory.gen_numeric_string(64))
                }
                for _ in range(fauxfactory.gen_integer(2, 20))
            ]
        }
    return out


@pytest.fixture(scope=FIXTURES_SCOPES)
def gen_project(provider):
    return Project(provider, fauxfactory.gen_alpha().lower())


@pytest.fixture(scope=FIXTURES_SCOPES)
def gen_image(provider):
    if MOCKED:
        return Image(provider, 'some.test.image', 'sha256:{}'
                     .format(fauxfactory.gen_alphanumeric(64)))
    return choice(provider.list_docker_image())


@pytest.fixture(scope=FIXTURES_SCOPES)
def gen_pod(provider):
    if MOCKED:
        return Pod(provider, 'some-test-pod', 'default')
    return choice(provider.list_container_group())


@pytest.fixture(scope=FIXTURES_SCOPES)
def gen_service(provider):
    if MOCKED:
        return Service(provider, 'some-test-service', 'default')
    return choice(provider.list_service())


@pytest.fixture(scope=FIXTURES_SCOPES)
def gen_node(provider):
    if MOCKED:
        return Node(provider, 'openshift-node.test.com')
    return choice(provider.list_node())


@pytest.fixture(scope=FIXTURES_SCOPES)
def gen_replicator(provider):
    if MOCKED:
        return Replicator(provider, 'some-test-replicator', 'default')
    return choice(provider.list_replication_controller())


@pytest.fixture(scope=FIXTURES_SCOPES)
def gen_route(provider):
    if MOCKED:
        return Route(provider, 'some.test.route.com', 'default')
    return choice(provider.list_route())


@pytest.fixture(scope=FIXTURES_SCOPES)
def gen_template(provider):
    if MOCKED:
        return Template(provider, 'some-test-template', 'default')
    return choice(provider.list_template())


@pytest.fixture(scope=FIXTURES_SCOPES)
def gen_image_registry(provider):
    return ImageRegistry(provider, 'openshift-hello-openshift',
                        'docker.io/openshift/hello-openshift', 'default')


@pytest.fixture(scope=FIXTURES_SCOPES)
def gen_volume(provider):
    if MOCKED:
        return Volume(provider, 'my-test-persistent-volume')
    return choice(provider.list_volume())


@pytest.fixture(scope=FIXTURES_SCOPES)
def gen_dc(provider):
    return DeploymentConfig(provider, fauxfactory.gen_alpha().lower(),
                            'default', 'openshift/hello-openshift', 1)


@pytest.fixture(scope=FIXTURES_SCOPES)
def label():
    return (fauxfactory.gen_alpha().lower(), fauxfactory.gen_alpha().lower())


def base__test_label_create(resource, label_key, label_value):
    if MOCKED:
        resource.provider.api.patch.return_value = \
            resource.provider.o_api.patch.return_value = [201, {}]
        resource.provider.api.get.return_value = \
            resource.provider.o_api.get.return_value = [200, {
                'metadata': {'labels': {label_key: label_value}}}]
    res = resource.set_label(label_key, label_value)
    assert res[0] in (200, 201)
    assert wait_for(lambda: label_key in resource.list_labels(),
                    message="Waiting for label {} of {} {} to exist..."
                    .format(label_key, type(resource).__name__, resource.name),
                    delay=5, timeout='1M').out


def base__test_label_delete(resource, label_key):
    if MOCKED:
        resource.provider.api.patch.return_value = \
            resource.provider.o_api.patch.return_value = [200, {}]
        resource.provider.api.get.return_value = \
            resource.provider.o_api.get.return_value = [200, {
                'metadata': {'labels': {label_key: 'doesntmatter'}}}]
    res = resource.delete_label(label_key)
    assert res[0] == 200
    if MOCKED:
        resource.provider.api.get.return_value = \
            resource.provider.o_api.get.return_value = [200, {
                'metadata': {'labels': {}}}]
    assert wait_for(lambda: label_key not in resource.list_labels(),
                    message="Waiting for label {} of {} {} to be deleted..."
                    .format(label_key, type(resource).__name__, resource.name),
                    delay=5, timeout='1M').out


@pytest.mark.incremental
class TestProject(object):
    def test_list(self, provider):
        if MOCKED:
            provider.o_api.get.return_value = [200, {
                'items': [
                    {'metadata': {'name': 'mockedprject{}'.format(i)}}
                    for i in range(fauxfactory.gen_integer(2, 20))
                ]
            }]
        assert all([isinstance(inst, Project) for inst in provider.list_project()])

    def test_project_create(self, provider, gen_project):
        if MOCKED:
            provider.api.post.return_value = [201, {
                "apiVersion": "v1", "kind": "Project", "metadata": {"name": gen_project.name}}]
            provider.api.get.return_value = [200, {}]
        gen_project.create()
        assert wait_for(lambda: gen_project.exists(),
                        message="Waiting for project {} to be created..."
                                .format(gen_project.name),
                        delay=5, timeout='1M')

    def test_labels_create(self, provider, gen_project, label):
        base__test_label_create(gen_project, label[0], label[1])

    def test_labels_delete(self, provider, gen_project, label):
        base__test_label_delete(gen_project, label[0])

    def test_project_delete(self, provider, gen_project):
        if MOCKED:
            provider.o_api.delete.return_value = provider.api.delete.return_value = [200, {}]
            provider.o_api.get.side_effect = provider.api.get.side_effect = \
                RequestFailedException('Request Failed')
        res = gen_project.delete()
        assert res[0] == 200
        assert wait_for(lambda: not gen_project.exists(),
                        message="Waiting for project {} to be deleted..."
                        .format(gen_project.name),
                        delay=5, timeout='1M')

    def test_invalid_name(self):
        with pytest.raises(InvalidValueException):
            Project(provider, 'this_is_invalid_project_name')
            Project(provider, 'this/is/invalid/project/name/as/well')


@pytest.mark.incremental
class TestImage(object):
    def test_list(self, provider):
        if MOCKED:
            provider.o_api.get.return_value = provider.api.get.return_value = mocked_image_data()
        assert all([isinstance(inst, Image) for inst in provider.list_docker_image()])
        assert all([isinstance(inst, Image) for inst in provider.list_image()])
        assert all([isinstance(inst, Image) for inst in provider.list_image_openshift()])

    def test_labels_create(self, provider, gen_image, label):
        base__test_label_create(gen_image, label[0], label[1])

    def test_labels_delete(self, provider, gen_image, label):
        base__test_label_delete(gen_image, label[0])

    def test_properties(self, gen_image):
        # Just test that there are no errors when we try to get properties
        if MOCKED:
            gen_image.api.get.return_value = [200, {
                'dockerImageReference': 'this.is.some.fake/registry:{}'
                '@sha256:some-long-fake-id-with-numbers-{}'
                .format(fauxfactory.gen_numeric_string(3), fauxfactory.gen_numeric_string(64))
            }]
        gen_image.registry, gen_image.tag


@pytest.mark.incremental
class TestPod(object):
    def test_labels_create(self, provider, gen_pod, label):
        base__test_label_create(gen_pod, label[0], label[1])

    def test_labels_delete(self, provider, gen_pod, label):
        base__test_label_delete(gen_pod, label[0])

    def test_properties(self, gen_pod):
        # Just test that there are no errors when we try to get properties
        if MOCKED:
            gen_pod.provider.api.get.return_value = [200, {
                'spec': {
                    'restartPolicy': 'Always',
                    'dnsPolicy': 'Sometimes'
                }
            }]
        gen_pod.restart_policy, gen_pod.dns_policy


@pytest.mark.incremental
class TestService(object):
    def test_labels_create(self, provider, gen_service, label):
        base__test_label_create(gen_service, label[0], label[1])

    def test_labels_delete(self, provider, gen_service, label):
        base__test_label_delete(gen_service, label[0])

    def test_properties(self, gen_service):
        # Just test that there are no errors when we try to get properties
        if MOCKED:
            gen_service.provider.api.get.return_value = [200, {
                'spec': {
                    'sessionAffinity': 'ClientIP',
                    'clusterIP': '127.0.0.1'
                }
            }]
        gen_service.portal_ip, gen_service.session_affinity


@pytest.mark.incremental
class TestRoute(object):
    def test_labels_create(self, provider, gen_route, label):
        base__test_label_create(gen_route, label[0], label[1])

    def test_labels_delete(self, provider, gen_route, label):
        base__test_label_delete(gen_route, label[0])


@pytest.mark.incremental
class TestNode(object):
    def test_labels_create(self, provider, gen_node, label):
        base__test_label_create(gen_node, label[0], label[1])

    def test_labels_delete(self, provider, gen_node, label):
        base__test_label_delete(gen_node, label[0])

    def test_properties(self, gen_node):
        # Just test that there are no errors when we try to get properties
        if MOCKED:
            gen_node.provider.api.get.return_value = [200, {
                'status': {
                    'capacity': {
                        'cpu': fauxfactory.gen_integer(1, 8),
                        'memory': '{}kb'.format(fauxfactory.gen_numeric_string())
                    },
                    'conditions': [{'status': 'Running'}]
                }
            }]
        gen_node.cpu, gen_node.ready, gen_node.memory


@pytest.mark.incremental
class TestReplicator(object):
    def test_labels_create(self, provider, gen_replicator, label):
        base__test_label_create(gen_replicator, label[0], label[1])

    def test_labels_delete(self, provider, gen_replicator, label):
        base__test_label_delete(gen_replicator, label[0])

    def test_properties(self, gen_replicator):
        # Just test that there are no errors when we try to get properties
        if MOCKED:
            replicas = fauxfactory.gen_integer(1, 50)
            gen_replicator.provider.api.get.return_value = [200, {
                'spec': {'replicas': replicas},
                'status': {'replicas': replicas}
            }]
        gen_replicator.replicas, gen_replicator.current_replicas


@pytest.mark.incremental
class TestTemplate(object):
    def test_labels_create(self, provider, gen_replicator, label):
        base__test_label_create(gen_replicator, label[0], label[1])

    def test_labels_delete(self, provider, gen_replicator, label):
        base__test_label_delete(gen_replicator, label[0])


@pytest.mark.incremental
class TestDeploymentConfig(object):
    def test_list(self, provider):
        if MOCKED:
            provider.api.post.return_value = provider.o_api.get.return_value = [200, {
                'items': [
                    {
                        'metadata': {
                            'name': fauxfactory.gen_alphanumeric(),
                            'namespace': choice(('default', 'openshift-infra', 'kube-system'))
                        },
                        'spec': {
                            'template': {'spec': {'containers': [
                                {'image': 'img{}'.format(i)}
                                for i in range(fauxfactory.gen_integer(1, 10))
                            ]}},
                            'replicas': fauxfactory.gen_integer(1, 50)
                        }
                    }
                    for _ in range(fauxfactory.gen_integer(1, 30))
                ]
            }]
        assert all([isinstance(inst, DeploymentConfig)
                    for inst in provider.list_deployment_config()])

    def test_dc_create(self, provider, gen_dc):
        if MOCKED:
            provider.o_api.post.return_value = [201, {}]
            provider.o_api.get.return_value = [200, {}]
        res = gen_dc.create()
        assert res[0] in (200, 201)
        assert wait_for(lambda: gen_dc.exists(),
                        message="Waiting for dc {} to exist..."
                        .format(gen_dc.name),
                        delay=5, timeout='1M')

    def test_labels_create(self, provider, gen_dc, label):
        base__test_label_create(gen_dc, label[0], label[1])

    def test_labels_delete(self, provider, gen_dc, label):
        base__test_label_delete(gen_dc, label[0])

    def test_dc_delete(self, provider, gen_dc):
        if MOCKED:
            provider.o_api.delete.return_value = [200, {}]
            provider.o_api.get.side_effect = RequestFailedException('Request Failed')
        res = gen_dc.delete()
        assert res[0] == 200
        assert wait_for(lambda: not gen_dc.exists(),
                        message="Waiting for dc {} to be deleted..."
                        .format(gen_dc.name),
                        delay=5, timeout='1M')

    def test_invalid_name(self):
        with pytest.raises(InvalidValueException):
            DeploymentConfig(provider, 'this_is_invalid_dc_name', '', '', 0)
            DeploymentConfig(provider, 'this/is/invalid/dc/name/as/well', '', '', 0)


@pytest.mark.incremental
class TestImageRegistry(object):
    def test_list(self, provider):
        if MOCKED:
            provider.o_api.get.return_value = provider.api.get.return_value = mocked_image_data()
        assert all([isinstance(inst, ImageRegistry)
                    for inst in provider.list_image_registry()])
        assert all([isinstance(inst, ImageRegistry)
                    for inst in provider.list_docker_registry()])

    def test_import_image(self, provider, gen_image_registry):
        if MOCKED:
            provider.o_api.post.return_value = [200, {
                'status': {
                    'images': [{'image': {'dockerImageReference': gen_docker_image_reference()[0]}}]
                }
            }]
            provider.o_api.get.return_value = provider.o_api.delete.return_value = [200, {}]
        image = gen_image_registry.import_image()
        assert image.exists()
        image.delete()

    def test_labels_create(self, provider, gen_image_registry, label):
        base__test_label_create(gen_image_registry, label[0], label[1])

    def test_labels_delete(self, provider, gen_image_registry, label):
        base__test_label_delete(gen_image_registry, label[0])

    def test_invalid_name(self):
        with pytest.raises(InvalidValueException):
            ImageRegistry(provider, 'this/is/invalid/name',
                          'docker.io/openshift/hello-openshift', 'default')


@pytest.mark.incremental
class TestVolume(object):
    def test_labels_create(self, provider, gen_volume, label):
        base__test_label_create(gen_volume, label[0], label[1])

    def test_labels_delete(self, provider, gen_volume, label):
        base__test_label_delete(gen_volume, label[0])

    def test_properties(self, gen_volume):
        # Just test that there are no errors when we try to get properties
        if MOCKED:
            gen_volume.api.get.return_value = [200, {
                'spec': {
                    'capacity': {'storage': '5Gib'},
                    'accessModes': ['ReadOnlyMany']
                }
            }]
        gen_volume.capacity, gen_volume.accessmodes
