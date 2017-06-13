# -*- coding: utf-8 -*-
"""Unit tests for Hawkular client."""
import json
from urlparse import urlparse

import os
import pytest
from wrapanapi import hawkular
from mock import patch
from random import sample
from wrapanapi.hawkular import CanonicalPath


def fake_urlopen(c_client, url, headers, params):
    """
    A stub urlopen() implementation that load json responses from
    the filesystem.
    """
    # Map path from url to a file
    parsed_url = urlparse("{}/{}".format(c_client.api_entry, url)).path
    if parsed_url.startswith('/hawkular/inventory/traversal') \
            or parsed_url.startswith('/hawkular/inventory/entity'):
        # Change parsed url, when we use default one, 'd;configuration' replaced with 'd'
        parsed_url = "{}/{}".format(urlparse("{}".format(c_client.api_entry)).path, url)
        parsed_url = parsed_url.replace('traversal/', '')
        parsed_url = parsed_url.replace('entity/', '')
        parsed_url = parsed_url.replace('f;', 'feeds/')
        parsed_url = parsed_url.replace('r;', 'resources/', 1)
        parsed_url = parsed_url.replace('r;', '')
        parsed_url = parsed_url.replace('rt;', 'resourceTypes/')
        parsed_url = parsed_url.replace('rl;defines/', '')
        parsed_url = parsed_url.replace('type=rt', 'resourceTypes')
        parsed_url = parsed_url.replace('type=r', 'resources')
        parsed_url = parsed_url.replace('type=f', 'feeds')
        parsed_url = parsed_url.replace('d;configuration', 'data')
    resource_file = os.path.normpath("tests/resources/{}.json".format(parsed_url))
    # Must return a file-like object
    return json.load(open(resource_file))


def fake_urldelete(c_client, url, headers):
    """
    A stub delete_status() implementation that returns True
    """
    return True


def fake_urlput(c_client, url, data, headers):
    """
    A stub put_status() implementation that returns True
    """
    return True


def fake_urlpost(c_client, url, data, headers):
    """
    A stub post_status() implementation that returns True
    """
    return True


@pytest.yield_fixture(scope="function")
def provider():
    """
    A stub urlopen() implementation that load json responses from
    the filesystem.
    """
    if not os.getenv('HAWKULAR_HOSTNAME'):
        patcher = patch('wrapanapi.rest_client.ContainerClient.get_json', fake_urlopen)
        patcher.start()
        patcher = patch('wrapanapi.rest_client.ContainerClient.delete_status', fake_urldelete)
        patcher.start()
        patcher = patch('wrapanapi.rest_client.ContainerClient.post_status', fake_urlpost)
        patcher.start()
        patcher = patch('wrapanapi.rest_client.ContainerClient.put_status', fake_urlput)
        patcher.start()

    hwk = hawkular.Hawkular(
        hostname=os.getenv('HAWKULAR_HOSTNAME', 'localhost'),
        protocol=os.getenv('HAWKULAR_PROTOCOL', 'http'),
        port=os.getenv('HAWKULAR_PORT', 8080),
        username=os.getenv('HAWKULAR_USERNAME', 'jdoe'),
        password=os.getenv('HAWKULAR_PASSWORD', 'password'),
        ws_connect=False
    )
    yield hwk
    if not os.getenv('HAWKULAR_HOSTNAME'):
        patcher.stop()


@pytest.yield_fixture(scope="function")
def datasource(provider):
    """
    Fixture for preparing Datasource for tests.
    It creates resource and resource data for Datasource.
    On the end of testing, Datasource is deleted.
    """
    datasources = provider.inventory.list_server_datasource()
    assert len(datasources) > 0, "No resource data is listed for any of datasources"
    new_datasource = None
    for datasource in sample(datasources, 1):
        r_data = _read_resource_data(provider, datasource)
        assert r_data

        name_ext = "MWTest"
        new_datasource = hawkular.Resource(name="{}{}".format(datasource.name, name_ext),
                                id="{}{}".format(datasource.id, name_ext),
                                path=hawkular.CanonicalPath(
                                    "{}{}".format(datasource.path.to_string, name_ext)))
        new_datasource.path.resource_id = new_datasource.path.resource_id[1]
        resource_type = hawkular.ResourceType(id=None, name=None,
                                              path=CanonicalPath("/rt;Datasource"))

        new_datasource_data = hawkular.ResourceData(name=None, path=None, value=r_data.value)
        new_datasource_data.value.update(
            {"JNDI Name": "{}{}".format(r_data.value["JNDI Name"], name_ext),
             "Enabled": "true"
             }
        )
        _delete_resource(provider, new_datasource)
        result = _create_resource(provider, resource=new_datasource,
                                  resource_data=new_datasource_data, resource_type=resource_type)
        assert result, "Create should be successful"
        r_data = _read_resource_data(provider, new_datasource)
        assert r_data, "Resource data should exist"
        assert r_data.value == new_datasource_data.value
    yield new_datasource
    if new_datasource:
        _delete_resource(provider, new_datasource)


def test_list_feed(provider):
    """ Checks whether any feed is listed """
    feeds = provider.inventory.list_feed()
    assert len(feeds) > 0, "No feeds are listed"
    for feed in feeds:
        assert feed.id
        assert feed.path


def test_list_resource_type(provider):
    """ Checks whether any resource type is listed and has attributes """
    feeds = provider.inventory.list_feed()
    for feed in feeds:
        res_types = provider.inventory.list_resource_type(feed_id=feed.id)
        for res_type in res_types:
            assert res_type.id
            assert res_type.name
            assert res_type.path
    assert len(res_types) > 0, "No resource type is listed for any of feeds"


def test_list_server(provider):
    """ Checks whether any server is listed and has attributes"""
    servers = provider.inventory.list_server()
    for server in servers:
        assert server.id
        assert server.name
        assert server.path
        assert server.data
    assert len(servers) > 0, "No server is listed for any of feeds"


def test_list_domain(provider):
    """ Checks whether any domain is listed and has attributes"""
    domains = provider.inventory.list_domain()
    for domain in domains:
        assert domain.id
        assert domain.name
        assert domain.path
        assert domain.data
    assert len(domains) > 0, "No domain is listed for any of feeds"


def test_list_server_group(provider):
    """ Checks whether any group is listed and has attributes"""
    domains = provider.inventory.list_domain()
    for domain in domains:
        server_groups = provider.inventory.list_server_group(domain.path.feed_id)
        for server_group in server_groups:
            assert server_group.id
            assert server_group.name
            assert server_group.path
            assert server_group.data
        assert len(server_groups) > 0, "No server group is listed for any of feeds"


def test_list_server_deployment(provider):
    """ Checks whether any deployment is listed and has attributes """
    deployments = provider.inventory.list_server_deployment()
    for deployment in deployments:
        assert deployment.id
        assert deployment.name
        assert deployment.path
    assert len(deployments) > 0, "No deployment is listed for any of feeds"


def test_list_messaging(provider):
    """ Checks whether any messaging is listed and has attributes """
    messagings = provider.inventory.list_messaging()
    for messaging in messagings:
        assert messaging.id
        assert messaging.name
        assert messaging.path
    assert len(messagings) > 0, "No messaging is listed for any of feeds"


def test_get_config_data(provider):
    """ Checks whether resource data is provided and has attributes """
    found = False
    servers = provider.inventory.list_server()
    for server in servers:
        r_data = provider.inventory.get_config_data(feed_id=server.path.feed_id,
                                                    resource_id=server.id)
        if r_data:
            found = True
            assert r_data.name
            assert r_data.path
            assert r_data.value
    assert found, "No resource data is listed for any of servers"


def test_edit_resource_data(provider, datasource):
    """ Checks whether resource data is edited """
    r_data = _read_resource_data(provider, datasource)
    assert r_data, "Resource data should exist"
    r_data.value['Enabled'] = "false"
    result = _update_resource_data(provider, r_data, datasource)
    assert result, "Update should be successful"
    r_data = _read_resource_data(provider, datasource)
    # skip value verification for mocked provider
    if os.getenv('HAWKULAR_HOSTNAME'):
        assert r_data.value['Enabled'] == "false"


def test_delete_resource(provider, datasource):
    """ Checks whether resource is deleted """
    r_data = _read_resource_data(provider, datasource)
    assert r_data, "Resource data should exist"
    result = _delete_resource(provider, datasource)
    assert result, "Delete should be successful"
    r_data = _read_resource_data(provider, datasource)
    # skip deleted verification for mocked provider
    if os.getenv('HAWKULAR_HOSTNAME'):
        assert not r_data


def _read_resource_data(provider, resource):
    return provider.inventory.get_config_data(feed_id=resource.path.feed_id,
                                              resource_id=resource.path.resource_id)


def _create_resource(provider, resource, resource_data, resource_type):
    return provider.inventory.create_resource(resource=resource, resource_data=resource_data,
                                              resource_type=resource_type,
                                              feed_id=resource.path.feed_id)


def _update_resource_data(provider, resource_data, resource):
    return provider.inventory.edit_config_data(resource_data=resource_data,
                                               feed_id=resource.path.feed_id,
                                               resource_id=resource.path.resource_id)


def _delete_resource(provider, resource):
    return provider.inventory.delete_resource(feed_id=resource.path.feed_id,
                    resource_id=resource.path.resource_id)


def test_list_server_datasource(provider):
    """ Checks whether any datasource is listed and has attributes """
    found = False
    datasources = provider.inventory.list_server_datasource()
    if len(datasources) > 0:
        found = True
    for datasource in datasources:
        assert datasource.id
        assert datasource.name
        assert datasource.path
    assert found | provider.inventory._stats_available['num_datasource'](provider.inventory) > 0,\
        "No any datasource is listed for any of feeds, but they exists"


def test_path(provider):
    """ Checks whether path returned correctly """
    feeds = provider.inventory.list_feed()
    for feed in feeds:
        assert feed.path
        assert feed.path.feed_id
    servers = provider.inventory.list_server()
    for server in servers:
        assert server.path
        assert server.path.tenant_id
        assert server.path.feed_id
        assert server.path.resource_id


def test_num_server(provider):
    """ Checks whether number of servers is returned correct """
    servers_count = 0
    feeds = provider.inventory.list_feed()
    for feed in feeds:
        servers_count += len(provider.inventory.list_server(feed_id=feed.id))
    num_server = provider.inventory._stats_available['num_server'](provider.inventory)
    assert num_server == servers_count, "Number of servers is wrong"


def test_num_deployment(provider):
    """ Checks whether number of deployments is returned correct """
    deployments_count = 0
    feeds = provider.inventory.list_feed()
    for feed in feeds:
        deployments_count += len(provider.inventory.list_server_deployment(feed_id=feed.id))
    num_deployment = provider.inventory._stats_available['num_deployment'](provider.inventory)
    assert num_deployment == deployments_count, "Number of deployments is wrong"


def test_num_datasource(provider):
    """ Checks whether number of datasources is returned correct """
    datasources_count = 0
    feeds = provider.inventory.list_feed()
    for feed in feeds:
        datasources_count += len(provider.inventory.list_server_datasource(feed_id=feed.id))
    num_datasource = provider.inventory._stats_available['num_datasource'](provider.inventory)
    assert num_datasource == datasources_count, "Number of datasources is wrong"


def test_num_messaging(provider):
    """ Checks whether number of messagings is returned correct """
    messagings_count = 0
    feeds = provider.inventory.list_feed()
    for feed in feeds:
        messagings_count += len(provider.inventory.list_messaging(feed_id=feed.id))
    num_messaging = provider.inventory._stats_available['num_messaging'](provider.inventory)
    assert num_messaging == messagings_count, "Number of messagings is wrong"


def test_list_event(provider):
    """ Checks whether is any event listed """
    events = provider.alert.list_event()
    if len(events) > 0:
        event = events[0]
        assert event.id
        assert event.eventType
        assert event.ctime
        assert event.dataSource
        assert event.dataId
        assert event.category
        assert event.text
