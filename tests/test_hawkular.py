# -*- coding: utf-8 -*-
"""Unit tests for Hawkular client."""
import json
from urlparse import urlparse

import os
import pytest
from mgmtsystem import hawkular
from mock import patch
from random import sample


def fake_urlopen(c_client, url, headers):
    """
    Added temporary solution by working with deprecated API for new Hawkular-Inventory
    A stub urlopen() implementation that load json responses from
    the filesystem.
    """
    # Map path from url to a file
    parsed_url = urlparse("{}/{}".format(c_client.api_entry, url)).path
    # temporary solution for deprecated API
    parsed_url = parsed_url.replace("/deprecated/", "/")
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
        patcher = patch('mgmtsystem.rest_client.ContainerClient.get_json', fake_urlopen)
        patcher.start()
        patcher = patch('mgmtsystem.rest_client.ContainerClient.delete_status', fake_urldelete)
        patcher.start()
        patcher = patch('mgmtsystem.rest_client.ContainerClient.post_status', fake_urlpost)
        patcher.start()
        patcher = patch('mgmtsystem.rest_client.ContainerClient.put_status', fake_urlput)
        patcher.start()

    hwk = hawkular.Hawkular(
        hostname=os.getenv('HAWKULAR_HOSTNAME', 'localhost'),
        protocol=os.getenv('HAWKULAR_PROTOCOL', 'http'),
        port=os.getenv('HAWKULAR_PORT', 8080),
        username=os.getenv('HAWKULAR_USERNAME', 'jdoe'),
        password=os.getenv('HAWKULAR_PASSWORD', 'password')
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
    datasources = provider.list_server_datasource()
    assert len(datasources) > 0, "No resource data is listed for any of datasources"
    new_datasource = None
    for datasource in sample(datasources, 1):
        r_data = _read_resource_data(provider, datasource)
        assert r_data

        name_ext = "MWTest"
        new_datasource = hawkular.Resource(name="{}{}".format(datasource.name, name_ext),
                                id="{}{}".format(datasource.id, name_ext),
                                path=hawkular.Path("{}{}".format(datasource.path, name_ext)))
        new_datasource.path.resource = new_datasource.path.resource[1]

        resource_type = hawkular.ResourceType(id=None, name=None, path="Datasource")

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
    feeds = provider.list_feed()
    assert len(feeds) > 0, "No feeds are listed"
    for feed in feeds:
        assert feed.id
        assert feed.path


def test_list_resource_type(provider):
    """ Checks whether any resource type is listed and has attributes """
    feeds = provider.list_feed()
    for feed in feeds:
        res_types = provider.list_resource_type(feed_id=feed.id)
        for res_type in res_types:
            assert res_type.id
            assert res_type.name
            assert res_type.path
    assert len(res_types) > 0, "No resource type is listed for any of feeds"


def test_list_server(provider):
    """ Checks whether any server is listed and has attributes"""
    servers = provider.list_server()
    for server in servers:
        assert server.id
        assert server.name
        assert server.path
        assert server.data['data_name']
        assert server.data['Hostname']
        assert server.data['Server State']
    assert len(servers) > 0, "No server is listed for any of feeds"


def test_list_server_deployment(provider):
    """ Checks whether any deployment is listed and has attributes """
    deployments = provider.list_server_deployment()
    for deployment in deployments:
        assert deployment.id
        assert deployment.name
        assert deployment.path
    assert len(deployments) > 0, "No deployment is listed for any of feeds"


def test_resource_data(provider):
    """ Checks whether resource data is provided and has attributes """
    found = False
    servers = provider.list_server()
    for server in servers:
        r_data = provider.resource_data(feed_id=server.path.feed, resource_id=server.id)
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
    return provider.resource_data(feed_id=resource.path.feed,
                    resource_id=_get_resource_id(resource))


def _create_resource(provider, resource, resource_data, resource_type):
    return provider.create_resource(resource=resource, resource_data=resource_data,
                                    resource_type=resource_type, feed_id=resource.path.feed)


def _update_resource_data(provider, resource_data, resource):
    return provider.edit_resource_data(resource_data=resource_data, feed_id=resource.path.feed,
                    resource_id=_get_resource_id(resource))


def _delete_resource(provider, resource):
    return provider.delete_resource(feed_id=resource.path.feed,
                    resource_id=_get_resource_id(resource))


def _get_resource_id(resource):
    if isinstance(resource.path.resource, list):
        return "{}/{}".format(resource.path.resource[0], resource.path.resource[1])
    else:
        return resource.path.resource


def test_list_server_datasource(provider):
    """ Checks whether any datasource is listed and has attributes """
    found = False
    datasources = provider.list_server_datasource()
    if len(datasources) > 0:
        found = True
    for datasource in datasources:
        assert datasource.id
        assert datasource.name
        assert datasource.path
    assert (found | provider._stats_available['num_datasource'](provider) > 0,
            "No any datasource is listed for any of feeds, but they exists")


def test_path(provider):
    """ Checks whether path returned correctly """
    feeds = provider.list_feed()
    for feed in feeds:
        assert feed.path
        assert feed.path.tenant
        assert feed.path.feed
    servers = provider.list_server()
    for server in servers:
        assert server.path
        assert server.path.tenant
        assert server.path.feed
        assert server.path.resource


def test_num_server(provider):
    """ Checks whether number of servers is returned correct """
    servers_count = 0
    feeds = provider.list_feed()
    for feed in feeds:
        servers_count += len(provider.list_server(feed_id=feed.id))
    num_server = provider._stats_available['num_server'](provider)
    assert num_server == servers_count, "Number of servers is wrong"


def test_num_deployment(provider):
    """ Checks whether number of deployments is returned correct """
    deployments_count = 0
    feeds = provider.list_feed()
    for feed in feeds:
        deployments_count += len(provider.list_server_deployment(feed_id=feed.id))
    num_deployment = provider._stats_available['num_deployment'](provider)
    assert num_deployment == deployments_count, "Number of deployments is wrong"


def test_num_datasource(provider):
    """ Checks whether number of datasources is returned correct """
    datasources_count = 0
    feeds = provider.list_feed()
    for feed in feeds:
        datasources_count += len(provider.list_server_datasource(feed_id=feed.id))
    num_datasource = provider._stats_available['num_datasource'](provider)
    assert num_datasource == datasources_count, "Number of datasources is wrong"


def test_list_event(provider):
    """ Checks whether is any event listed """
    events = provider.list_event()
    if len(events) > 0:
        event = events[0]
        assert event.id
        assert event.eventType
        assert event.ctime
        assert event.dataSource
        assert event.dataId
        assert event.category
        assert event.text
