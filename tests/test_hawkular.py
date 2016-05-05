# -*- coding: utf-8 -*-
"""Unit tests for Hawkular client."""
import os
import pytest

from mgmtsystem import hawkular


@pytest.fixture(scope="function")
def provider():
    hwk = hawkular.Hawkular(
        hostname=os.getenv('HAWKULAR_HOSTNAME', 'localhost'),
        protocol=os.getenv('HAWKULAR_PROTOCOL', 'http'),
        port=os.getenv('HAWKULAR_PORT', 8080),
        username=os.getenv('HAWKULAR_USERNAME', 'jdoe'),
        password=os.getenv('HAWKULAR_PASSWORD', 'password')
    )
    return hwk


def test_list_feed(provider):
    """ Checks whether any feed is listed """
    feeds = provider.list_feed()
    assert len(feeds) > 0, "No feeds are listed"
    for feed in feeds:
        assert feed.id
        assert feed.path


def test_list_resource_type(provider):
    """ Checks whether any resource type is listed and has attributes """
    found = False
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
    found = False
    servers = provider.list_server()
    for server in servers:
        assert server.id
        assert server.name
        assert server.path
        assert server.data['data_name']
        assert server.data['Hostname']
        assert server.data['Server State']
        assert server.data['Version']
        assert server.data['Product Name']
    assert len(servers) > 0, "No server is listed for any of feeds"


def test_list_server_deployment(provider):
    """ Checks whether any deployment is listed and has attributes """
    found = False
    deployments = provider.list_server_deployment()
    for deployment in deployments:
        assert deployment.id
        assert deployment.name
        assert deployment.path
    assert len(deployments) > 0, "No deployment is listed for any of feeds"


def test_resource_data(provider):
    """ Checks whether resource data is provided and has attributes """
    found = False
    feeds = provider.list_feed()
    for feed in feeds:
        resource_types = provider.list_resource_type(feed_id=feed.id)
        for r_type in resource_types:
            resources = provider.list_resource(feed_id=feed.id, type_id=r_type.id)
            for resource in resources:
                r_data = provider.resource_data(feed_id=feed.id, resource_id=resource.id)
                if r_data:
                    found = True
                    assert r_data.name
                    assert r_data.path
                    assert r_data.value
    assert found, "No resource data is listed for any of servers"


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


def test_list_event_empty(provider):
    """ Checks that events are filtered and empty list is returned """
    assert len(provider.list_event(0, 0)) == 0, "Unexpected events are returned"


def test_list_event(provider):
    """ Checks whether is any event listed """
    events = provider.list_event()
    assert len(events) > 0, "No events are listed"
    event = events[0]
    assert event.id
    assert event.eventType
    assert event.ctime
    assert event.dataSource
    assert event.dataId
    assert event.category
    assert event.text
