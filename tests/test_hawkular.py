# -*- coding: utf-8 -*-
"""Unit tests for Hawkular client."""
import pytest

from mgmtsystem import hawkular


@pytest.fixture(scope="function")
def provider():
    hwk = hawkular.Hawkular(hostname='livingontheedge.hawkular.org', port=80,
                            username='jdoe', password='password')
    return hwk


def test_list_feed(provider):
    """ Checks whether is any feed listed """
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
        res_types = provider.list_resource_type(feed.id)
        if len(res_types) > 0:
            found = True
        for res_type in res_types:
            assert res_type.id
            assert res_type.name
            assert res_type.path
    assert found, "No any resource type is listed for any of feeds"


def test_list_server(provider):
    """ Checks whether any server is listed and has attributes"""
    found = False
    feeds = provider.list_feed()
    for feed in feeds:
        servers = provider.list_server(feed.id)
        if len(servers) > 0:
            found = True
        for server in servers:
            assert server.id
            assert server.name
            assert server.path
    assert found, "No any server is listed for any of feeds"


def test_list_server_deployment(provider):
    """ Checks whether any deployment is listed and has attributes """
    found = False
    feeds = provider.list_feed()
    for feed in feeds:
        deployments = provider.list_server_deployment(feed.id)
        if len(deployments) > 0:
            found = True
        for deployment in deployments:
            assert deployment.id
            assert deployment.name
            assert deployment.path
    assert found, "No any deployment is listed for any of feeds"


def test_get_server_status(provider):
    """ Checks whether server status is provided and has attributes """
    found = False
    feeds = provider.list_feed()
    for feed in feeds:
        status = provider.get_server_status(feed.id, 'Local~~')
        if status:
            found = True
            assert status.address
            assert status.version
            assert status.state
            assert status.product
            assert status.host
    assert found, "No Status is listed for any of servers"


def test_num_server(provider):
    """ Checks whether number of servers is returned correct """
    servers_count = 0
    feeds = provider.list_feed()
    for feed in feeds:
        servers_count += len(provider.list_server(feed.id))
    num_server = provider._stats_available['num_server'](provider)
    assert num_server == servers_count, "Number of servers is wrong"


def test_num_deployment(provider):
    """ Checks whether number of deployments is returned correct """
    deployments_count = 0
    feeds = provider.list_feed()
    for feed in feeds:
        deployments_count += len(provider.list_server_deployment(feed.id))
    num_deployment = provider._stats_available['num_deployment'](provider)
    assert num_deployment == deployments_count, "Number of deployments is wrong"
