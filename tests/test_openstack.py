# -*- coding: utf-8 -*-
"""Unit tests for Openstack client."""
import pytest

from mgmtsystem import exceptions, openstack


@pytest.fixture(scope="function")
def provider():
    os = openstack.OpenstackSystem(tenant=None, username=None, password=None, auth_url=None)
    return os


class mkobj(object):
    def __init__(self, **d):
        self.__dict__ = d


def test_vm_status_error_raises_with_fault(provider, monkeypatch):
    """Check that if the Instance gets to the ERROR state, vm_status will let us know."""
    def _find_instance_by_name(vm_name):
        return mkobj(
            status="ERROR",
            fault={"code": 500, "created": "2015-11-02T10:54:18Z", "details": "x", "message": "y"})

    monkeypatch.setattr(provider, '_find_instance_by_name', _find_instance_by_name)

    with pytest.raises(exceptions.VMError):
        provider.vm_status("xyz")


def test_vm_status_error_raises_without_fault(provider, monkeypatch):
    """Check that if the Instance gets to the ERROR state, vm_status will let us know.

    With no fault field.
    """
    def _find_instance_by_name(vm_name):
        return mkobj(status="ERROR")

    monkeypatch.setattr(provider, '_find_instance_by_name', _find_instance_by_name)

    with pytest.raises(exceptions.VMError):
        provider.vm_status("xyz")


def test_vm_status_no_error(provider, monkeypatch):
    """Check that if the Instance is not in error state, it works as usual."""
    def _find_instance_by_name(vm_name):
        return mkobj(status="UP")

    monkeypatch.setattr(provider, '_find_instance_by_name', _find_instance_by_name)

    assert provider.vm_status("xyz") == "UP"
