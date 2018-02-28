from __future__ import absolute_import
import pytest
from wrapanapi import SCVMMSystem


@pytest.fixture(scope="module")
def mgmtsys(variables):
    data = variables.get("scvmm")
    if data:
        return SCVMMSystem(**data)
    pytest.skip("scvmm variables missing")


@pytest.fixture(scope="module")
def first_vm(mgmtsys):
    return mgmtsys.all_vms()[0].name


def test_list_vm(mgmtsys):
    print(mgmtsys.list_vm())


def test_list_hosts(mgmtsys):
    print(mgmtsys.list_hosts())


def test_all_vms(mgmtsys):
    print(mgmtsys.all_vms())


def test_list_template(mgmtsys):
    print(mgmtsys.list_template())


def test_list_network(mgmtsys):
    print(mgmtsys.list_network())


def test_vm_creation_time(mgmtsys, first_vm):
    print(mgmtsys.vm_creation_time(first_vm))


def test_vm_status(mgmtsys, first_vm):
    print(mgmtsys.vm_status(first_vm))


def test_current_ip_address(mgmtsys, first_vm):
    print(mgmtsys.current_ip_address(first_vm))


def test_get_vms_vmhost(mgmtsys, first_vm):
    print(mgmtsys.get_vms_vmhost(first_vm))


def test_data(mgmtsys, first_vm):
    print(mgmtsys.data(first_vm))
