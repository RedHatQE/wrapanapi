import pytest
from wrapanapi import SCVMMSystem


@pytest.fixture(scope="module")
def mgmtsys(variables):
    data = variables.get("scvmm")
    if data:
        return SCVMMSystem(**data)
    pytest.skip("scvmm variables missing")


def test_list_vm(mgmtsys):
    print mgmtsys.list_vm()


def test_list_hosts(mgmtsys):
    print mgmtsys.list_hosts()


def test_all_vms(mgmtsys):
    print(mgmtsys.all_vms())
