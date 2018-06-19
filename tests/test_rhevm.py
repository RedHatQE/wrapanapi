import os
import pytest

from wrapanapi import RHEVMSystem
from wrapanapi.exceptions import VMInstanceNotFound
BAD_NAME_SUFFIX = "-None-shoud-be-found1337"


@pytest.fixture(name='rhevm', scope="module")
def make_rhevm(variables):
    data = variables.get('rhevm')
    if not data:
        pytest.skip('no rhevm data')
    return RHEVMSystem(**data['system'])


@pytest.fixture(name="first_vm_name")
def fetch_first_vm_name(rhevm):
    return rhevm.list_vm()[0]


@pytest.fixture(name="small_template_name")
def fetch_template_name(variables):
    data = variables.get('rhevm')
    if not data:
        pytest.skip('no rhevm data')
    return data["templates"]["small_template"]["name"]


@pytest.fixture(name="vm_name")
def generate_vm_name(request):
    # todo - handle the possibility of collisions
    # between different computers/runs
    return "{name}-{pid}".format(
        name=request.node.obj.__name__,
        pid=os.getpid(),
    )


@pytest.fixture(name='vm')
def create_vm(request, small_template_name, rhevm, vm_name):
    args = request.node.get_marker('deploy_args')
    if args is None:
        deploy_args = {}
    else:
        deploy_args = args.kwargs
    vm_name = rhevm.deploy_template(
        small_template_name,
        cluster=rhevm.list_cluster()[0],
        vm_name=vm_name,
        **deploy_args
    )
    yield vm_name

    if request.node.get_marker('dont_destroy_vm') is None:
        rhevm.delete_vm(vm_name)


def test_internal_get_vm(rhevm, first_vm_name):
    vm = rhevm._get_vm(first_vm_name)
    print(first_vm_name, vm)

    with pytest.raises(VMInstanceNotFound):
        rhevm._get_vm(None)

    with pytest.raises(VMInstanceNotFound):
        rhevm._get_vm(first_vm_name + BAD_NAME_SUFFIX)


def test_ip_address(rhevm, first_vm_name):
    ip = rhevm.current_ip_address(first_vm_name)
    assert rhevm.get_ip_address(first_vm_name, timeout=20) == ip
    assert rhevm.get_vm_name_from_ip(ip) == first_vm_name


def test_vm_exists(rhevm, first_vm_name):
    assert rhevm.does_vm_exist(first_vm_name)
    assert not rhevm.does_vm_exist(first_vm_name + BAD_NAME_SUFFIX)


@pytest.mark.dont_destroy_vm
@pytest.mark.deploy_args(start_vm=False)
def test_delete_vm(vm, rhevm):
    assert rhevm.does_vm_exist(vm)
    rhevm.delete_vm(vm)
    assert not rhevm.does_vm_exist(vm)


@pytest.mark.deploy_args(start_vm=False)
def test_start_vm(vm, rhevm):
    if not rhevm.is_vm_stopped(vm):
        rhevm.stop_vm(vm)  # workaround for startup deployment issue
    assert rhevm.is_vm_stopped(vm)
    rhevm.start_vm(vm)
    assert not rhevm.is_vm_stopped(vm)


def test_stop_vm(vm, rhevm):
    assert not rhevm.is_vm_stopped(vm)
    rhevm.stop_vm(vm)
    assert rhevm.is_vm_stopped(vm)


@pytest.mark.skip
def test_create_vm():
    pass


def test_restart_vm(vm, rhevm):
    rhevm.restart_vm(vm)


def test_list_vm(rhevm):
    print(rhevm.list_vm())


def test_all_vms(rhevm):
    print(rhevm.all_vms())


def test__get_vm_guid(rhevm, first_vm_name):
    print(rhevm.get_vm_guid(first_vm_name))


def test__list_host(rhevm):
    print(rhevm.list_host())


def test__list_datastore(rhevm):
    print(rhevm.list_datastore())


def test__list_cluster(rhevm):
    print(rhevm.list_cluster())


def test__list_template(rhevm):
    print(rhevm.list_template())


def test__vm_status(rhevm, first_vm_name):
    print(rhevm.vm_status(first_vm_name))


def test__vm_creation_time(rhevm, first_vm_name):
    print(rhevm.vm_creation_time(first_vm_name))


def test__in_steady_state(rhevm, first_vm_name):
    print(rhevm.in_steady_state(first_vm_name))


def test__is_vm_running(rhevm, first_vm_name):
    print(rhevm.is_vm_running(first_vm_name))


def test__is_vm_stopped(rhevm, first_vm_name):
    print(rhevm.is_vm_stopped(first_vm_name))


def test__is_vm_suspended(rhevm, first_vm_name):
    print(rhevm.is_vm_suspended(first_vm_name))


@pytest.mark.skip
def test__suspend_vm(rhevm, vm):
    assert not rhevm.is_vm_suspended(vm)
    rhevm.suspend_vm(vm)
    assert not rhevm.is_vm_suspended(vm)


@pytest.mark.skip
def test__clone_vm(rhevm, vm):
    clone_name = vm + "_clone"
    rhevm.clone_vm(vm, clone_name)
    assert rhevm.does_vm_exist(clone_name)
    rhevm.delete_vm(clone_name)


@pytest.mark.skip
def test__deploy_template(rhevm, small_template_name):
    pass  # for now ignored


@pytest.mark.skip
def test__remove_host_from_cluster():
    pass


@pytest.mark.skip
def test__mark_as_template():
    pass


@pytest.mark.skip
def test___rename_template():
    pass


@pytest.mark.skip
def test__rename_vm():
    pass


@pytest.mark.skip
def test___wait_template_ok():
    pass


@pytest.mark.skip
def test___wait_template_exists():
    pass


@pytest.mark.skip
def test__does_template_exist():
    pass


@pytest.mark.skip
def test__delete_template():
    pass


@pytest.mark.skip
def test__vm_hardware_configuration():
    pass


def test__usage_and_quota(rhevm):
    print(rhevm.usage_and_quota)
