"""
Simple sanity test for VM/template systems

TODO: Possibly mock this out, remove reliance on 'cfme', etc.

If running within a cfme venv, disable the cfme plugins like so:
   $ pytest test_vm_and_template_systems.py -p no:cfme -s
"""

import datetime
import logging

import pytest
import fauxfactory
from wait_for import wait_for

import wrapanapi
from wrapanapi import VmState
from wrapanapi.entities import StackMixin
from wrapanapi.systems.ec2 import StackStates
from wrapanapi.exceptions import MultipleItemsError


log = logging.getLogger('wrapanapi.tests.test_vm_and_template_systems')

logging.basicConfig(level=logging.INFO)

PROVIDER_KEYS_LIST = [
    'rhos11', 'vsphere65-nested', 'scvmm', 'azure', 'gce_central', 'ec2west', 'rhv41'
]


@pytest.fixture(params=PROVIDER_KEYS_LIST)
def provider_crud(request):
    providers = pytest.importorskip('cfme.utils.providers')
    log.info("Using provider key: %s", request.param)
    return providers.get_crud(request.param)


@pytest.fixture
def test_template(provider_crud):
    deploy_args = {}
    try:
        template_name = provider_crud.data['templates']['small_template']['name']
        deploy_args.update({'template': template_name})
    except KeyError:
        raise KeyError('small_template not defined for Provider {} in cfme_data.yaml'
                       .format(provider_crud.data['name']))
    log.info(
        "Using template %s on provider %s",
        deploy_args['template'], provider_crud.data['name']
    )

    deploy_args.update(vm_name='TBD')
    deploy_args.update(provider_crud.deployment_helper(deploy_args))
    log.debug("Deploy args: %s", deploy_args)

    if isinstance(provider_crud.mgmt, wrapanapi.systems.AzureSystem):
        template = provider_crud.mgmt.get_template(
            template_name, container=deploy_args['template_container'])
    else:
        template = provider_crud.mgmt.get_template(template_name)

    return template


@pytest.yield_fixture
def test_vm(provider_crud, test_template):
    deploy_args = {}
    try:
        template_name = provider_crud.data['templates']['small_template']['name']
        deploy_args.update({'template': template_name})
    except KeyError:
        raise KeyError('small_template not defined for Provider {} in cfme_data.yaml'
                       .format(provider_crud.data['name']))

    vm_name = 'test-{}'.format(fauxfactory.gen_alphanumeric(6)).lower()
    log.info("Deploying VM %s", vm_name)

    deploy_args.update(vm_name=vm_name)
    deploy_args.update(provider_crud.deployment_helper(deploy_args))
    log.debug("Deploy args: %s", deploy_args)

    vm = test_template.deploy(timeout=900, **deploy_args)

    yield vm

    try:
        vm.cleanup()
    except Exception:
        log.exception("Failed to cleanup vm")


def test_sanity(provider_crud, test_template, test_vm):

    template = test_template
    vm = test_vm
    mgmt = provider_crud.mgmt

    if isinstance(mgmt, StackMixin):
        stacks = mgmt.list_stacks(StackStates.ALL)
        assert [repr(stack) for stack in stacks]
        assert str(stacks[0])
        stack = stacks[0]
        assert stack in mgmt.find_stacks(name=stack.name)
        assert mgmt.get_stack(name=stack.name) == stack
        assert mgmt.does_stack_exist(stack.name)
        assert not mgmt.does_stack_exist("some_fake_name")

    log.info("Listing VMs")
    vms = mgmt.list_vms()
    assert [repr(v) for v in vms]
    assert str(vms[0])

    assert template.name
    assert template.uuid
    assert template.exists

    templates = mgmt.list_templates()
    assert [repr(tmp) for tmp in templates]
    assert str(templates[0])
    try:
        assert template in mgmt.find_templates(name=template.name)
    except NotImplementedError:
        pass
    assert template in mgmt.list_templates()
    try:
        assert mgmt.get_template(template.name) == template
    except MultipleItemsError:
        pass

    assert vm

    assert isinstance(vm.creation_time, datetime.datetime)

    assert vm.exists
    try:
        assert vm.get_hardware_configuration()
    except NotImplementedError:
        pass

    assert vm.in_steady_state
    assert vm.state in VmState.valid_states()

    if isinstance(mgmt, wrapanapi.systems.OpenstackSystem):
        pools = mgmt.api.floating_ip_pools.list()
        pool_name = pools[0].name
        vm.assign_floating_ip(pool_name)

    wait_for(lambda: vm.ip, timeout="1m", delay=1)

    if isinstance(mgmt, wrapanapi.systems.OpenstackSystem):
        vm.unassign_floating_ip()
        assert not vm.ip

    try:
        new_name = 'test-{}'.format(fauxfactory.gen_alphanumeric(6)).lower()
        vm.rename(new_name)
        assert vm.name == new_name
    except NotImplementedError:
        pass

    if isinstance(vm, wrapanapi.entities.Instance):
        assert vm.type

    vm.ensure_state(VmState.STOPPED)

    vm.ensure_state(VmState.RUNNING)

    log.info("Checking state changes")
    if vm.system.can_pause:
        vm.pause()
        assert vm.is_paused
        vm.ensure_state(VmState.RUNNING)

    if vm.system.can_suspend:
        vm.suspend()
        assert vm.is_suspended
        vm.ensure_state(VmState.RUNNING)

    assert vm.stop()
    assert vm.is_stopped
    assert vm.start()
    assert vm.is_running

    assert vm.restart()
    assert vm.is_running

    try:
        assert vm in mgmt.find_vms(name=vm.name)
    except NotImplementedError:
        pass
    assert vm in mgmt.list_vms()
    assert mgmt.get_vm(vm.name) == vm

    log.info("Cleaning up VM")
    assert vm.cleanup()

    assert not vm.exists
    assert vm not in mgmt.list_vms()
