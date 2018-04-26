# -*- coding: utf-8 -*-
"""Backend management system classes

Used to communicate with providers without using CFME facilities
"""
from __future__ import absolute_import
import os
import pytz

from cached_property import cached_property

from azure.common.credentials import ServicePrincipalCredentials
from azure.common.exceptions import CloudError
from azure.common import AzureConflictHttpError
from azure.mgmt.compute import ComputeManagementClient
from azure.mgmt.compute.models import (VirtualMachineCaptureParameters, DiskCreateOptionTypes,
                                       VirtualMachineSizeTypes, VirtualHardDisk)
from azure.mgmt.network import NetworkManagementClient
from azure.mgmt.network.models import NetworkSecurityGroup
from azure.mgmt.resource import ResourceManagementClient, SubscriptionClient
from azure.mgmt.resource.subscriptions.models import SubscriptionState
from azure.mgmt.storage import StorageManagementClient
from azure.storage.blob import BlockBlobService
from contextlib import contextmanager
from datetime import datetime, timedelta
from wait_for import wait_for

from .exceptions import VMInstanceNotFound
from .base import WrapanapiAPIBaseVM


# TODO: add handler for logger msrest.service_client if needed
# TODO: add better description to each method


class AzureSystem(WrapanapiAPIBaseVM):
    """This class is used to connect to Microsoft Azure Portal via PowerShell AzureRM Module
    """
    STATE_RUNNING = "VM running"
    STATE_STOPPED = "VM deallocated"
    STATE_STARTING = "VM starting"
    STATE_SUSPEND = "VM stopped"
    STATE_PAUSED = "Paused"
    STATES_STEADY = {STATE_RUNNING, STATE_PAUSED, STATE_STOPPED}

    _stats_available = {
        'num_vm': lambda self: len(self.list_vm()),
        'num_template': lambda self: len(self.list_template()),
    }

    def __init__(self, **kwargs):
        super(AzureSystem, self).__init__(kwargs)
        self.client_id = kwargs.get("username")
        self.client_secret = kwargs.get("password")
        self.tenant = kwargs.get("tenant_id")
        self.subscription_id = kwargs.get("subscription_id")
        self.resource_group = kwargs['provisioning']['resource_group']  # default resource group
        self.storage_account = kwargs.get("storage_account")
        self.storage_key = kwargs.get("storage_key")
        self.template_container = kwargs['provisioning']['template_container']
        self.region = kwargs["provisioning"]["region_api"].replace(' ', '').lower()

        self.credentials = ServicePrincipalCredentials(client_id=self.client_id,
                                                       secret=self.client_secret,
                                                       tenant=self.tenant)

    def __setattr__(self, key, value):
        """If the subscription_id is changed, invalidate client caches"""
        if key in ['credentials', 'subscription_id']:
            for client in ['compute_client', 'resource_client', 'network_client',
                           'subscription_client', 'storage_client']:
                if getattr(self, client, False):
                    del self.__dict__[client]
        if key in ['storage_account', 'storage_key']:
            if getattr(self, 'container_client', False):
                del self.__dict__['container_client']
        self.__dict__[key] = value

    @cached_property
    def compute_client(self):
        return ComputeManagementClient(self.credentials, self.subscription_id)

    @cached_property
    def resource_client(self):
        return ResourceManagementClient(self.credentials, self.subscription_id)

    @cached_property
    def network_client(self):
        return NetworkManagementClient(self.credentials, self.subscription_id)

    @cached_property
    def storage_client(self):
        return StorageManagementClient(self.credentials, self.subscription_id)

    @cached_property
    def container_client(self):
        return BlockBlobService(self.storage_account, self.storage_key)

    @cached_property
    def subscription_client(self):
        return SubscriptionClient(self.credentials)

    @property
    def vms_collection(self):
        return self.compute_client.virtual_machines

    def start_vm(self, vm_name, resource_group=None):
        col = self.vms_collection
        self.logger.info("starting vm {v}".format(v=vm_name))
        operation = col.start(resource_group_name=resource_group or self.resource_group,
                              vm_name=vm_name)
        operation.wait()
        return operation.status()

    def stop_vm(self, vm_name, resource_group=None):
        col = self.vms_collection
        self.logger.info("stopping vm {v}".format(v=vm_name))
        operation = col.deallocate(resource_group_name=resource_group or self.resource_group,
                                   vm_name=vm_name)
        operation.wait()
        return operation.status()

    def restart_vm(self, vm_name, resource_group=None):
        col = self.vms_collection
        self.logger.info("restarting vm {v}".format(v=vm_name))
        operation = col.restart(resource_group_name=resource_group or self.resource_group,
                                vm_name=vm_name)
        operation.wait()
        return operation.status()

    def suspend_vm(self, vm_name, resource_group=None):
        col = self.vms_collection
        self.logger.info("suspending vm {v}".format(v=vm_name))
        operation = col.power_off(resource_group_name=resource_group or self.resource_group,
                                  vm_name=vm_name)
        operation.wait()
        return operation.status()

    def create_vm(self, vm_name, *args, **kwargs):
        # todo: implement it later
        raise NotImplementedError('NIE - create_vm not implemented.')

    def delete_vm(self, vm_name, *args, **kwargs):
        # todo: check that all resources are deleted
        resource_group = kwargs.get('resource_group')
        col = self.vms_collection
        self.logger.info("trying to delete vm {v}".format(v=vm_name))
        operation = col.delete(resource_group_name=resource_group or self.resource_group,
                               vm_name=vm_name)
        operation.wait()
        return operation.status()

    def list_vm(self):
        """
        Returns list of Instances from all Resource Groups available in current Region
        """
        vm_list = []
        for res_group in self.list_resource_groups():
            vms = self.vms_collection.list(resource_group_name=res_group)
            vm_list.extend([vm.name for vm in vms if vm.location == self.region])
        return vm_list

    def list_vm_by_resource_group(self, resource_group=None):
        vms = self.vms_collection.list(resource_group_name=resource_group or self.resource_group)
        return [vm.name for vm in vms]

    def capture_vm(self, vm_name, container, image_name, overwrite_vhds=True, **kwargs):
        self.logger.info("Attempting to Capture Azure VM {}".format(vm_name))
        resource_group = kwargs.get('resource_group') or self.resource_group
        col = self.vms_collection
        params = VirtualMachineCaptureParameters(vhd_prefix=image_name,
                                                 destination_container_name=container,
                                                 overwrite_vhds=overwrite_vhds)
        self.logger.info("Stopping VM {}".format(vm_name))
        self.stop_vm(vm_name, resource_group)
        self.logger.info("Generalizing VM {}".format(vm_name))
        col.generalize(resource_group_name=resource_group, vm_name=vm_name)
        self.logger.info("Capturing VM {}".format(vm_name))
        operation = col.capture(resource_group_name=resource_group, vm_name=vm_name,
                                parameters=params)
        operation.wait()
        return operation.status()

    def data(self, vm_name, resource_group=None):
        raise NotImplementedError('data not implemented.')

    def vm_status(self, vm_name, resource_group=None):
        self.logger.info("Attempting to Retrieve Azure VM Status {}".format(vm_name))
        vm = self.vms_collection.get(resource_group_name=resource_group or self.resource_group,
                                     vm_name=vm_name, expand='instanceView')

        first_status = vm.instance_view.statuses[0]
        if first_status.display_status == 'Provisioning failed':
            raise VMInstanceNotFound(first_status.message)

        last_power_status = vm.instance_view.statuses[-1].display_status
        self.logger.info("Returned Status was {}".format(last_power_status))
        return last_power_status

    def vm_type(self, vm_name, resource_group=None):
        self.logger.info("Attempting to Retrieve Azure VM Type {}".format(vm_name))
        vm = self.vms_collection.get(resource_group_name=resource_group or self.resource_group,
                                     vm_name=vm_name, expand='instanceView')
        vm_type = vm.hardware_profile.vm_size
        self.logger.info("Returned Type was {}".format(vm_type))
        return vm_type

    def is_vm_running(self, vm_name, resource_group=None):
        if self.vm_status(vm_name, resource_group) == self.STATE_RUNNING:
            self.logger.info("According to Azure, the VM \"{}\" is running".format(vm_name))
            return True
        else:
            return False

    def is_vm_stopped(self, vm_name, resource_group=None):
        if self.vm_status(vm_name, resource_group) == self.STATE_STOPPED:
            self.logger.info("According to Azure, the VM \"{}\" is stopped".format(vm_name))
            return True
        else:
            return False

    def is_vm_starting(self, vm_name, resource_group=None):
        if self.vm_status(vm_name, resource_group) == self.STATE_STARTING:
            self.logger.info("According to Azure, the VM \"{}\" is starting".format(vm_name))
            return True
        else:
            return False

    def is_vm_suspended(self, vm_name, resource_group=None):
        if self.vm_status(vm_name, resource_group) == self.STATE_SUSPEND:
            self.logger.info("According to Azure, the VM \"{}\" is suspended".format(vm_name))
            return True
        else:
            return False

    def in_steady_state(self, vm_name, resource_group=None):
        return self.vm_status(vm_name, resource_group) in self.STATES_STEADY

    def clone_vm(self, source_name, vm_name):
        """It wants exact host and placement (c:/asdf/ghjk) :("""
        raise NotImplementedError('NIE - clone_vm not implemented.')

    def does_vm_exist(self, vm_name, resource_group=None):
        return vm_name in self.list_vm_by_resource_group(resource_group=resource_group)

    def wait_vm_running(self, vm_name, resource_group=None, num_sec=300):
        wait_for(
            lambda: self.is_vm_running(vm_name, resource_group),
            message="Waiting for Azure VM {} to be running.".format(vm_name),
            num_sec=num_sec)

    def wait_vm_steady(self, vm_name, resource_group=None, num_sec=300):
        self.logger.info("All states are steady in Azure. {}".format(vm_name))
        # todo: need to check what is that ?
        return True

    def wait_vm_stopped(self, vm_name, resource_group=None, num_sec=300):
        wait_for(
            lambda: self.is_vm_stopped(vm_name, resource_group),
            message="Waiting for Azure VM {} to be stopped.".format(vm_name),
            num_sec=num_sec)

    def wait_vm_suspended(self, vm_name, resource_group=None, num_sec=300):
        wait_for(
            lambda: self.is_vm_suspended(vm_name, resource_group),
            message="Waiting for Azure VM {} to be suspended.".format(vm_name),
            num_sec=num_sec)

    def vm_creation_time(self, vm_name, resource_group=None):
        # There is no such parameter as vm creation time.  Using VHD date instead.
        self.logger.info("Attempting to Retrieve Azure VM Modification Time {}".format(vm_name))
        vm = self.vms_collection.get(resource_group_name=resource_group or self.resource_group,
                                     vm_name=vm_name, expand='instanceView')
        create_time = vm.instance_view.statuses[0].time
        self.logger.info("VM creation time {}".format(str(create_time)))
        return create_time

    def remove_host_from_cluster(self, hostname):
        """I did not notice any scriptlet that lets you do this."""

    def disconnect_dvd_drives(self, vm_name):
        raise NotImplementedError('disconnect_dvd_drives not implemented.')

    def get_network_interface(self, vm_name, resource_group=None):
        # todo: weird function, to refactor it later
        self.logger.info("Attempting to Retrieve Azure VM Network Interface %s", vm_name)
        vm = self.vms_collection.get(resource_group_name=resource_group or self.resource_group,
                                     vm_name=vm_name)
        first_if = vm.network_profile.network_interfaces[0]
        self.logger.info("Returned URI was %s", first_if.id)
        return os.path.split(first_if.id)[1]

    def get_vm_vhd(self, vm_name, resource_group=None):
        self.logger.info("get_vm_vhd - Attempting to Retrieve Azure VM VHD %s", vm_name)
        vm = self.vms_collection.get(resource_group_name=resource_group or self.resource_group,
                                     vm_name=vm_name)
        vhd_endpoint = vm.storage_profile.os_disk.vhd.uri
        self.logger.info("Returned Disk Endpoint was %s", vhd_endpoint)
        return vhd_endpoint

    def current_ip_address(self, vm_name, resource_group=None):
        """
        To get IP we have to fetch:
                              - nic object from VM
                              - ip_config object from nic
                              - public_ip object from ip_config
        *_id - is a valid Azure resource id
        e.g. - /subscriptions/<subscription>/resourceGroups/<resource_group>/providers/
        Microsoft.Network/publicIPAddresses/<object_name>

        Return:
            1)Public IP which meets requirements(IPv4 Public IP of the primary ip_config of the
            primary network interface)
            2)None if no public IP found
            3)May raise an exception if resource_group wasn't provided and VM doesn't belong to
            AzureSystem.resource group - provider's default one
        """
        # TODO rework after PR240 - verify/get VM's resource group
        resource_group = resource_group or self.resource_group
        vm = self.vms_collection.get(resource_group_name=resource_group,
                                     vm_name=vm_name)
        # Getting id of the first network interface of the vm
        for nic in vm.network_profile.network_interfaces:
            # nic.primary is None when we have only one network interface attached to the VM
            if nic.primary is not False:
                first_vm_if_id = nic.id
                break
        if_name = os.path.split(first_vm_if_id)[1]
        if_obj = self.network_client.network_interfaces.get(resource_group, if_name)
        # Getting name of the first IP configuration of the network interface
        for ip_config in if_obj.ip_configurations:
            if ip_config.primary is True:
                ip_config_name = ip_config.name
                break
        ip_config_obj = self.network_client.network_interface_ip_configurations.get(resource_group,
                                                                                    if_name,
                                                                                    ip_config_name)
        # Getting public IP id from the IP configuration object
        try:
            pub_ip_id = ip_config_obj.public_ip_address.id
            pub_ip_name = os.path.split(pub_ip_id)[1]
            public_ip = self.network_client.public_ip_addresses.get(resource_group, pub_ip_name)
            if not public_ip.ip_address:
                # Dynamic ip will be allocated for Running VMs only
                self.logger.error("Couldn't get Public IP of {}. public_ip_allocation_method - {}. "
                                "Please check VM status".
                                format(vm_name, public_ip.public_ip_allocation_method))
                return None
            return public_ip.ip_address
        except AttributeError:
            self.logger.error("VM {} doesn't have public IP on {}:{}".format(vm_name, if_name,
                                                                             ip_config_name))
            return None

    def get_ip_address(self, vm_name, resource_group=None, **kwargs):
        current_ip_address = self.current_ip_address(vm_name, resource_group or self.resource_group)
        return current_ip_address

    def list_subscriptions(self):
        return [(str(s.display_name), str(s.subscription_id)) for s in
                self.subscription_client.subscriptions.list() if
                s.state == SubscriptionState.enabled]

    def list_region(self, subscription=None):
        """
        Get a list of available geo-locations

        NOTE: This operation provides all the locations that are available for resource providers;
        however, each resource provider may support a subset of this list.

        Return: list of tuples - (name, display_name)
        """
        subscription = subscription or self.subscription_id
        return [(region.name, region.display_name) for region in
                self.subscription_client.subscriptions.list_locations(subscription)]

    def list_storage_accounts_by_resource_group(self, resource_group):
        """List Azure Storage accounts on current subscription by resource group"""
        return [s.name for s in self.storage_client.storage_accounts.list_by_resource_group(
                resource_group)]

    def get_storage_account_key(self, storage_account_name, resource_group):
        """Each Storage account has 2 keys by default - both are valid and equal"""
        keys = {v.key_name: v.value for v in self.storage_client.storage_accounts.list_keys(
            resource_group, storage_account_name).keys}
        return keys['key1']

    def list_resource_groups(self):
        """
        List Resource Groups under current subscription_id
        """
        return [r.name for r in self.resource_client.resource_groups.list()]

    def list_free_pip(self, pip_template, resource_group=None):
        """
        List available Public IP in selected resource_group
        """
        resource_group = resource_group or self.resource_group
        ips = self.network_client.public_ip_addresses.list(resource_group_name=resource_group)
        return [ip.name for ip in ips if not ip.ip_configuration and pip_template in ip.name]

    def list_free_nics(self, nic_template, resource_group=None):
        """
        List available Network Interfaces in selected resource_group
        """
        resource_group = resource_group or self.resource_group
        ips = self.network_client.network_interfaces.list(resource_group_name=resource_group)
        return [ip.name for ip in ips if not ip.virtual_machine and nic_template in ip.name]

    def list_stack(self, resource_group=None, days_old=0):
        """
        List available Deployment Stacks in selected resource_group
        """
        resource_group = resource_group or self.resource_group
        # todo: check maybe bug - today instead of now ?
        today = datetime.utcnow().replace(tzinfo=pytz.utc)
        some_date = today - timedelta(days=days_old)
        some_date.replace(tzinfo=None)
        self.logger.info("Attempting to List Azure Orchestration Deployment Stacks")
        deps = self.resource_client.deployments
        deps_list = deps.list_by_resource_group(resource_group_name=resource_group)
        found_stacks = []
        for dep in deps_list:
            if dep.properties.timestamp < some_date:
                found_stacks.append(dep.name)
        return found_stacks

    def list_flavor(self):
        raise NotImplementedError('list_flavor not implemented.')

    def list_network(self):
        self.logger.info("Attempting to list Azure Virtual Private Networks in '%s'", self.region)
        # Azure API returns all networks from all regions, and there is options to filter by region.
        # In CFME only the networks of the provider regions are displayed.
        all_networks = self.network_client.virtual_networks.list_all()
        self.logger.debug('self.region {}'.format(self.region))
        networks_in_region = []
        for network in all_networks:
            if network.location == self.region:
                networks_in_region.append(network.name)
        return networks_in_region

    def list_subnet(self):
        self.logger.info("Attempting to List Azure Subnets")
        # There is no way to list all the subnets from a network filtered by location, and there
        # is only one network in the resource_group defined in cfme_data.
        all_networks = self.network_client.virtual_networks.list_all()

        self.logger.debug('self.region {}'.format(self.region))
        subnets = dict()
        for network in all_networks:
            if network.location == self.region:
                subnets[network.name] = [subnet.name for subnet in network.subnets]

        return subnets

    def list_security_group(self):
        self.logger.info("Attempting to List Azure security groups")
        all_sec_groups = self.network_client.network_security_groups.list_all()
        self.logger.debug('self.region {}'.format(self.region))
        location = self.region.replace(' ', '').lower()
        sec_groups_in_location = []
        for sec_gp in all_sec_groups:
            if sec_gp.location == location:
                sec_groups_in_location.append(sec_gp.name)
        return sec_groups_in_location

    def list_router(self):
        self.logger.info("Attempting to List Azure routes table")
        all_routers = self.network_client.route_tables.list_all()
        self.logger.debug('self.region {}'.format(self.region))
        routers_in_location = []
        for router in all_routers:
            if router.location == self.region:
                routers_in_location.append(router.name)
        return routers_in_location

    def disconnect(self):
        pass

    def remove_nics_by_search(self, nic_template, resource_group=None):
        """
        Used for clean_up jobs to remove NIC that are not attached to any test VM
        in selected resource_group.If None (default) resource_group provided, the instance's
        resource group is used instead
        """
        self.logger.info('Attempting to List NICs with "%s" name template', nic_template)
        results = []
        nic_list = self.list_free_nics(nic_template,
                                       resource_group=resource_group or self.resource_group)

        for nic in nic_list:
            operation = self.network_client.network_interfaces.delete(
                resource_group_name=resource_group or self.resource_group,
                network_interface_name=nic)
            operation.wait()
            self.logger.info('"%s" nic removed', nic)
            results.append((nic, operation.status()))
        if not results:
            self.logger.debug('No NICs matching "%s" template were found', nic_template)
        return results

    def remove_pips_by_search(self, pip_template, resource_group=None):
        """
        Used for clean_up jobs to remove public IPs that are not associated to any NIC
        in selected resource_group. If None (default) resource_group provided, the instance's
        resource group is used instead
        """
        self.logger.info('Attempting to List Public IPs with "%s" name template', pip_template)
        results = []
        pip_list = self.list_free_pip(pip_template,
                                      resource_group=resource_group or self.resource_group)

        for pip in pip_list:
            operation = self.network_client.public_ip_addresses.delete(
                resource_group_name=resource_group or self.resource_group,
                public_ip_address_name=pip)
            operation.wait()
            self.logger.info('"%s" pip removed', pip)
            results.append((pip, operation.status()))
        if not results:
            self.logger.debug('No PIPs matching "%s" template were found', pip_template)
            return results

    def create_netsec_group(self, group_name, resource_group=None):
        security_groups = self.network_client.network_security_groups
        self.logger.info("Attempting to Create New Azure Security Group %s", group_name)
        nsg = NetworkSecurityGroup(location=self.region)
        operation = security_groups.create_or_update(resource_group_name=resource_group or
                                                     self.resource_group,
                                                     network_security_group_name=group_name,
                                                     parameters=nsg)
        operation.wait()
        self.logger.info("Network Security Group '%s' is created", group_name)
        return operation.status()

    def remove_netsec_group(self, group_name, resource_group=None):
        """
        Used to remove Network Security Group from selected resource_group.
        If None (default) resource_group provided, the instance's
        resource group is used instead
        """
        self.logger.info("Attempting to Remove Azure Security Group '%s'", group_name)
        security_groups = self.network_client.network_security_groups
        operation = security_groups.delete(resource_group_name=resource_group or
                                           self.resource_group,
                                           network_security_group_name=group_name)
        operation.wait()
        self.logger.info("Network Security Group '%s' is removed", group_name)
        return operation.status()

    def list_load_balancer(self):
        self.logger.info("Attempting to List Azure Load Balancers")
        self.logger.debug('self.region {}'.format(self.region))
        all_lbs = self.network_client.load_balancers.list_all()
        lbs_in_location = []
        for lb in all_lbs:
            if lb.location == self.region:
                lbs_in_location.append(lb.name)
        return lbs_in_location

    def does_load_balancer_exist(self, lb_name):
        return lb_name in self.list_load_balancer()

    def remove_diags_container(self, container_client=None):
        """
        If None (default) container_client provided, the instance's
        container_client is used instead
        """
        container_client = container_client or self.container_client
        for container in container_client.list_containers():
            if container.name.startswith('bootdiagnostics-test'):
                self.logger.info("Removing container '%s'", container.name)
                self.container_client.delete_container(container_name=container.name)
        self.logger.info("All diags containers are removed from '%s'",
                         container_client.account_name)

    def list_blob_images(self, container):
        return [blob.name for blob in self.container_client.list_blobs(container_name=container)
                if blob.name.endswith("vhd" or "vhdx")]

    def remove_blob_image(self, blob, container):
        self.logger.info("Removing Blob '%s' from containter '%s'", blob, container)
        self.container_client.delete_blob(container_name=container, blob_name=blob)
        # delete_blob doesn't return any status

    def copy_blob_image(self, template, vm_name, storage_account,
                        template_container, storage_container):
        # todo: weird method to refactor it later
        container_client = BlockBlobService(storage_account, self.storage_key)
        src_uri = container_client.make_blob_url(container_name=template_container,
                                                 blob_name=template)
        operation = container_client.copy_blob(container_name=storage_container,
                                               blob_name=vm_name + ".vhd",
                                               copy_source=src_uri)
        wait_for(lambda: operation.status != 'pending', num_sec='10m', delay=15)
        # copy operation obj.status->str
        return operation.status

    def list_template(self):
        """
        Returns a list of VHDs/Images which might be used as provision template for Instance
        """
        self.logger.info("Attempting to List Azure VHDs/Images")
        return self.list_blob_images(container=self.template_container)\
            + [item.name for item in self.resource_client.resources.list(
                filter="resourceType eq 'Microsoft.Compute/images'")]

    @contextmanager
    def with_vm(self, *args, **kwargs):
        """Context manager for better cleanup"""
        name = self.deploy_template(*args, **kwargs)
        yield name
        self.delete_vm(name)

    def stack_exist(self, stack_name):
        return stack_name in self.list_stack()

    def delete_stack(self, stack_name, resource_group=None):
        """
        Delete Deployment Stack from 'resource_group'
        """
        self.logger.info("Removes a Deployment Stack resource created with Orchestration")
        deps = self.resource_client.deployments
        operation = deps.delete(resource_group_name=resource_group or self.resource_group,
                                deployment_name=stack_name)
        operation.wait()
        self.logger.info("'%s' was removed from '%s' resource group", stack_name,
                         resource_group or self.resource_group)
        return operation.status()

    def delete_stack_by_date(self, days_old, resource_group=None):
        resource_group = resource_group or self.resource_group
        # todo: to check and refactor this method
        self.logger.info("Removes a Deployment Stack resource older than %s days", days_old)
        results = []
        for stack in self.list_stack(resource_group=resource_group, days_old=days_old):
            self.logger.info("Removing Deployment Stack '%s'", stack)
            result = self.delete_stack(stack_name=stack, resource_group=resource_group)
            results.append((stack, result))
            self.logger.info("Attempt to remove Stack '%s' finished with status '%s'", stack,
                             result)
        return results

    def deploy_template(self, template, vm_name=None, **vm_settings):
        resource_group = vm_settings['resource_group']
        location = vm_settings['region_api']
        subnet = vm_settings['subnet_range']
        address_space = vm_settings['address_space']
        vnet_name = vm_settings['virtual_net']
        vm_size = vm_settings['vm_size']
        storage_container = vm_settings['storage_container']
        # nsg_name = vm_settings['network_nsg']  # todo: check whether nsg is necessary at all

        # allocating public ip address for new vm
        public_ip_params = {
            'location': location,
            'public_ip_allocation_method': 'Dynamic'
        }
        public_ip = self.network_client.public_ip_addresses.create_or_update(
            resource_group_name=resource_group,
            public_ip_address_name=vm_name,
            parameters=public_ip_params
        ).result()

        # creating virtual network
        virtual_networks = self.network_client.virtual_networks
        if vnet_name not in [v.name for v in virtual_networks.list(resource_group)]:
            vnet_params = {
                'location': location,
                'address_space': {
                    'address_prefixes': [address_space, ]
                }
            }
            virtual_networks.create_or_update(
                resource_group_name=resource_group,
                virtual_network_name=vnet_name,
                parameters=vnet_params
            ).result()

        # creating sub net
        subnet_name = 'default'
        subnets = self.network_client.subnets
        if subnet_name not in [v.name for v in subnets.list(resource_group, vnet_name)]:
            vsubnet = subnets.create_or_update(
                resource_group_name=resource_group,
                virtual_network_name=vnet_name,
                subnet_name='default',
                subnet_parameters={'address_prefix': subnet}
            ).result()
        else:
            vsubnet = subnets.get(
                resource_group_name=resource_group,
                virtual_network_name=vnet_name,
                subnet_name='default')

        # creating network interface
        nic_params = {
            'location': location,
            'ip_configurations': [{
                'name': vm_name,
                'public_ip_address': public_ip,
                'subnet': {
                    'id': vsubnet.id
                }
            }]
        }
        nic = self.network_client.network_interfaces.create_or_update(
            resource_group_name=resource_group,
            network_interface_name=vm_name,
            parameters=nic_params
        ).result()

        # preparing os disk
        # todo: replace with copy disk operation
        self.copy_blob_image(template, vm_name, vm_settings['storage_account'],
                             vm_settings['template_container'], storage_container)
        image_uri = self.container_client.make_blob_url(container_name=storage_container,
                                                        blob_name=vm_name)
        # creating virtual machine
        vm_parameters = {
            'location': location,
            'hardware_profile': {
                'vm_size': getattr(VirtualMachineSizeTypes, vm_size)
            },
            'storage_profile': {
                'os_disk': {
                    'os_type': 'Linux',
                    'name': vm_name,
                    'vhd': VirtualHardDisk(uri=image_uri + ".vhd"),
                    'create_option': DiskCreateOptionTypes.attach,
                }
            },
            'network_profile': {
                'network_interfaces': [{
                    'id': nic.id
                }]
            },
        }
        vm = self.compute_client.virtual_machines.create_or_update(
            resource_group_name=resource_group,
            vm_name=vm_name,
            parameters=vm_parameters).result()
        self.wait_vm_running(vm.name, vm_settings['resource_group'])
        return vm.name

    def list_stack_resources(self, stack_name, resource_group=None):
        self.logger.info("Checking Stack %s resources ", stack_name)
        # todo: weird implementation to refactor this method later
        resources = {
            'vms': [],
            'nics': [],
            'pips': [],
        }
        dep_op_list = self.resource_client.deployment_operations.list(
            resource_group_name=resource_group or self.resource_group,
            deployment_name=stack_name
        )
        for dep in dep_op_list:
            if dep.properties.target_resource:
                target = dep.properties.target_resource
                res_type, res_name = (target.resource_type, target.resource_name)

                if res_type == 'Microsoft.Compute/virtualMachines':
                    try:
                        self.compute_client.virtual_machines.get(
                            resource_group_name=resource_group or self.resource_group,
                            vm_name=res_name
                        )
                        res_exists = True
                    except CloudError:
                        res_exists = False
                    resources['vms'].append((res_name, res_exists))
                elif res_type == 'Microsoft.Network/networkInterfaces':
                    try:
                        self.network_client.network_interfaces.get(
                            resource_group_name=resource_group or self.resource_group,
                            network_interface_name=res_name
                        )
                        res_exists = True
                    except CloudError:
                        res_exists = False
                    resources['nics'].append((res_name, res_exists))
                elif res_type == 'Microsoft.Network/publicIpAddresses':
                    # todo: double check this match
                    try:
                        self.network_client.public_ip_addresses.get(
                            resource_group_name=resource_group or self.resource_group,
                            public_ip_address_name=res_name
                        )
                        res_exists = True
                    except CloudError:
                        res_exists = False
                    resources['pips'].append((res_name, res_exists))
        return resources

    def is_stack_empty(self, stack_name, resource_group):
        resources = self.list_stack_resources(stack_name, resource_group=resource_group)
        for resource_type in resources:
            for res_name, exists in resources[resource_type]:
                if exists:
                    return False
        return True

    def info(self):
        pass

    def remove_container_blob(self, container_client, container, blob, remove_snapshots=True):
        self.logger.info("Removing Blob '%s' from containter '%s'", blob.name, container.name)
        try:
            container_client.delete_blob(
                container_name=container.name, blob_name=blob.name)
        except AzureConflictHttpError as e:
            if 'SnapshotsPresent' in str(e) and remove_snapshots:
                self.logger.warn("Blob '%s' has snapshots present, removing them", blob.name)
                container_client.delete_blob(
                    container_name=container.name, blob_name=blob.name, delete_snapshots="include")
            else:
                raise

    def remove_unused_blobs(self, resource_group=None):
        """
        Cleanup script to remove unused blobs: Managed vhds and unmanaged disks
        Runs though all storage accounts in 'resource_group'. If None (default) resource_group
        provided, the instance's resource group is used instead
        Returns list of removed disks
        """
        removed_blobs = {}
        self.logger.info("Attempting to List unused disks/blobs")
        resource_group = resource_group or self.resource_group
        removed_blobs[resource_group] = {}
        for storage_account in self.list_storage_accounts_by_resource_group(resource_group):
            self.logger.info("Checking storage account '%s'", storage_account)
            removed_blobs[resource_group][storage_account] = {}
            # removing unmanaged disks
            key = self.get_storage_account_key(storage_account, resource_group)
            container_client = BlockBlobService(storage_account, key)
            for container in container_client.list_containers():
                removed_blobs[resource_group][storage_account][container.name] = []
                for blob in container_client.list_blobs(container_name=container.name,
                                                        prefix='test'):
                    if blob.properties.lease.status == 'unlocked':
                        self.remove_container_blob(container_client, container, blob)
                        removed_blobs[resource_group][storage_account][container.name].append(
                            blob.name)
            # also delete unused 'bootdiag' containers
            self.remove_diags_container(container_client)

        # removing managed disks
        removed_disks = []
        for disk in self.compute_client.disks.list_by_resource_group(resource_group):
            if disk.name.startswith('test') and disk.managed_by is None:
                self.logger.info("Removing disk '%s'", disk.name)
                self.compute_client.disks.delete(resource_group_name=resource_group,
                                                 disk_name=disk.name)
                removed_disks.append({'resource_group': resource_group,
                                      'disk': disk.name})
        if not removed_disks:
            self.logger.debug("No Managed disks matching 'test*' were found in '%s'",
                              resource_group)
        return {'Managed': removed_disks, 'Unmanaged': removed_blobs}
