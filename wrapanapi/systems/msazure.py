# -*- coding: utf-8 -*-
"""Backend management system classes

Used to communicate with providers without using CFME facilities
"""
from __future__ import absolute_import

import os
from datetime import datetime, timedelta

import pytz
from azure.common import AzureConflictHttpError
from azure.common.credentials import ServicePrincipalCredentials
from azure.common.exceptions import CloudError
from azure.mgmt.compute import ComputeManagementClient
from azure.mgmt.compute.models import (DiskCreateOptionTypes, VirtualHardDisk,
                                       VirtualMachineCaptureParameters,
                                       VirtualMachineSizeTypes)
from azure.mgmt.network import NetworkManagementClient
from azure.mgmt.network.models import NetworkSecurityGroup
from azure.mgmt.resource import ResourceManagementClient, SubscriptionClient
from azure.mgmt.resource.subscriptions.models import SubscriptionState
from azure.mgmt.storage import StorageManagementClient
from azure.storage.blob import BlockBlobService
from cached_property import cached_property
from wait_for import wait_for

from wrapanapi.entities import (Instance, Template, TemplateMixin, VmMixin,
                                VmState)
from wrapanapi.exceptions import (ImageNotFoundError, MultipleImagesError,
                                  VMInstanceNotFound)
from wrapanapi.systems.base import System


class AzureInstance(Instance):
    state_map = {
        'VM starting': VmState.STARTING,
        'VM running': VmState.RUNNING,
        'VM deallocated': VmState.STOPPED,
        'VM stopped': VmState.SUSPENDED,
        'Paused': VmState.PAUSED,
    }

    def __init__(self, system, raw=None, **kwargs):
        """
        Constructor for an AzureInstance tied to a specific system.

        Args:
            system: an AzureSystem object
            raw: azure.mgmt.compute.models.VirtualMachine object if already obtained, or None
            name: name of instance
            resource_group: name of resource group this instance is in
        """
        self._resource_group = kwargs.get('resource_group')
        self._name = kwargs.get('name')
        if not self._name or not self._resource_group:
            raise ValueError("missing required kwargs: 'resource_group', 'name'")

        super(AzureInstance, self).__init__(system, raw, **kwargs)
        self._api = self.system.vms_collection

    @property
    def _identifying_attrs(self):
        return {'name': self._name, 'resource_group': self._resource_group}

    def _wait_on_operation(self, operation):
        if operation:
            operation.wait()
            return True if operation.status().lower() == "succeeded" else False
        self.logger.warning(
            "wait_on_operation got operation=None, expected an OperationStatusResponse")
        return True

    @property
    def name(self):
        return self._name

    @property
    def uuid(self):
        return self.raw.id

    def refresh(self):
        """
        Update instance's raw data

        Ensure that this VM still exists AND provisioning was successful on azure
        """
        try:
            vm = self._api.get(
                resource_group_name=self._resource_group, vm_name=self._name, expand='instanceView')
        except CloudError as e:
            if e.response.status_code == 404:
                raise VMInstanceNotFound(self._name)
            else:
                raise

        first_status = vm.instance_view.statuses[0]
        if first_status.display_status == 'Provisioning failed':
            raise VMInstanceNotFound('provisioning failed for VM {}'.format(self._name))
        self.raw = vm
        return self.raw

    def _get_state(self):
        self.logger.info("retrieving azure VM status for '%s'", self.name)
        self.refresh()
        last_power_status = self.raw.instance_view.statuses[-1].display_status
        self.logger.info("returned status was '%s'", last_power_status)
        return self._api_state_to_vmstate(last_power_status)

    def get_network_interfaces(self):
        self.refresh()
        return self.raw.network_profile.network_interfaces

    @property
    def ip(self):
        """
        Return current public IPv4 address of the primary ip_config of the primary nic

        To get IP we have to fetch:
            - nic object from VM
            - ip_config object from nic
            - public_ip object from ip_config

        e.g. - /subscriptions/<subscription>/resourceGroups/<resource_group>/providers/
        Microsoft.Network/publicIPAddresses/<object_name>
        Returns :
            ip addres (str) -- IPv4 Public IP of the primary ip_config of the primary nic
            None -- if no public IP found
        """
        self.refresh()
        network_client = self.system.network_client

        # Getting id of the first network interface of the vm
        for nic in self.raw.network_profile.network_interfaces:
            # nic.primary is None when we have only one network interface attached to the VM
            if nic.primary is not False:
                first_vm_if_id = nic.id
                break

        if not first_vm_if_id:
            self.logger.error("Unable to get first vm nic interface for vm %s", self.name)
            return None

        if_name = os.path.split(first_vm_if_id)[1]
        if_obj = network_client.network_interfaces.get(self._resource_group, if_name)

        # Getting name of the first IP configuration of the network interface
        for ip_config in if_obj.ip_configurations:
            if ip_config.primary is True:
                ip_config_name = ip_config.name
                break

        ip_config_obj = network_client.network_interface_ip_configurations.get(
            self._resource_group, if_name, ip_config_name)

        # Getting public IP id from the IP configuration object
        try:
            pub_ip_id = ip_config_obj.public_ip_address.id
        except AttributeError:
            self.logger.error(
                "VM '%s' doesn't have public IP on %s:%s", self.name, if_name, ip_config_name)
            return None

        pub_ip_name = os.path.split(pub_ip_id)[1]
        public_ip = network_client.public_ip_addresses.get(self._resource_group, pub_ip_name)
        if not hasattr(public_ip, 'ip_address') or not public_ip.ip_address:
            # Dynamic ip will be allocated for Running VMs only
            self.logger.error(
                "Couldn't get Public IP of vm '%s'.  public_ip_allocation_method -- '%s'. ",
                self.name, public_ip.public_ip_allocation_method
            )
            return None

        return public_ip.ip_address

    @property
    def type(self):
        return self.raw.hardware_profile.vm_size

    @property
    def creation_time(self):
        self.refresh()
        return self.raw.instance_view.statuses[0].time

    def delete(self):
        self.logger.info("deleting vm '%s'", self.name)
        operation = self._api.delete(
            resource_group_name=self._resource_group, vm_name=self.name)
        return self._wait_on_operation(operation)

    def cleanup(self):
        """
        Clean up a VM

        Deletes VM, then also deletes NICs/PIPs associated with the VM.
        Any exceptions raised during NIC/PIP delete attempts are logged only.
        """
        self.delete()
        self.logger.info("cleanup: removing NICs/PIPs for VM '%s'", self.name)
        try:
            self.system.remove_nics_by_search(self.name, self._resource_group)
            self.system.remove_pips_by_search(self.name, self._resource_group)
        except Exception:
            self.logger.exception(
                "cleanup: failed to cleanup NICs/PIPs for VM '%s'", self.name)
        return True

    def start(self):
        self.logger.info("starting vm '%s'", self.name)
        operation = self._api.start(
            resource_group_name=self._resource_group, vm_name=self.name)
        if self._wait_on_operation(operation):
            self.wait_for_state(VmState.RUNNING)
            return True
        return False

    def stop(self):
        self.logger.info("stopping vm '%s'", self.name)
        operation = self._api.deallocate(
            resource_group_name=self._resource_group, vm_name=self.name)
        if self._wait_on_operation(operation):
            self.wait_for_state(VmState.STOPPED)
            return True
        return False

    def restart(self):
        self.logger.info("restarting vm '%s'", self.name)
        operation = self._api.restart(
            resource_group_name=self._resource_group, vm_name=self.name)
        if self._wait_on_operation(operation):
            self.wait_for_state(VmState.RUNNING)
            return True
        return False

    def suspend(self):
        self.logger.info("suspending vm '%s'", self.name)
        operation = self._api.power_off(
            resource_group_name=self._resource_group, vm_name=self.name)
        if self._wait_on_operation(operation):
            self.wait_for_state(VmState.SUSPENDED)
            return True
        return False

    def capture(self, container, image_name, overwrite_vhds=True):
        self.logger.info("Attempting to Capture Azure VM '%s'", self.name)
        params = VirtualMachineCaptureParameters(vhd_prefix=image_name,
                                                 destination_container_name=container,
                                                 overwrite_vhds=overwrite_vhds)
        self.stop()
        self.logger.info("Generalizing VM '%s'", self.name)
        operation = self._api.generalize(
            resource_group_name=self._resource_group, vm_name=self.name)
        self._wait_on_operation(operation)
        self.logger.info("Capturing VM '%s'", self.name)
        operation = self._api.capture(
            resource_group_name=self._resource_group, vm_name=self.name, parameters=params)
        return self._wait_on_operation(operation)

    def get_vhd_uri(self):
        self.logger.info("attempting to Retrieve Azure VM VHD %s", self.name)
        self.refresh()
        vhd_endpoint = self.raw.storage_profile.os_disk.vhd.uri
        self.logger.info("Returned Disk Endpoint was %s", vhd_endpoint)
        return vhd_endpoint


class AzureBlobImage(Template):
    def __init__(self, system, raw=None, **kwargs):
        """
        Constructor for an AzureBlobImage on a specific system

        Note this is not a Microsoft.Compute image, it is just a .vhd/.vhdx
        stored in blob storage.

        Args:
            system: an AzureSystem object
            raw: the azure.storage.blob.models.Blob object if already obtained, or None
            name: name of template
            container: container the template is stored in
        """
        self._name = kwargs.get('name')
        self._container = kwargs.get('container')
        if not self._name or not self._container:
            raise ValueError("missing required kwargs: 'name', 'container'")

        super(AzureBlobImage, self).__init__(system, raw, **kwargs)
        self._api = self.system.container_client

    @property
    def _identifying_attrs(self):
        return {'name': self._name, 'container': self._container}

    @property
    def name(self):
        return self._name

    @property
    def uuid(self):
        """
        Return blob URL

        There doesn't seem to be a uuid associated with Azure Blobs
        """
        return self._api.make_blob_url(self._container, self._name)

    def refresh(self):
        try:
            self.raw = self._api.get_blob_properties(self._container, self._name)
        except CloudError as e:
            if e.response.status_code == 404:
                raise ImageNotFoundError(self._name)
        return self.raw

    def delete(self, delete_snapshots=True):
        kwargs = {}
        if delete_snapshots:
            kwargs['delete_snapshots'] = 'include'
        self._api.delete_blob(self._container, self.name, **kwargs)

    def delete_snapshots_only(self):
        self._api.delete_blob(self._container, self.name, delete_snapshots='only')

    def cleanup(self):
        return self.delete()

    def deploy(self, vm_name, **vm_settings):
        # TODO: this method is huge, it should be broken up ...
        # TODO #2: check args of vm_settings better
        # TODO #3: possibly use compute images instead of blob images?
        resource_group = vm_settings.get('resource_group', self.system.resource_group)
        location = vm_settings.get('region_api', self.system.region)
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
        public_ip = self.system.network_client.public_ip_addresses.create_or_update(
            resource_group_name=resource_group,
            public_ip_address_name=vm_name,
            parameters=public_ip_params
        ).result()

        # creating virtual network
        virtual_networks = self.system.network_client.virtual_networks
        if vnet_name not in [v.name for v in virtual_networks.list(resource_group)]:
            vnet_params = {
                'location': location,
                'address_space': {
                    'address_prefixes': [address_space]
                }
            }
            virtual_networks.create_or_update(
                resource_group_name=resource_group,
                virtual_network_name=vnet_name,
                parameters=vnet_params
            ).result()

        # creating sub net
        subnet_name = 'default'
        subnets = self.system.network_client.subnets
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

        def _create_or_update_nic():
            return self.system.network_client.network_interfaces.create_or_update(
                resource_group_name=resource_group,
                network_interface_name=vm_name,
                parameters=nic_params
            ).result()

        try:
            nic = _create_or_update_nic()
        except CloudError:
            # Try one more time if we hit an error
            nic = _create_or_update_nic()

        # preparing os disk
        # todo: replace with copy disk operation
        self.system.copy_blob_image(
            self.name, vm_name, vm_settings['storage_account'],
            self._container, storage_container
        )
        image_uri = self.system.container_client.make_blob_url(
            container_name=storage_container, blob_name=vm_name)
        # creating virtual machine
        vm_parameters = {
            'location': location,
            'hardware_profile': {
                'vm_size': getattr(VirtualMachineSizeTypes, vm_size)
            },
            'storage_profile': {
                'os_disk': {
                    'os_type': 'Linux',  # TODO: why is this hardcoded?
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
        vm = self.system.compute_client.virtual_machines.create_or_update(
            resource_group_name=resource_group,
            vm_name=vm_name,
            parameters=vm_parameters).result()
        vm = AzureInstance(
            system=self.system, name=vm.name,
            resource_group=vm_settings['resource_group'], raw=vm)
        vm.wait_for_state(VmState.RUNNING)
        return vm


class AzureSystem(System, VmMixin, TemplateMixin):
    """This class is used to connect to Microsoft Azure Portal via PowerShell AzureRM Module
    """
    _stats_available = {
        'num_vm': lambda self: len(self.list_vms()),
        'num_template': lambda self: len(self.list_templates()),
    }

    can_suspend = True
    can_pause = False

    def __init__(self, **kwargs):
        super(AzureSystem, self).__init__(**kwargs)
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

    @property
    def _identifying_attrs(self):
        return {
            'tenant': self.tenant,
            'subscription_id': self.subscription_id,
            'storage_account': self.storage_account
        }

    @property
    def can_suspend(self):
        return True

    @property
    def can_pause(self):
        return False

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

    def create_vm(self, vm_name, *args, **kwargs):
        raise NotImplementedError

    def find_vms(self, name=None, resource_group=None):
        """
        Returns list of Instances in current Region

        Can be filtered by: vm_name or resource_group

        If those are not specified all VMs are returned in the region
        """
        vm_list = []
        resource_groups = [resource_group] if resource_group else self.list_resource_groups()
        for res_group in resource_groups:
            vms = self.vms_collection.list(resource_group_name=res_group)
            vm_list.extend([
                AzureInstance(system=self, name=vm.name, resource_group=res_group, raw=vm)
                for vm in vms if vm.location == self.region
            ])
        if name:
            return [vm for vm in vm_list if vm.name == name]
        return vm_list

    def list_vms(self, resource_group=None):
        return self.find_vms(resource_group=resource_group)

    def get_vm(self, name):
        vms = self.find_vms(name=name)
        if not vms:
            raise VMInstanceNotFound(name)
        # Azure VM names are unique across whole cloud, there should only be 1 item in the list
        return vms[0]

    def data(self, vm_name, resource_group=None):
        raise NotImplementedError('data not implemented.')

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
        return [
            s.name for s in
            self.storage_client.storage_accounts.list_by_resource_group(resource_group)
        ]

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
        self.logger.debug('self.region=%s', self.region)
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

        self.logger.debug('self.region=%s', self.region)
        subnets = dict()
        for network in all_networks:
            if network.location == self.region:
                subnets[network.name] = [subnet.name for subnet in network.subnets]

        return subnets

    def list_security_group(self):
        self.logger.info("Attempting to List Azure security groups")
        all_sec_groups = self.network_client.network_security_groups.list_all()
        self.logger.debug('self.region=%s', self.region)
        location = self.region.replace(' ', '').lower()
        sec_groups_in_location = []
        for sec_gp in all_sec_groups:
            if sec_gp.location == location:
                sec_groups_in_location.append(sec_gp.name)
        return sec_groups_in_location

    def list_router(self):
        self.logger.info("Attempting to List Azure routes table")
        all_routers = self.network_client.route_tables.list_all()
        self.logger.debug("self.region='%s", self.region)
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
            try:
                operation = self.network_client.network_interfaces.delete(
                    resource_group_name=resource_group or self.resource_group,
                    network_interface_name=nic)
            except CloudError as e:
                self.logger.error('{} nic can\'t be removed - {}'.format(nic, e.error.error))
                results.append((nic, e.error.error))
                continue
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
        self.logger.debug('self.region=%s', self.region)
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

    def _remove_container_blob(self, container_client, container, blob, remove_snapshots=True):
        # Redundant with AzureBlobImage.delete(), but used below in self.remove_unused_blobs()
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
                        self._remove_container_blob(container_client, container, blob)
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

    def create_template(self, *args, **kwargs):
        raise NotImplementedError

    def find_templates(self, name=None, container=None, prefix=None, only_vhd=True):
        """
        Find all templates, optionally filtering by given name, optionally filtering by container

        NOTE: this is searching for blob images (.vhd/.vhdx files stored in containers), use
        list_compute_images() to get Microsoft.Compute images.

        Args:
            name (str): optional, name to search for
            container (str): optional, container to search in
            prefix (str): optional, name prefix to search for
            only_vhd (bool): default True, only return .vhd/.vhdx blobs
        Returns:
            list of AzureImage objects
        """
        matches = []
        for found_container in self.container_client.list_containers():
            found_container_name = found_container.name
            if container and found_container_name.lower() != container.lower():
                continue
            for image in self.container_client.list_blobs(found_container_name, prefix=prefix):
                img_name = image.name
                if only_vhd and (not img_name.endswith('.vhd') and not img_name.endswith('.vhdx')):
                    continue
                if name and name.lower() != img_name.lower():
                    continue
                matches.append(
                    AzureBlobImage(
                        system=self, name=img_name, container=found_container_name, raw=image))
        return matches

    def list_templates(self):
        """
        Return all templates in all containers
        """
        return self.find_templates()

    def list_compute_images(self):
        return self.resource_client.resources.list(
            filter="resourceType eq 'Microsoft.Compute/images'")

    def list_all_image_names(self):
        blob_image_names = [item.name for item in self.find_templates()]
        compute_image_names = [item.name for item in self.list_compute_images()]
        return set(blob_image_names.extend(compute_image_names))

    def get_template(self, name, container=None):
        """
        Return template with given name

        Raises MultipleItemsError if more than 1 exist with that name

        Args:
            container (str) -- specific container to search in
        """
        templates = self.find_templates(name=name, container=container)
        if not templates:
            raise ImageNotFoundError(name)
        elif len(templates) > 1:
            raise MultipleImagesError(
                "Image with name '{}' exists in multiple containers".format(name)
            )
        return templates[0]

    # TODO: Refactor the below stack methods into the StackMixin/StackEntity structure
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
            for _, exists in resources[resource_type]:
                if exists:
                    return False
        return True

    def info(self):
        pass
