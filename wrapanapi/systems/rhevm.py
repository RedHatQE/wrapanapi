# coding: utf-8
"""Backend management system classes

Used to communicate with providers without using CFME facilities
"""
from __future__ import absolute_import

import fauxfactory
import pytz
from ovirtsdk4 import NotFoundError as OVirtNotFoundError
from ovirtsdk4 import Connection, Error, types
from wait_for import TimedOutError, wait_for

from wrapanapi.entities import Template, TemplateMixin, Vm, VmMixin, VmState
from wrapanapi.exceptions import (
    ItemNotFound, MultipleItemsError, NotFoundError, VMInstanceNotFound, VMInstanceNotSuspended,
    VMNotFoundViaIP, ResourceAlreadyExistsException)
from wrapanapi.systems.base import System


class _SharedMethodsMixin(object):
    """
    Mixin class that holds properties/methods both VM's and templates share.

    This should be listed first in the child class inheritance to satisfy
    the methods required by the Vm/Template abstract base class
    """
    @property
    def _identifying_attrs(self):
        return {'uuid': self._uuid}

    def refresh(self):
        """
        Re-pull the data for this entity and update this instance's attributes
        """
        try:
            self._raw = self.api.get()
        except OVirtNotFoundError:
            raise ItemNotFound(self.uuid, self.__class__.__name__)

    @property
    def name(self):
        """
        Returns name of entity
        """
        return self.raw.name

    @property
    def uuid(self):
        """
        Returns unique ID of entity
        """
        return self._uuid

    def _get_nic_service(self, nic_name):
        for nic in self.api.nics_service().list():
            if nic.name == nic_name:
                return self.api.nics_service().nic_service(nic.id)
        else:
            raise NotFoundError('Unable to find NicService for nic {} on {}'.format(nic_name, self))

    def _get_network(self, network_name):
        """retreive a network object by name"""
        networks = self.system.api.system_service().networks_service().list(
            search='name={}'.format(network_name))
        try:
            return networks[0]
        except IndexError:
            raise NotFoundError('No match for network by "name={}"'.format(network_name))

    def get_nics(self):
        return self.api.nics_service().list()

    def _nic_action(self, nic, network_name, interface='VIRTIO', on_boot=True,
                   vnic_profile=None, nic_service=None, action='add'):
        """Call an action on nic_service, could be a vmnic or vmnics service
        example, action 'add' on vmnicsservice, or 'update' on VmNicService
        currently written for nic actions on the service, though other actions are available

        Args:
            nic: the Nic object itself, could be existing on the vm or not
            network_name: string name of the network, also default for vnic_profile name if empty
            interface: string interface type for ovirt, interfaces are resolved to a specific type
            on_boot: boolean, kwarg for nic options
            vnic_profile: string name of the vnic_profile, network_name is used if empty
            nic_service: the VmNicsService or VmNicService, defaults to VmNicsService
            action: string action method to call on the service, defaults to add (VmNicsService)
        """
        # TODO take kwargs and match them to types.Nic attributes so callers can set any property
        service = nic_service or self.api.nics_service()
        nic.network = self._get_network(network_name)
        vnic_name = vnic_profile or network_name
        vnic_list = self.system.api.system_service().vnic_profiles_service().list()
        try:
            nic.vnic_profile = [v_nic for v_nic in vnic_list if v_nic.name == vnic_name][0]
        except IndexError:
            raise NotFoundError('Unable to find vnic_profile matching name {}'.format(vnic_name))
        nic.interface = getattr(types.NicInterface, interface)
        nic.on_boot = on_boot
        # service attribute should be method we can call and pass the nic to
        getattr(service, action)(nic)

    def add_nic(self, network_name, nic_name='nic1', interface='VIRTIO', on_boot=True,
                vnic_profile=None):
        """Add a nic to VM/Template

        Args:
            network_name: string name of the network, also default for vnic_profile name if empty
            nic_name: string name of the nic to add
            interface: string interface type for ovirt, interfaces are resolved to a specific type
            on_boot: boolean, kwarg for nic options
            vnic_profile: string name of the vnic_profile, network_name is used if empty

        Raises:
            ResourceAlreadyExistsException: method checks if the nic already exists
        """
        try:
            self._get_nic_service(nic_name)
        except NotFoundError:
            pass
        else:
            raise ResourceAlreadyExistsException('Nic with name {} already exists on {}'
                                                 .format(nic_name, self.name))
        nics_service = self.api.nics_service()
        nic = types.Nic(name=nic_name)
        self._nic_action(nic, network_name, interface, on_boot, vnic_profile,
                         nics_service, action='add')

    def update_nic(self, network_name, nic_name='nic1', interface='VIRTIO', on_boot=True,
                   vnic_profile=None):
        """Update a nic on VM/Template
        Args:
            network_name: string name of the network, also default for vnic_profile name if empty
            nic_name: string name of the nic to add
            interface: string interface type for ovirt, interfaces are resolved to a specific type
            on_boot: boolean, kwarg for nic options
            vnic_profile: string name of the vnic_profile, network_name is used if empty

        Raises:
            NotFoundError: from _get_nic_service call if the name doesn't exist
        """
        nic_service = self._get_nic_service(nic_name)
        self._nic_action(nic_service.get(), network_name, interface, on_boot, vnic_profile,
                         nic_service, action='update')


class RHEVMVirtualMachine(_SharedMethodsMixin, Vm):
    """
    Represents a VM entity on RHEV
    """
    state_map = {
        'up': VmState.RUNNING,
        'down': VmState.STOPPED,
        'powering_up': VmState.STARTING,
        'suspended': VmState.SUSPENDED,
    }

    def __init__(self, system, raw=None, **kwargs):
        """
        Constructor for a RHEV VM tied to a specific system

        Args:
            system - instance of wrapanapi.systems.RHEVMSystem
            raw - raw ovirtsdk4.types.Vm object (if already obtained)
            uuid - vm ID
        """
        super(RHEVMVirtualMachine, self).__init__(system, raw, **kwargs)
        self._uuid = raw.id if raw else kwargs.get('uuid')
        if not self._uuid:
            raise ValueError("missing required kwarg: 'uuid'")
        self.api = system.api.system_service().vms_service().vm_service(self._uuid)

    @property
    def cluster(self):
        self.refresh()
        return self.raw.cluster

    def delete(self):
        """
        Removes the entity on the provider
        """
        self.ensure_state(VmState.STOPPED)
        self.logger.debug(' Deleting RHEV VM %s/%s', self.name, self.uuid)

        self.api.remove()

        wait_for(
            lambda: not self.exists,
            message="wait for RHEV VM '{}' deleted".format(self.uuid),
            num_sec=300
        )
        return True

    def cleanup(self):
        """
        Removes the entity on the provider and any of its associated resources

        This should be more than a simple delete, though if that takes care of
        the job and cleans up everything, simply calling "self.delete()" works
        """
        return self.delete()

    def rename(self, new_name):
        try:
            result = self.api.update(types.Vm(name=new_name))
            if not result:
                raise Exception("Update API call returned 'false'")
        except Exception:
            self.logger.exception("Failed to rename VM %s to %s", self.name, new_name)
            return False
        else:
            self.logger.info(
                "RHEVM VM '%s' renamed to '%s', now restarting", self.name, new_name)
            self.restart()  # Restart is required for a rename in RHEV
            self.refresh()  # Update raw so we pick up the new name
            return True

    def _get_state(self):
        """
        Returns VMState object representing the VM's current state

        Should always refresh to get the latest status from the API
        """
        self.refresh()
        return self._api_state_to_vmstate(self.raw.status.value)

    @property
    def ip(self):
        """
        Returns IP address of the VM/instance
        """
        rep_dev_service = self.api.reported_devices_service()
        try:
            first = rep_dev_service.list()[0]
            return first.ips[0].address
        except IndexError:
            return None

    @property
    def all_ips(self):
        ips = []
        rep_dev_service = self.api.reported_devices_service()
        for dev in rep_dev_service.list():
            for listed_ip in dev.ips:
                ips.append(listed_ip.address)
        return ips

    @property
    def creation_time(self):
        """
        Returns creation time of VM/instance
        """
        self.refresh()
        return self.raw.creation_time.astimezone(pytz.UTC)

    def start(self):
        """
        Starts the VM/instance. Blocks until task completes.

        Returns: True if vm action has been initiated properly
        """
        self.wait_for_steady_state()
        self.logger.info(' Starting RHEV VM %s', self.name)
        if self.is_running:
            self.logger.info(' RHEV VM %s is already running.', self.name)
            return True
        else:
            self.api.start()
            self.wait_for_state(VmState.RUNNING)
            return True

    def stop(self):
        """
        Stops the VM/instance. Blocks until task completes.

        Returns: True if vm action has been initiated properly
        """
        self.wait_for_steady_state()
        self.logger.info(' Stopping RHEV VM %s', self.name)
        if self.is_stopped:
            self.logger.info(' RHEV VM %s is already stopped.', self.name)
            return True
        else:
            self.api.stop()
            self.wait_for_state(VmState.STOPPED)
            return True

    def restart(self):
        """
        Restarts the VM/instance. Blocks until task completes.

        Returns: True if vm action has been initiated properly
        """
        self.logger.debug(' Restarting RHEV VM %s', self.name)
        return self.stop() and self.start()

    def suspend(self):
        """
        Suspends the VM/instance.  Blocks until task completes.

        Returns: True if vm action has been initiated properly
        """
        self.wait_for_steady_state()
        self.logger.debug(' Suspending RHEV VM %s', self.name)
        if self.is_stopped:
            # TODO: possibly use ensure_state(VmState.RUNNING) here?
            raise VMInstanceNotSuspended(self.name)
        elif self.is_suspended:
            self.logger.info(' RHEV VM %s is already suspended.', self.name)
            return True
        else:
            self.api.suspend()
            self.wait_for_state(VmState.SUSPENDED)
            return True

    def mark_as_template(self, template_name=None, cluster=None, delete=True, delete_on_error=True,
                         **kwargs):
        """Turns the VM off, creates template from it and deletes the original VM.

        Mimics VMware behaviour here.
        Delete also controls renaming
        If delete is false and no template_name provided, auto-generated mrk_tmpl_<hash>
         becomes the final template name, as we can't use the vm name
        In other words, can only rename when template_name != vm_name, or when delete is true
        Args:
            delete: Whether to delete the VM (default: True)
            template_name: If you want, you can specific an exact template name
            delete_on_error: delete on timeout as well.

        Returns:
        wrapanapi.systems.rhevm.RHEVMTemplate object
        """
        temp_template_name = template_name or "mrk_tmpl_{}".format(fauxfactory.gen_alphanumeric(8))
        try:
            # Check if this template already exists and ensure it is in an OK state...
            create_new_template = True
            if self.system.does_template_exist(temp_template_name):
                try:
                    template = self.system.get_template(temp_template_name)
                    template.wait_for_ok_status()
                except NotFoundError:
                    pass  # It got deleted.
                else:
                    create_new_template = False

            # Template does not exist, so create a new one...
            if create_new_template:
                self.ensure_state(VmState.STOPPED)
                # Create template based on this VM
                template = self.system.create_template(
                    template_name=temp_template_name, vm_name=self.name, cluster_name=cluster)
            if delete and self.exists:
                # Delete the original VM
                self.delete()
            # if template_name was passed, it was used in creating the template, no rename needed
            # rename back to the VM name only if no template_name passed and delete
            if not template_name and delete:
                template.rename(self.name)
        except TimedOutError:
            self.logger.error("Hit TimedOutError marking VM as template")
            if delete_on_error:
                try:
                    template.delete()
                except Exception:
                    self.logger.exception("Failed to delete template when cleaning up")
            raise
        return template

    def get_hardware_configuration(self):
        self.refresh()
        return {
            'ram': self.raw.memory,
            'cpu': self.raw.cpu.topology.cores * self.raw.cpu.topology.sockets
        }

    def _get_disk_attachment_service(self, disk_name):
        disk_attachments_service = self.api.disk_attachments_service()
        for disk_attachment_service in disk_attachments_service.list():
            disk = self.system.api.follow_link(disk_attachment_service.disk)
            if disk.name == disk_name:
                return disk_attachments_service.service(disk.id)
        raise ItemNotFound(disk_name, 'disk')

    def is_disk_attached(self, disk_name):
        try:
            return bool(self._get_disk_attachment_service(disk_name))
        except ItemNotFound:
            return False

    def get_disks_count(self):
        return len(self.api.disk_attachments_service().list())

    def _is_disk_ok(self, disk_id):
        disk = [self.system.api.follow_link(disk_attach.disk)
                for disk_attach in self.api.disk_attachments_service().list()
                if self.system.api.follow_link(disk_attach.disk).id == disk_id].pop()
        return getattr(disk, 'status', None) == types.DiskStatus.OK

    def add_disk(self, storage_domain=None, size=None, interface='VIRTIO', format=None,
                 active=True):
        """
        Add disk to VM

        Args:
            storage_domain: string name of the storage domain (datastore)
            size: integer size of disk in bytes, ex 8GB: 8*1024*1024
            interface: string disk interface type
            format: string disk format type
            active: boolean whether the disk is active
        Returns: None
        Notes:
            Disk format and interface type definitions, and their valid values,
            can be found in ovirtsdk documentation:
            http://ovirt.github.io/ovirt-engine-sdk/4.1/types.m.html#ovirtsdk4.types.DiskInterface
            http://ovirt.github.io/ovirt-engine-sdk/4.1/types.m.html#ovirtsdk4.types.DiskFormat
        """
        disk_attachments_service = self.api.disk_attachments_service()
        disk_attach = types.DiskAttachment(
            disk=types.Disk(format=types.DiskFormat(format),
                            provisioned_size=size,
                            storage_domains=[types.StorageDomain(name=storage_domain)]),
            interface=getattr(types.DiskInterface, interface),
            active=active
        )
        disk_attachment = disk_attachments_service.add(disk_attach)
        wait_for(self._is_disk_ok, func_args=[disk_attachment.disk.id], delay=5, num_sec=900,
                 message="check if disk is attached")

    def connect_direct_lun(self, lun_name=None, lun_ip_addr=None, lun_port=None,
                           lun_iscsi_target=None, interface=None):
        """
        Connects a direct lun disk to the VM.

        Args:
            lun_name: name of LUN
            lun_ip_addr: LUN ip address
            lun_port: LUN port
            lun_iscsi_target: iscsi target
        """
        disk_attachments_service = self.api.disk_attachments_service()
        if not self.system.does_disk_exist(lun_name):
            disk_attachment = types.DiskAttachment(
                disk=types.Disk(
                    name=lun_name,
                    shareable=True,
                    format='raw',
                    lun_storage=types.HostStorage(
                        type=types.StorageType.ISCSI,
                        logical_units=[
                            types.LogicalUnit(
                                address=lun_ip_addr,
                                port=lun_port,
                                target=lun_iscsi_target,
                            )
                        ]
                    )
                ),
                interface=types.DiskInterface(getattr(types.DiskInterface, interface or 'VIRTIO')),
                active=True
            )
        else:
            disk_attachment = self._get_disk_attachment_service(lun_name).get()
        disk_attachments_service.add(disk_attachment)
        wait_for(
            self._is_disk_ok, func_args=[disk_attachment.disk.id], delay=5, num_sec=900,
            message="check if disk is attached"
        )
        return True

    def disconnect_disk(self, disk_name):
        """Disconnect a disk from the VM"""
        if not self.is_disk_attached(disk_name):
            self.logger.info("Disk with name '%s' is not attached to VM '%s'", disk_name, self.name)
            return True
        disk_attachment_service = self._get_disk_attachment_service(disk_name)
        disk_attachment_service.remove(detach_only=True, wait=True)
        wait_for(
            lambda: not self.is_disk_attached(disk_name),
            delay=5, num_sec=900, message="disk to no longer be attached"
        )
        return True


class RHEVMTemplate(_SharedMethodsMixin, Template):
    """
    Represents a template entity on RHEV.
    """
    def __init__(self, system, raw=None, **kwargs):
        """
        Constructor for a RHEV template tied to a specific system

        Args:
            system - instance of wrapanapi.systems.RHEVMSystem
            raw - raw ovirtsdk4.types.Vm object (if already obtained)
            uuid - template ID
        """
        super(RHEVMTemplate, self).__init__(system, raw=None, **kwargs)
        self._uuid = raw.id if raw else kwargs.get('uuid')
        if not self._uuid:
            raise ValueError("missing required kwarg: 'uuid'")
        self.api = system.api.system_service().templates_service().template_service(self._uuid)

    @property
    def _identifying_attrs(self):
        return {'uuid': self._uuid}

    def refresh(self):
        """
        Re-pull the data for this entity and update this instance's attributes
        """
        try:
            self._raw = self.api.get()
        except OVirtNotFoundError:
            raise ItemNotFound(self.uuid, self.__class__.__name__)

    @property
    def name(self):
        """
        Returns name of template
        """
        return self.raw.name

    @property
    def uuid(self):
        """
        Returns unique ID of template
        """
        return self._uuid

    def delete(self, timeout=120):
        """
        Removes the entity on the provider

        Args:
            timeout: time to wait for template to be successfully deleted
        """
        self.logger.debug(' Deleting RHEV template %s/%s', self.name, self.uuid)
        self.wait_for_ok_status()
        self.api.remove()
        wait_for(lambda: not self.exists, num_sec=timeout, delay=5)

    def cleanup(self):
        """
        Removes the entity on the provider and any of its associated resources

        This should be more than a simple delete, though if that takes care of
        the job and cleans up everything, simply calling "self.delete()" works
        """
        return self.delete()

    def rename(self, new_name):
        try:
            result = self.api.update(types.Template(name=new_name))
            if not result:
                raise Exception("Update API call returned 'false'")
        except Exception:
            self.logger.exception("Failed to rename template %s to %s", self.name, new_name)
            return False
        else:
            # Update raw so we pick up the new name
            self.refresh()
            return True

    def wait_for_ok_status(self):
        wait_for(
            lambda: self.api.get().status == types.TemplateStatus.OK,
            num_sec=30 * 60, message="template is OK", delay=45)

    def deploy(self, vm_name, **kwargs):
        """
        Deploy a VM using this template

        Args:
            vm_name -- name of VM to create
            cluster -- cluster to which VM should be deployed
            timeout (optional) -- default 900
            power_on (optional) -- default True
            placement_policy_host (optional)
            placement_policy_affinity (optional)
            cpu (optional) -- number of cpu cores
            sockets (optional) -- numbner of cpu sockets
            ram (optional) -- memory in GB

        Returns:
            wrapanapi.systems.rhevm.RHEVMVirtualMachine
        """
        self.logger.debug(' Deploying RHEV template %s to VM %s', self.name, vm_name)
        timeout = kwargs.pop('timeout', 900)
        power_on = kwargs.pop('power_on', True)
        vm_kwargs = {
            'name': vm_name,
            'cluster': self.system.get_cluster(kwargs['cluster']),
            'template': self.raw,
        }
        if 'placement_policy_host' in kwargs and 'placement_policy_affinity' in kwargs:
            host = types.Host(name=kwargs['placement_policy_host'])
            policy = types.VmPlacementPolicy(
                hosts=[host],
                affinity=kwargs['placement_policy_affinity'])
            vm_kwargs['placement_policy'] = policy
        if 'cpu' in kwargs:
            vm_kwargs['cpu'] = types.Cpu(
                topology=types.CpuTopology(
                    cores=kwargs['cpu'],
                    sockets=kwargs.pop('sockets')
                )
            )
        if 'ram' in kwargs:
            vm_kwargs['memory'] = int(kwargs['ram'])  # in Bytes
        vms_service = self.system.api.system_service().vms_service()
        vms_service.add(types.Vm(**vm_kwargs))
        vm = self.system.get_vm(vm_name)
        vm.wait_for_state(VmState.STOPPED, timeout=timeout)
        if power_on:
            vm.start()
        return vm


class RHEVMSystem(System, VmMixin, TemplateMixin):
    """
    Client to RHEVM API

    This class piggy backs off ovirtsdk.

    Benefits of ovirtsdk:

    * Don't need intimite knowledge w/ RHEVM api itself.

    Detriments of ovirtsdk:

    * Response to most quaries are returned as an object rather than a string.
      This makes it harder to do simple stuff like getting the status of a vm.
    * Because of this, it makes listing VMs based on \*\*kwargs impossible
      since ovirtsdk relies on re class to find matches.

      * | For example: List out VM with this name (positive case)
        | Ideal: self.api.vms.list(name='test_vm')
        | Underneath the hood:

        * ovirtsdk fetches list of all vms [ovirtsdk.infrastructure.brokers.VM
          object, ...]
        * ovirtsdk then tries to filter the result using re.

          * tries to look for 'name' attr in ovirtsdk.infrastructure.brokers.VM
            object
          * found name attribute, in this case, the type of the value of the
            attribute is string.
          * match() succeed in comparing the value to 'test_vm'

      * | For example: List out VM with that's powered on (negative case)
        | Ideal: self.api.vms.list(status='up')
        | Underneath the hood:

        * **same step as above except**

          * found status attribute, in this case, the type of the value of
            the attribute is ovirtsdk.xml.params.Status
          * match() failed because class is compared to string 'up'

     This problem should be attributed to how RHEVM api was designed rather
     than how ovirtsdk handles RHEVM api responses.

    * Obj. are not updated after action calls.

      * For example::
          vm = api.vms.get(name='test_vm')
          vm.status.get_state() # returns 'down'
          vm.start()
          # wait a few mins
          vm.status.get_state() # returns 'down'; wtf?

          vm = api.vms.get(name='test_vm')
          vm.status.get_state() # returns 'up'

    Args:
        hostname: The hostname of the system.
        username: The username to connect with.
        password: The password to connect with.

    Keywords:
        port: (Optional) Port where RHEVM API listens.
        api_endpoint: (Optional) If you need to fine-tune and pass an exact endpoint in form of a
            full URL, use this keyword. the ``port`` keyword is then not used.

    Returns: A :py:class:`RHEVMSystem` object.
    """

    _stats_available = {
        'num_vm': lambda self: len(self.list_vms()),
        'num_host': lambda self: len(self.list_host()),
        'num_cluster': lambda self: len(self.list_cluster()),
        'num_template': lambda self: len(self.list_templates()),
        'num_datastore': lambda self: len(self.list_datastore()),
    }

    can_suspend = True
    can_pause = False
    # Over-ride default steady_wait_time
    steady_wait_time = 6 * 60

    def __init__(self, hostname, username, password, **kwargs):
        # generate URL from hostname
        super(RHEVMSystem, self).__init__(kwargs)
        less_than_rhv_4 = float(kwargs['version']) < 4.0
        url_component = 'api' if less_than_rhv_4 else 'ovirt-engine/api'
        if 'api_endpoint' in kwargs:
            url = kwargs['api_endpoint']
        elif 'port' in kwargs:
            url = 'https://{}:{}/{}'.format(hostname, kwargs['port'], url_component)
        else:
            url = 'https://{}/{}'.format(hostname, url_component)

        self._api = None
        self._api_kwargs = {
            'url': url,
            'username': username,
            'password': password,
            'insecure': True,
        }
        self.kwargs = kwargs

    @property
    def _identifying_attrs(self):
        return {'url': self._api_kwargs['url']}

    @property
    def can_suspend(self):
        return True

    @property
    def can_pause(self):
        return False

    @property
    def api(self):
        # test() will return false if the connection timeouts, catch it and force it to re-init
        try:
            if self._api is None or not self._api.test():
                self._api = Connection(**self._api_kwargs)
        # if the connection was disconnected, force it to re-init
        except Error:
            self._api = Connection(**self._api_kwargs)
        return self._api

    @property
    def _vms_service(self):
        return self.api.system_service().vms_service()

    def find_vms(self, name=None, uuid=None):
        if not name and not uuid:
            raise ValueError("Must specify name or uuid for find_vms()")
        if name:
            query = 'name={}'.format(name)
        elif uuid:
            query = 'id={}'.format(uuid)
        query_result = self._vms_service.list(search=query)
        return [RHEVMVirtualMachine(system=self, uuid=vm.id) for vm in query_result]

    def list_vms(self):
        return [
            RHEVMVirtualMachine(system=self, uuid=vm.id)
            for vm in self._vms_service.list()
        ]

    def get_vm(self, name=None, uuid=None):
        """
        Get a single VM by name or ID

        Returns:
            wrapanapi.systems.rhevm.RHEVMVirtualMachine
        Raises:
            MultipleItemsError if multiple VM's found with this name/id
            VMInstanceNotFound if VM not found with this name/id
        """
        matches = self.find_vms(name=name, uuid=uuid)
        if not matches:
            raise VMInstanceNotFound('name={}, id={}'.format(name, uuid))
        if len(matches) > 1:
            raise MultipleItemsError(
                'Found multiple matches for VM with name={}, id={}'
                .format(name, uuid)
            )
        return matches[0]

    def create_vm(self, vm_name, **kwargs):
        raise NotImplementedError('create_vm not implemented')

    def get_vm_from_ip(self, ip):
        """
        Gets a vm from its IP.

        Args:
            ip: The ip address of the vm.

        Returns: wrapanapi.systems.rhevm.RHEVMVirtualMachine object
        """
        vms = self.list_vms()
        for vm in vms:
            if ip in vm.all_ips:
                return vm
        raise VMNotFoundViaIP("IP '{}' is not known as a VM".format(ip))

    def list_host(self, **kwargs):
        host_list = self.api.system_service().hosts_service().list(**kwargs)
        return [host.name for host in host_list]

    def list_datastore(self, sd_type=None, **kwargs):
        datastore_list = self.api.system_service().storage_domains_service().list(**kwargs)
        if sd_type:
            def cond(ds):
                return ds.status is None and ds.type.value == sd_type
        else:
            def cond(ds):
                return ds.status is None
        return [ds.name for ds in datastore_list if cond(ds)]

    def list_cluster(self, **kwargs):
        cluster_list = self.api.system_service().clusters_service().list(**kwargs)
        return [cluster.name for cluster in cluster_list]

    def info(self):
        # and we got nothing!
        pass

    def disconnect(self):
        self.api.close()

    def _get_cluster(self, cluster_name):
        cluster = 'name={}'.format(cluster_name)
        return self.api.system_service().clusters_service().list(search=cluster)[0]

    def remove_host_from_cluster(self, hostname):
        raise NotImplementedError('remove_host_from_cluster not implemented')

    def get_cluster(self, cluster_name):
        cluster = 'name={}'.format(cluster_name)
        return self.api.system_service().clusters_service().list(search=cluster)[0]

    @property
    def _templates_service(self):
        return self.api.system_service().templates_service()

    def find_templates(self, name=None, uuid=None):
        if not name and not uuid:
            raise ValueError("Must specify name or uuid for find_templates()")
        if name:
            query = 'name={}'.format(name)
        elif uuid:
            query = 'id={}'.format(uuid)
        query_result = self._templates_service.list(search=query)
        return [
            RHEVMTemplate(system=self, uuid=template.id)
            for template in query_result
        ]

    def list_templates(self):
        """
        Note: CFME ignores the 'Blank' template, so we do too
        """
        return [
            RHEVMTemplate(system=self, uuid=template.id)
            for template in self._templates_service.list() if template.name != "Blank"
        ]

    def get_template(self, name=None, uuid=None):
        """
        Get a single template by name or ID

        Returns:
            wrapanapi.systems.rhevm.RHEVMTemplate
        Raises:
            MultipleItemsError if multiple templates found with this name/id
            NotFoundError if template not found with this name/id
        """
        matches = self.find_templates(name=name, uuid=uuid)
        if not matches:
            raise NotFoundError('Template with name={}, id={}'.format(name, uuid))
        if len(matches) > 1:
            raise MultipleItemsError(
                'Found multiple matches for template with name={}, id={}'
                .format(name, uuid)
            )
        return matches[0]

    def create_template(self, template_name, vm_name, cluster_name=None):
        """
        Create a template based on a VM.

        Creates on the same cluster as the VM unless 'cluster_name' is specified
        """
        vm = self.get_vm(vm_name)

        if cluster_name:
            cluster = self.get_cluster(cluster_name)
        else:
            cluster = vm.cluster

        new_template = types.Template(name=template_name, vm=vm.raw, cluster=cluster)
        self.api.system_service().templates_service().add(new_template)

        # First it has to appear
        wait_for(
            lambda: self.does_template_exist(template_name),
            num_sec=30 * 60, message="template exists", delay=45)
        # Then the process has to finish
        template = self.get_template(template_name)
        template.wait_for_ok_status()
        return template

    def usage_and_quota(self):
        host_ram = 0
        host_cpu = 0
        used_ram = 0
        used_cpu = 0
        for host in self.api.system_service().hosts_service().list():
            host_ram += host.memory / 1024 / 1024
            topology = host.cpu.topology
            host_cpu += topology.cores * topology.sockets

        for vm in self._vms_service.list():
            assert isinstance(vm, types.Vm)
            if vm.status != types.VmStatus.UP:
                continue

            used_ram += vm.memory / 1024 / 1024
            assert isinstance(vm.cpu.topology, types.CpuTopology)
            topology = vm.cpu.topology
            used_cpu += topology.cores * topology.sockets

        return {
            # RAM
            'ram_used': used_ram,
            'ram_limit': host_ram,
            'ram_total': host_ram,
            # CPU
            'cpu_used': used_cpu,
            'cpu_total': host_cpu,
            'cpu_limit': None,
        }

    @property
    def _glance_servers_service(self):
        return self.api.system_service().openstack_image_providers_service()

    def _get_glance_server_service(self, name):
        for glance_server in self._glance_servers_service.list():
            if glance_server.name == name:
                return self._glance_servers_service.provider_service(glance_server.id)
        raise ItemNotFound(name, 'glance server')

    def _get_glance_server(self, name):
        return self._get_glance_server_service(name).get()

    def does_glance_server_exist(self, name):
        try:
            return bool(self._get_glance_server_service(name))
        except ItemNotFound:
            return False

    def add_glance_server(self, authentication_url=None, certificates=None, comment=None,
                          description=None, id=None, images=None, name=None, password=None,
                          properties=None, requires_authentication=None, tenant_name=None,
                          url=None, username=None):
        self._glance_servers_service.add(
            types.OpenStackImageProvider(
                name=name,
                description=description,
                url=url,
                requires_authentication=requires_authentication,
                authentication_url=authentication_url,
                username=username,
                password=password,
                tenant_name=tenant_name,
                certificates=certificates,
                comment=comment,
                id=id,
                images=images,
                properties=properties
            )
        )
        wait_for(self.does_glance_server_exist, func_args=[name], delay=5, num_sec=240)

    @property
    def _storage_domains_service(self):
        return self.api.system_service().storage_domains_service()

    def _get_storage_domain_service(self, name):
        query = 'name={}'.format(name)
        query_result = self._storage_domains_service.list(search=query)
        if not query_result:
            raise ItemNotFound(name, 'storage domain')
        else:
            storage_domain = query_result[0]
            return self._storage_domains_service.storage_domain_service(storage_domain.id)

    def _get_storage_domain(self, name):
        return self._get_storage_domain_service(name).get()

    def _get_images_service(self, storage_domain_name):
        return self._get_storage_domain_service(storage_domain_name).images_service()

    def _get_image_service(self, storage_domain_name, image_name):
        for image in self._get_images_service(storage_domain_name).list():
            if image.name == image_name:
                return self._get_images_service(storage_domain_name).image_service(image.id)

    def import_glance_image(self, source_storage_domain_name, source_template_name,
                            target_storage_domain_name, target_cluster_name, target_template_name,
                            async=True, import_as_template=True):
        image_service = self._get_image_service(source_storage_domain_name, source_template_name)
        image_service.import_(
            async=async,
            import_as_template=import_as_template,
            template=types.Template(name=target_template_name),
            cluster=types.Cluster(name=target_cluster_name),
            storage_domain=types.StorageDomain(name=target_storage_domain_name)
        )
        wait_for(self.does_template_exist, func_args=[target_template_name], delay=5, num_sec=240)

    def _get_disk_service(self, disk_name):
        disks_service = self.api.system_service().disks_service()
        query_result = disks_service.list(search="name={}".format(disk_name))
        if not query_result:
            raise ItemNotFound(disk_name, 'disk')
        else:
            disk = query_result[0]
            return disks_service.service(disk.id)

    def does_disk_exist(self, disk_name):
        try:
            return bool(self._get_disk_service(disk_name))
        except ItemNotFound:
            return False

    @property
    def _data_centers_service(self):
        return self.api.system_service().data_centers_service()

    def _get_attached_storage_domain_service(self, datacenter_id, storage_domain_id):
        return (self._data_centers_service.data_center_service(datacenter_id).
                storage_domains_service().storage_domain_service(storage_domain_id))

    def get_storage_domain_connections(self, storage_domain):
        return self._get_storage_domain_service(storage_domain).storage_connections_service().list()

    def change_storage_domain_state(self, state, storage_domain_name):
        dcs = self._data_centers_service.list()
        for dc in dcs:
            storage_domains = self.api.follow_link(dc.storage_domains)
            for domain in storage_domains:
                if domain.name == storage_domain_name:
                    asds = self._get_attached_storage_domain_service(dc.id, domain.id)
                    if state == "maintenance" and domain.status.value == "active":
                        asds.deactivate()
                    elif state == "active" and domain.status.value != "active":
                        asds.activate()
                    wait_for(lambda: domain.status.value == state, delay=5, num_sec=240)
                    return True
        return False

    def get_template_from_storage_domain(self, template_name, storage_domain_name):
        sds = self._get_storage_domain_service(storage_domain_name)
        for template in sds.templates_service().list(unregistered=False):
            if template.name == template_name:
                return RHEVMTemplate(system=self, uuid=template.id)
        raise NotFoundError(
            'template {} in storage domain {}'
            .format(template_name, storage_domain_name)
        )

    def import_template(self, edomain, sdomain, cluster, temp_template):
        export_sd_service = self._get_storage_domain_service(edomain)
        export_template = self.get_template_from_storage_domain(temp_template, edomain)
        target_storage_domain = self._get_storage_domain(sdomain)
        cluster_id = self._get_cluster(cluster).id
        sd_template_service = export_sd_service.templates_service().template_service(
            export_template.id)
        sd_template_service.import_(
            storage_domain=types.StorageDomain(id=target_storage_domain.id),
            cluster=types.Cluster(id=cluster_id),
            template=types.Template(id=export_template.id)
        )
