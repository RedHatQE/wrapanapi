# coding: utf-8
"""Backend management system classes

Used to communicate with providers without using CFME facilities
"""
from __future__ import absolute_import
import fauxfactory
import pytz
from ovirtsdk4 import Connection, Error, types
from wait_for import wait_for, TimedOutError

from .base import WrapanapiAPIBaseVM, VMInfo
from .exceptions import (
    ItemNotFound, VMInstanceNotFound, VMInstanceNotSuspended, VMNotFoundViaIP)


class RHEVMSystem(WrapanapiAPIBaseVM):
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
        'num_vm': lambda self: len(self.list_vm()),
        'num_host': lambda self: len(self.list_host()),
        'num_cluster': lambda self: len(self.list_cluster()),
        'num_template': lambda self: len(self.list_template()),
        'num_datastore': lambda self: len(self.list_datastore()),
    }

    STEADY_WAIT_MINS = 6

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

    def _get_vm_service(self, vm_name):
        """
        Args:
            vm_name: The name of the VM.

        Returns: ``ovirtsdk4.services.VmService`` object.
        """
        query = 'name={}'.format(vm_name)
        query_result = self._vms_service.list(search=query)
        if not query_result:
            raise VMInstanceNotFound(vm_name)
        else:
            vm = query_result[0]
            return self._vms_service.vm_service(vm.id)

    def _get_vm(self, vm_name=None):
        return self._get_vm_service(vm_name).get()

    def current_ip_address(self, vm_name):
        vm_service = self._get_vm_service(vm_name)
        rep_dev_service = vm_service.reported_devices_service()
        try:
            first = rep_dev_service.list()[0]
            return first.ips[0].address
        except IndexError:
            return None

    def get_ip_address(self, vm_name, timeout=600):
        try:
            return wait_for(
                lambda: self.current_ip_address(vm_name),
                fail_condition=None, delay=5, num_sec=timeout,
                message="get_ip_address from rhevm")[0]
        except TimedOutError:
            return None

    def get_vm_name_from_ip(self, ip):
        # unfortunately it appears you cannot query for ip address from the sdk,
        #   unlike curling rest api which does work
        """ Gets the name of a vm from its IP.

        Args:
            ip: The ip address of the vm.
        Returns: The vm name for the corresponding IP."""

        vms = self._vms_service.list()
        for vm in vms:
            vm_service = self._vms_service.vm_service(vm.id)

            rep_dev_service = vm_service.reported_devices_service()
            for dev in rep_dev_service.list():
                for listed_ip in dev.ips:
                    if listed_ip.address == ip:
                        return vm.name
        raise VMNotFoundViaIP('The requested IP is not known as a VM')

    def does_vm_exist(self, name):
        try:
            return bool(self._get_vm_service(name))
        except VMInstanceNotFound:
            return False

    def start_vm(self, vm_name):
        self.wait_vm_steady(vm_name)
        self.logger.info(' Starting RHEV VM %s' % vm_name)
        if self.is_vm_running(vm_name):
            self.logger.info(' RHEV VM %s os already running.' % vm_name)
            return True
        else:
            vm_service = self._get_vm_service(vm_name)
            vm_service.start()
            self.wait_vm_running(vm_name)
            return True

    def stop_vm(self, vm_name):
        self.wait_vm_steady(vm_name)
        self.logger.info(' Stopping RHEV VM %s' % vm_name)
        if self.is_vm_stopped(vm_name):
            self.logger.info(' RHEV VM %s os already stopped.' % vm_name)
            return True
        else:
            vm_service = self._get_vm_service(vm_name)
            vm_service.stop()
            self.wait_vm_stopped(vm_name)
            return True

    def delete_vm(self, vm_name, **kwargs):
        self.wait_vm_steady(vm_name)
        if not self.is_vm_stopped(vm_name):
            self.stop_vm(vm_name)
        self.logger.debug(' Deleting RHEV VM %s' % vm_name)

        def _do_delete():
            """Returns True if you have to retry"""
            if not self.does_vm_exist(vm_name):
                return False
            try:
                vm_service = self._get_vm_service(vm_name)
                vm_service.remove()
            except Error:
                # Handle some states that can occur and can be circumvented
                raise  # Raise other so we can see them and eventually add them into handling
            else:
                return False

        wait_for(_do_delete, fail_condition=True, num_sec=600, delay=15, message="execute delete")

        wait_for(
            lambda: self.does_vm_exist(vm_name),
            fail_condition=True,
            message="wait for RHEV VM %s deleted" % vm_name,
            num_sec=300
        )
        return True

    def create_vm(self, vm_name, **kwargs):
        raise NotImplementedError('This function has not yet been implemented.')

    def restart_vm(self, vm_name):
        self.logger.debug(' Restarting RHEV VM %s' % vm_name)
        return self.stop_vm(vm_name) and self.start_vm(vm_name)

    def list_vm(self):
        vm_list = self._vms_service.list()
        return [vm.name for vm in vm_list]

    def all_vms(self):
        result = []
        for vm in self._vms_service.list():
            ip = self.get_ip_address(vm.name, timeout=5)
            result.append(VMInfo(vm.id, vm.name, vm.status.value, ip))
        return result

    def get_vm_guid(self, vm_name):
        return self._get_vm(vm_name).id

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

    def list_template(self, **kwargs):
        """
        Note: CFME ignores the 'Blank' template, so we do too
        """
        template_list = self.api.system_service().templates_service().list(**kwargs)
        return [template.name for template in template_list if template.name != "Blank"]

    def list_flavor(self):
        raise NotImplementedError('This function is not supported on this platform.')

    def info(self):
        # and we got nothing!
        pass

    def disconnect(self):
        self.api.close()

    def vm_status(self, vm_name=None):
        return self._get_vm(vm_name).status.value

    def vm_creation_time(self, vm_name):
        return self._get_vm(vm_name).creation_time.astimezone(pytz.UTC)

    def in_steady_state(self, vm_name):
        return self.vm_status(vm_name) in {"up", "down", "suspended"}

    def is_vm_running(self, vm_name):
        return self.vm_status(vm_name) == "up"

    def wait_vm_running(self, vm_name, num_sec=360):
        self.logger.info(" Waiting for RHEV-M VM %s to change status to ON" % vm_name)
        wait_for(self.is_vm_running, [vm_name], num_sec=num_sec)

    def is_vm_stopped(self, vm_name):
        return self.vm_status(vm_name) == "down"

    def wait_vm_stopped(self, vm_name, num_sec=360):
        self.logger.info(" Waiting for RHEV-M VM %s to change status to OFF" % vm_name)
        wait_for(self.is_vm_stopped, [vm_name], num_sec=num_sec)

    def is_vm_suspended(self, vm_name):
        return self.vm_status(vm_name) == "suspended"

    def wait_vm_suspended(self, vm_name, num_sec=720):
        self.logger.info(" Waiting for RHEV-M VM %s to change status to SUSPENDED" % vm_name)
        wait_for(self.is_vm_suspended, [vm_name], num_sec=num_sec)

    def suspend_vm(self, vm_name):
        self.wait_vm_steady(vm_name)
        self.logger.debug(' Suspending RHEV VM %s' % vm_name)
        if self.is_vm_stopped(vm_name):
            raise VMInstanceNotSuspended(vm_name)
        elif self.is_vm_suspended(vm_name):
            self.logger.info(' RHEV VM %s is already suspended.' % vm_name)
            return True
        else:
            vm = self._get_vm_service(vm_name)
            vm.suspend()
            self.wait_vm_suspended(vm_name)
            return True

    def clone_vm(self, source_name, vm_name):
        raise NotImplementedError('This function has not yet been implemented.')

    def _get_vm_nic_service(self, vm_name, nic_name):
        vm_service = self._get_vm_service(vm_name)
        for nic in vm_service.nics_service().list():
            if nic.name == nic_name:
                return vm_service.nics_service().nic_service(nic.id)

    def _get_vm_nic(self, vm_name, nic_name):
        return self._get_vm_nic_service(vm_name, nic_name).get()

    def update_vm_nic(self, vm_name, network_name, nic_name='nic1',
                      interface=types.NicInterface.VIRTIO):
        nic = self._get_vm_nic(vm_name, nic_name)
        nic_service = self._get_vm_nic_service(vm_name, nic_name)
        nic.network = types.Network(name=network_name)
        nic.interface = interface
        nic_service.update(nic)

    def _get_cluster(self, cluster_name):
        cluster = 'name={}'.format(cluster_name)
        return self.api.system_service().clusters_service().list(search=cluster)[0]

    def deploy_template(self, template, *args, **kwargs):
        self.logger.debug(' Deploying RHEV template %s to VM %s' % (template, kwargs["vm_name"]))
        timeout = kwargs.pop('timeout', 900)
        power_on = kwargs.pop('power_on', True)
        vm_kwargs = {
            'name': kwargs['vm_name'],
            'cluster': self._get_cluster(kwargs['cluster']),
            'template': self._get_template(template)
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
            vm_kwargs['memory'] = int(kwargs['ram']) * 1024 * 1024  # MB
        self._vms_service.add(types.Vm(**vm_kwargs))
        self.wait_vm_stopped(kwargs['vm_name'], num_sec=timeout)
        if power_on:
            self.start_vm(kwargs['vm_name'])
        return kwargs['vm_name']

    def remove_host_from_cluster(self, hostname):
        raise NotImplementedError('remove_host_from_cluster not implemented')

    def mark_as_template(self, vm_name, delete=True, temporary_name=None,
            cluster=None, delete_on_error=True):
        """Turns the VM off, creates template from it and deletes the original VM.

        Mimics VMware behaviour here.

        Args:
            vm_name: Name of the VM to be turned to template
            delete: Whether to delete the VM (default: True)
            temporary_name: If you want, you can specific an exact temporary name for renaming.
            delete_on_error: delete on timeout as well.
        """
        temp_template_name = temporary_name or "templatize_{}".format(
            fauxfactory.gen_alphanumeric(8))
        try:
            with self.steady_wait(30):
                create_new_template = True
                if self.does_template_exist(temp_template_name):
                    try:
                        self._wait_template_ok(temp_template_name)
                    except VMInstanceNotFound:
                        pass  # It got deleted.
                    else:
                        create_new_template = False
                        if self.does_vm_exist(vm_name) and delete:
                            self.delete_vm(vm_name, )
                        if delete:  # We can only rename to the original name if we delete the vm
                            self._rename_template(temp_template_name, vm_name)

                if create_new_template:
                    self.stop_vm(vm_name)
                    vm = self._get_vm(vm_name)
                    actual_cluster = self._get_cluster(cluster) if cluster else vm.cluster
                    new_template = types.Template(
                        name=temp_template_name, vm=vm, cluster=actual_cluster)
                    self.api.system_service().templates_service().add(new_template)
                    # First it has to appear
                    self._wait_template_exists(temp_template_name)
                    # Then the process has to finish
                    self._wait_template_ok(temp_template_name)
                    # Delete the original VM
                    if self.does_vm_exist(vm_name) and delete:
                        self.delete_vm(vm_name, )
                    if delete:  # We can only rename to the original name if we delete the vm
                        self._rename_template(temp_template_name, vm_name)
        except TimedOutError:
            if delete_on_error:
                self.delete_template(temp_template_name)
            raise

    def _rename_template(self, old_name, new_name):
        template_service = self._get_template_service(old_name)
        template_service.update(types.Template(name=new_name))

    def rename_vm(self, vm_name, new_vm_name):
        vm_service = self._get_vm_service(vm_name)
        try:
            vm_service.update(types.Vm(name=new_vm_name))
        except Exception as e:
            self.logger.exception(e)
            return vm_name
        else:
            return new_vm_name

    def _get_template_service(self, template_name):
        query = 'name={}'.format(template_name or '')
        templates_service = self.api.system_service().templates_service()
        query_result = templates_service.list(search=query)
        if not query_result:
            raise ItemNotFound(template_name, 'template')
        else:
            template = query_result[0]
            return templates_service.template_service(template.id)

    def _get_template(self, template_name):
        return self._get_template_service(template_name).get()

    def _wait_template_ok(self, template_name):
        wait_for(
            lambda: self._get_template(template_name).status == types.TemplateStatus.OK,
            num_sec=30 * 60, message="template is OK", delay=45)

    def _wait_template_exists(self, template_name):
        wait_for(
            lambda: self.does_template_exist(template_name),
            num_sec=30 * 60, message="template exists", delay=45)

    def does_template_exist(self, template_name):
        try:
            return bool(self._get_template_service(template_name))
        except ItemNotFound:
            return False

    def delete_template(self, template_name):
        template_service = self._get_template_service(template_name)
        self._wait_template_ok(template_name)
        template_service.remove()
        wait_for(
            lambda: not self.does_template_exist(template_name),
            num_sec=15 * 60, delay=20)

    def vm_hardware_configuration(self, vm_name):
        vm = self._get_vm(vm_name)
        return {
            'ram': vm.memory / 1024 / 1024,
            'cpu': vm.cpu.topology.cores * vm.cpu.topology.sockets
        }

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
            description=None, id=None, images=None, name=None, password=None, properties=None,
            requires_authentication=None, tenant_name=None, url=None, username=None):
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
        return self._get_storage_domain(storage_domain_name).images_service()

    def _get_image_service(self, storage_domain_name, image_name):
        for image in self._get_images_service(storage_domain_name).list():
            if image.name == image_name:
                return self._get_images_service(storage_domain_name).image_service(image.id)

    def import_glance_image(self, storage_domain_name, cluster_name, temp_template_name,
                            template_name, async=True, import_as_template=True):
        image_service = self._get_image_service(storage_domain_name, template_name)
        image_service.import_(
            async=async,
            import_as_template=import_as_template,
            template=types.Template(name=temp_template_name),
            cluster=types.Cluster(name=cluster_name),
            storage_domain=types.StorageDomain(name=storage_domain_name)
        )
        wait_for(self.does_template_exist, func_args=[temp_template_name], delay=5, num_sec=240)

    def _get_disk_attachments_service(self, vm_name):
        vm_id = self.get_vm_guid(vm_name)
        return self._vms_service.vm_service(vm_id).disk_attachments_service()

    def _get_disk_attachment_service(self, vm_name, disk_name):
        disk_attachments_service = self._get_disk_attachments_service(vm_name)
        for disk_attachment_service in disk_attachments_service.list():
            disk = self.api.follow_link(disk_attachment_service.disk)
            if disk.name == disk_name:
                return disk_attachments_service.service(disk.id)
        raise ItemNotFound(disk_name, 'disk')

    def is_disk_attached_to_vm(self, vm_name, disk_name):
        try:
            return bool(self._get_disk_attachment_service(vm_name, disk_name))
        except ItemNotFound:
            return False

    def get_vm_disks_count(self, vm_name):
        return len(self._get_disk_attachments_service(vm_name).list())

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

    def _check_disk(self, disk_id):
        disks_service = self.api.system_service().disks_service()
        disk_service = disks_service.disk_service(disk_id)
        disk = disk_service.get()
        return disk.status == types.DiskStatus.OK

    def add_disk_to_vm(self, vm_name, storage_domain=None, size=None, interface=None, format=None,
            active=True):
        """

        Args:
            vm_name: string name
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
        disk_attachments_service = self._get_disk_attachments_service(vm_name)
        disk_attachment = disk_attachments_service.add(
            types.DiskAttachment(
                disk=types.Disk(
                    format=types.DiskFormat(format),
                    provisioned_size=size,
                    storage_domains=[
                        types.StorageDomain(
                            name=storage_domain,
                        )
                    ]
                ),
                interface=types.DiskInterface(interface),
                active=active
            )
        )
        wait_for(self._check_disk, func_args=[disk_attachment.disk.id], delay=5, num_sec=900,
                 message="check if disk is attached")

    def connect_direct_lun_to_appliance(self, vm_name, disconnect, lun_name=None, lun_ip_addr=None,
                                        lun_port=None, lun_iscsi_target=None):
        """Connects or disconnects the direct lun disk to an appliance.

        Args:
            vm_name: Name of the VM with the appliance.
            disconnect: If False, it will connect, otherwise it will disconnect
            lun_name: name of LUN
            lun_ip_addr: LUN ip address
            lun_port: LUN port
            lun_iscsi_target: iscsi target
        """
        if not disconnect:
            disk_attachments_service = self._get_vm_service(vm_name).disk_attachments_service()
            if not self.does_disk_exist(lun_name):
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
                                    target=lun_iscsi_target
                                )
                            ]
                        )
                    ),
                    interface=types.DiskInterface.VIRTIO,
                    active=True
                )
            else:
                disk_attachment = self._get_disk_attachment_service(vm_name, lun_name).get()
            disk_attachments_service.add(disk_attachment)
            wait_for(self._check_disk, func_args=[disk_attachment.disk.id], delay=5, num_sec=900,
                     message="check if disk is attached")
        # remove it
        else:
            if not self.is_disk_attached_to_vm(vm_name, lun_name):
                return
            else:
                disk_attachment_service = self._get_disk_attachment_service(vm_name, lun_name)
                disk_attachment_service.remove(detach_only=True)

    @property
    def _data_centers_service(self):
        return self.api.system_service().data_centers_service()

    def _get_attached_storage_domain_service(self, datacenter_id, storage_domain_id):
        return (self._data_centers_service.data_center_service(datacenter_id).
                storage_domains_service().storage_domain_service(storage_domain_id))

    def change_storage_domain_state(self, state, storage_domain_name):
        dcs = self._data_centers_service.list()
        for dc in dcs:
            storage_domains = self.api.follow_link(dc.storagedomains)
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
                return template
        return False

    def import_template(self, edomain, sdomain, cluster, temp_template):
        export_sd_service = self._get_storage_domain_service(edomain)
        export_template = self.get_template_from_storage_domain(temp_template, edomain)
        target_storage_domain = self._get_storage_domain(sdomain)
        cluster_id = self._get_cluster(cluster).id
        template_service = export_sd_service.templates_service().template_service(
            export_template.id)
        template_service.import_(
            storage_domain=types.StorageDomain(id=target_storage_domain.id),
            cluster=types.Cluster(id=cluster_id),
            template=types.Template(id=export_template.id)
        )

    def get_storage_domain_connections(self, storage_domain):
        storage_domain = self._get_storage_domain_service(storage_domain)
        return self.api.follow_link(storage_domain.storage_connections)
