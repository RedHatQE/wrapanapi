# coding: utf-8
"""Backend management system classes

Used to communicate with providers without using CFME facilities
"""
from __future__ import absolute_import

import atexit
import operator
import re
import ssl
import threading
import time
from datetime import datetime
from distutils.version import LooseVersion
from functools import partial

import pytz
import six
from cached_property import threaded_cached_property
from pyVim.connect import Disconnect, SmartConnect
from pyVmomi import vim, vmodl
from wait_for import TimedOutError, wait_for

from wrapanapi.entities import (Template, TemplateMixin, Vm, VmMixin,
                                VmState)
from wrapanapi.entities.base import Entity
from wrapanapi.exceptions import (HostNotRemoved, NotFoundError,
                                  VMCreationDateError, VMInstanceNotCloned,
                                  VMInstanceNotFound, VMInstanceNotStopped,
                                  VMInstanceNotSuspended, VMNotFoundViaIP)
from wrapanapi.systems.base import System


SELECTION_SPECS = [
    'resource_pool_traversal_spec',
    'resource_pool_vm_traversal_spec',
    'folder_traversal_spec',
    'datacenter_host_traversal_spec',
    'datacenter_vm_traversal_spec',
    'compute_resource_rp_traversal_spec',
    'compute_resource_host_traversal_spec',
    'host_vm_traversal_spec',
    'datacenter_datastore_traversal_spec'
]
TRAVERSAL_SPECS = [
    {
        'name': 'resource_pool_traversal_spec',
        'type': vim.ResourcePool,
        'path': 'resourcePool',
        'select_indices': [0, 1]
    },
    {
        'name': 'resource_pool_vm_traversal_spec',
        'type': vim.ResourcePool,
        'path': 'vm',
        'select_indices': []
    },
    {
        'name': 'compute_resource_rp_traversal_spec',
        'type': vim.ComputeResource,
        'path': 'resourcePool',
        'select_indices': [0, 1]
    },
    {
        'name': 'compute_resource_host_traversal_spec',
        'type': vim.ComputeResource,
        'path': 'host',
        'select_indices': []
    },
    {
        'name': 'datacenter_host_traversal_spec',
        'type': vim.Datacenter,
        'path': 'hostFolder',
        'select_indices': [2]
    },
    {
        'name': 'datacenter_datastore_traversal_spec',
        'type': vim.Datacenter,
        'path': 'datastoreFolder',
        'select_indices': [2]
    },
    {
        'name': 'datacenter_vm_traversal_spec',
        'type': vim.Datacenter,
        'path': 'vmFolder',
        'select_indices': [2]
    },
    {
        'name': 'host_vm_traversal_spec',
        'type': vim.HostSystem,
        'path': 'vm',
        'select_indices': [2]
    },
    {
        'name': 'folder_traversal_spec',
        'type': vim.Folder,
        'path': 'childEntity',
        'select_indices': [2, 3, 4, 5, 6, 7, 1, 8]
    }
]


def get_task_error_message(task):
    """Depending on the error type, a different attribute may contain the error message. This
    function will figure out the error message.
    """
    message = "faultCause='{}', faultMessage='{}', localizedMessage='{}'".format(
        task.info.error.faultCause if hasattr(task.info.error, 'faultCause') else "",
        task.info.error.faultMessage if hasattr(task.info.error, 'faultMessage') else "",
        task.info.error.localizedMessage if hasattr(task.info.error, 'localizedMessage') else ""
    )
    return message


class VMWareVMOrTemplate(Entity):
    """
    Holds shared methods/properties that VM's and templates have in common.

    A VM and a template are the same object type in pyVmomi, due to this they
    share many common operations

    A template will have 'config.template'==True
    """
    def __init__(self, system, raw=None, **kwargs):
        """
        Construct a VMWareVirtualMachine instance

        Args:
            system: instance of VMWareSystem
            raw: pyVmomi.vim.VirtualMachine object
            name: name of VM
        """
        super(VMWareVMOrTemplate, self).__init__(system, raw, **kwargs)
        self._name = raw.name if raw else kwargs.get('name')
        if not self._name:
            raise ValueError("missing required kwarg 'name'")

    @property
    def _identifying_attrs(self):
        return {'name': self._name}

    @property
    def name(self):
        return self._name

    def refresh(self):
        """
        Implemented in the VMWareVirtualMachine and VMWareTemplate classes.
        """
        raise NotImplementedError

    @property
    def uuid(self):
        try:
            return str(self.raw.summary.config.uuid)
        except AttributeError:
            return self.name

    @property
    def host(self):
        self.refresh()
        return self.raw.runtime.host.name

    def delete(self):
        self.logger.info(" Deleting vSphere VM/template %s", self.name)

        task = self.raw.Destroy_Task()

        try:
            wait_for(lambda: self.system.get_task_status(task) == 'success', delay=3, timeout="4m")
        except TimedOutError:
            self.logger.warn("Hit TimedOutError waiting for VM '%s' delete task", self.name)
            if self.exists:
                return False
        return True

    def cleanup(self):
        return self.delete()

    def rename(self, new_name):
        task = self.raw.Rename_Task(newName=new_name)
        # Cycle until the new named VM/template is found
        # That must happen or the error state can come up too
        old_name = self._name
        self._name = new_name
        while not self.exists:
            if self.system.get_task_status(task) == "error":
                self._name = old_name
                return False
            time.sleep(0.5)
        # The newly renamed VM/template is found
        return True

    def get_hardware_configuration(self):
        self.refresh()
        return {
            'ram': self.raw.config.hardware.memoryMB,
            'cpu': self.raw.config.hardware.numCPU,
        }

    def get_datastore_path(self, vm_config_datastore):
        datastore_url = [str(datastore.url)
                         for datastore in self.raw.config.datastoreUrl
                         if datastore.name in vm_config_datastore]
        return datastore_url.pop()

    def get_config_files_path(self):
        self.refresh()
        vmfilespath = self.raw.config.files.vmPathName
        return str(vmfilespath)

    @staticmethod
    def _progress_log_callback(logger, source, destination, progress):
        logger.info("Provisioning progress {}->{}: {}".format(
            source, destination, str(progress)))

    def _pick_datastore(self, allowed_datastores):
        """Pick a datastore based on free space."""
        possible_datastores = [
            ds for ds in self.system.get_obj_list(vim.Datastore)
            if ds.name in allowed_datastores and ds.summary.accessible and
            ds.summary.multipleHostAccess and ds.overallStatus != "red"]
        possible_datastores.sort(
            key=lambda ds: float(ds.summary.freeSpace) / float(ds.summary.capacity),
            reverse=True)
        if not possible_datastores:
            raise Exception("No possible datastores!")
        return possible_datastores[0]

    def _get_resource_pool(self, resource_pool_name=None):
        """ Returns a resource pool managed object for a specified name.

        Args:
            resource_pool_name (string): The name of the resource pool. If None, first one will be
        picked.
        Returns:
             pyVmomi.vim.ResourcePool: The managed object of the resource pool.
        """
        if resource_pool_name is not None:
            return self.system.get_obj(vim.ResourcePool, resource_pool_name)
        elif self.system.default_resource_pool is not None:
            return self.system.get_obj(vim.ResourcePool, self.system.default_resource_pool)
        return self.system.get_obj_list(vim.ResourcePool)[0]

    def _clone(self, destination, resourcepool=None, datastore=None, power_on=True,
               sparse=False, template=False, provision_timeout=1800, progress_callback=None,
               allowed_datastores=None, cpu=None, ram=None, relocate=False, host=None, **kwargs):
        """Clone this template to a VM
        When relocate is True, relocated (migrated) with VMRelocateSpec instead of being cloned
        Returns a VMWareVirtualMachine object
        """
        try:
            vm = self.system.get_vm(destination)
        except VMInstanceNotFound:
            vm = None
        if vm and not relocate:
            raise Exception("VM/template of the name {} already present!".format(destination))

        if progress_callback is None:
            progress_callback = partial(
                self._progress_log_callback, self.logger, self.name, destination)

        source_template = self.raw

        vm_clone_spec = vim.VirtualMachineCloneSpec()
        vm_reloc_spec = vim.VirtualMachineRelocateSpec()
        # DATASTORE
        if isinstance(datastore, six.string_types):
            vm_reloc_spec.datastore = self.system.get_obj(vim.Datastore, name=datastore)
        elif isinstance(datastore, vim.Datastore):
            vm_reloc_spec.datastore = datastore
        elif datastore is None:
            if allowed_datastores is not None:
                # Pick a datastore by space
                vm_reloc_spec.datastore = self._pick_datastore(allowed_datastores)
            else:
                # Use the same datastore
                datastores = source_template.datastore
                if isinstance(datastores, (list, tuple)):
                    vm_reloc_spec.datastore = datastores[0]
                else:
                    vm_reloc_spec.datastore = datastores
        else:
            raise NotImplementedError("{} not supported for datastore".format(datastore))
        progress_callback("Picked datastore `{}`".format(vm_reloc_spec.datastore.name))

        # RESOURCE POOL
        if isinstance(resourcepool, vim.ResourcePool):
            vm_reloc_spec.pool = resourcepool
        else:
            vm_reloc_spec.pool = self._get_resource_pool(resourcepool)
        progress_callback("Picked resource pool `{}`".format(vm_reloc_spec.pool.name))

        vm_reloc_spec.host = (host if isinstance(host, vim.HostSystem)
                              else self.system.get_obj(vim.HostSystem, host))  # could be none
        if sparse:
            vm_reloc_spec.transform = vim.VirtualMachineRelocateTransformation().sparse
        else:
            vm_reloc_spec.transform = vim.VirtualMachineRelocateTransformation().flat

        vm_clone_spec.powerOn = power_on
        vm_clone_spec.template = template
        vm_clone_spec.location = vm_reloc_spec
        vm_clone_spec.snapshot = None

        if cpu is not None:
            vm_clone_spec.config.numCPUs = int(cpu)
        if ram is not None:
            vm_clone_spec.config.memoryMB = int(ram)
        try:
            folder = source_template.parent.parent.vmParent
        except AttributeError:
            folder = source_template.parent
        progress_callback("Picked folder `{}`".format(folder.name))

        action = source_template.RelocateVM_Task if relocate else source_template.CloneVM_Task
        action_args = dict(spec=vm_reloc_spec) if relocate else dict(folder=folder,
                                                                     name=destination,
                                                                     spec=vm_clone_spec)

        task = action(**action_args)

        def _check(store=[task]):
            try:
                if hasattr(store[0].info, 'progress') and store[0].info.progress is not None:
                    progress_callback("{}/{}%".format(store[0].info.state, store[0].info.progress))
                else:
                    progress_callback("{}".format(store[0].info.state))
            except AttributeError:
                pass
            if store[0].info.state not in {"queued", "running"}:
                return True
            store[0] = self.system.get_updated_obj(store[0])
            return False

        wait_for(_check, num_sec=provision_timeout, delay=4)

        if task.info.state != 'success':
            self.logger.error(
                "Clone VM from VM/template '%s' failed: %s",
                self.name, get_task_error_message(task)
            )
            raise VMInstanceNotCloned(destination)
        if template:
            entity_cls = VMWareTemplate
        else:
            entity_cls = VMWareVirtualMachine
        if relocate:
            self.rename(destination)
        return entity_cls(system=self.system, name=destination)

    def add_disk(self, capacity_in_kb, provision_type=None, unit=None):
        """
        Create a disk on the given datastore (by name)

        Community Example used
        https://github.com/vmware/pyvmomi-community-samples/blob/master/samples/add_disk_to_vm.py

        Return task type from Task.result or Task.error
        https://github.com/vmware/pyvmomi/blob/master/docs/vim/TaskInfo.rst

        Args:
            capacity_in_kb (int): capacity of the new drive in Kilobytes
            provision_type (string): 'thin' or 'thick', will default to thin if invalid option
            unit (int): The unit number of the disk to add, use to override existing disk. Will
                search for next available unit number by default

        Returns:
            (bool, task_result): Tuple containing boolean True if task ended in success,
                                 and the contents of task.result or task.error depending on state
        """
        provision_type = provision_type if provision_type in ['thick', 'thin'] else 'thin'
        self.refresh()

        # if passed unit matches existing device unit, match these values too
        key = None
        controller_key = None
        unit_number = None
        virtual_disk_devices = [
            device for device
            in self.raw.config.hardware.device if isinstance(device, vim.vm.device.VirtualDisk)]
        for dev in virtual_disk_devices:
            if unit == int(dev.unitNumber):
                # user specified unit matching existing disk, match key too
                key = dev.key
            unit_number = unit or int(dev.unitNumber) + 1
            if unit_number == 7:  # reserved
                unit_number += 1
            controller_key = dev.controllerKey

        if not (controller_key or unit_number):
            raise ValueError('Could not identify VirtualDisk device on given vm')

        # create disk backing specification
        backing_spec = vim.vm.device.VirtualDisk.FlatVer2BackingInfo()
        backing_spec.diskMode = 'persistent'
        backing_spec.thinProvisioned = (provision_type == 'thin')

        # create disk specification, attaching backing
        disk_spec = vim.vm.device.VirtualDisk()
        disk_spec.backing = backing_spec
        disk_spec.unitNumber = unit_number
        if key:  # only set when overriding existing disk
            disk_spec.key = key
        disk_spec.controllerKey = controller_key
        disk_spec.capacityInKB = capacity_in_kb

        # create device specification, attaching disk
        device_spec = vim.vm.device.VirtualDeviceSpec()
        device_spec.fileOperation = 'create'
        device_spec.operation = vim.vm.device.VirtualDeviceSpec.Operation.add
        device_spec.device = disk_spec

        # create vm specification for device changes
        vm_spec = vim.vm.ConfigSpec()
        vm_spec.deviceChange = [device_spec]

        # start vm reconfigure task
        task = self.raw.ReconfigVM_Task(spec=vm_spec)

        try:
            wait_for(lambda: task.info.state not in ['running', 'queued'])
        except TimedOutError:
            self.logger.exception('Task did not go to success state: %s', task)
        finally:
            if task.info.state == 'success':
                result = (True, task.info.result)
            elif task.info.state == 'error':
                result = (False, task.info.error)
            else:  # shouldn't happen
                result = (None, None)
        return result


class VMWareVirtualMachine(VMWareVMOrTemplate, Vm):
    state_map = {
        'poweredOn': VmState.RUNNING,
        'poweredOff': VmState.STOPPED,
        'suspended': VmState.SUSPENDED,
    }

    def refresh(self):
        self.raw = self.system.get_vm(self._name, force=True).raw
        return self.raw

    def _get_state(self):
        self.refresh()
        return self._api_state_to_vmstate(str(self.raw.runtime.powerState))

    @property
    def ip(self):
        ipv4_re = r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}'
        self.refresh()
        try:
            ip_address = self.raw.summary.guest.ipAddress
            if not re.match(ipv4_re, ip_address) or ip_address == '127.0.0.1':
                ip_address = None
            return ip_address
        except (AttributeError, TypeError):
            # AttributeError: vm doesn't have an ip address yet
            # TypeError: ip address wasn't a string
            return None

    @property
    def creation_time(self):
        """Detect the vm_creation_time either via uptime if non-zero, or by last boot time

        The API provides no sensible way to actually get this value. The only way in which
        vcenter API MAY have this is by filtering through events

        Return tz-naive datetime object
        """
        vm = self.raw

        filter_spec = vim.event.EventFilterSpec(
            entity=vim.event.EventFilterSpec.ByEntity(
                entity=vm, recursion=vim.event.EventFilterSpec.RecursionOption.self),
            eventTypeId=['VmDeployedEvent', 'VmCreatedEvent'])
        collector = self.system.content.eventManager.CreateCollectorForEvents(filter=filter_spec)
        collector.SetCollectorPageSize(1000)  # max allowed value
        events = collector.latestPage
        collector.DestroyCollector()  # limited number of collectors allowed per client

        if events:
            creation_time = events.pop().createdTime  # datetime object
        else:
            # no events found for VM, fallback to last boot time
            creation_time = vm.runtime.bootTime
            if not creation_time:
                raise VMCreationDateError('Could not find a creation date for {}'.format(self.name))
        # localize and make tz-naive
        return creation_time.astimezone(pytz.UTC)

    @property
    def cpu_hot_plug(self):
        return self.raw.config.cpuHotAddEnabled

    @property
    def memory_hot_plug(self):
        return self.raw.config.memoryHotAddEnabled

    @cpu_hot_plug.setter
    def cpu_hot_plug(self, value):
        """
        Set cpuHotPlug (enabled/disabled) for VM/Instance.

        Args:
            value (bool): cpu hot plug state
        """
        if self.cpu_hot_plug != value:
            if self.is_stopped:
                spec = vim.vm.ConfigSpec()
                spec.cpuHotAddEnabled = value
                task = self.raw.ReconfigVM_Task(spec)

                try:
                    wait_for(lambda: task.info.state not in ["running", "queued"])
                except TimedOutError:
                    self.logger.exception("Task did not go to success state: %s", task)
            else:
                raise VMInstanceNotStopped(self.name, "cpuHotPlug")

    @memory_hot_plug.setter
    def memory_hot_plug(self, value):
        """
        Set memoryHotPlug (enabled/disabled) for VM/Instance

        Args:
            value (bool): memory hot plug state
        """
        if self.memory_hot_plug != value:
            if self.is_stopped:
                spec = vim.vm.ConfigSpec()
                spec.memoryHotAddEnabled = value
                task = self.raw.ReconfigVM_Task(spec)

                try:
                    wait_for(lambda: task.info.state not in ["running", "queued"])
                except TimedOutError:
                    self.logger.exception("Task did not go to success state: %s", task)
            else:
                raise VMInstanceNotStopped(self.name, "memoryHotPlug")

    def start(self):
        if self.is_running:
            self.logger.info(" vSphere VM %s is already running", self.name)
            return True
        self.logger.info(" Starting vSphere VM %s", self.name)
        self.raw.PowerOnVM_Task()
        self.wait_for_state(VmState.RUNNING)
        return True

    def stop(self):
        if self.is_stopped:
            self.logger.info(" vSphere VM %s is already stopped", self.name)
            return True
        self.logger.info(" Stopping vSphere VM %s", self.name)
        # resume VM if it is suspended
        self.ensure_state(VmState.RUNNING)
        self.raw.PowerOffVM_Task()
        self.wait_for_state(VmState.STOPPED)
        return True

    def restart(self):
        self.logger.info(" Restarting vSphere VM %s", self.name)
        return self.stop() and self.start()

    def suspend(self):
        self.logger.info(" Suspending vSphere VM %s", self.name)

        if self.is_stopped:
            raise VMInstanceNotSuspended(self.name)
        else:
            self.raw.SuspendVM_Task()
            self.wait_for_state(VmState.SUSPENDED)
            return True

    def delete(self):
        self.ensure_state(VmState.STOPPED)
        return super(VMWareVirtualMachine, self).delete()

    def mark_as_template(self, template_name=None, **kwargs):
        self.ensure_state(VmState.STOPPED)
        self.raw.MarkAsTemplate()
        template = VMWareTemplate(system=self.system, name=self.name, raw=self.raw)
        template.refresh()
        if template_name and template_name != template.name:
            template.rename(template_name)
        return template

    def clone(self, vm_name, **kwargs):
        kwargs['destination'] = vm_name
        self.ensure_state(VmState.STOPPED)
        return self._clone(**kwargs)


class VMWareTemplate(VMWareVMOrTemplate, Template):
    def refresh(self):
        self.raw = self.system.get_template(self._name, force=True).raw
        return self.raw

    def deploy(self, vm_name, timeout=1800, **kwargs):
        """
        Clone a VM based on this template, wait for it to reach desired power state.

        Returns a VMWareVirtualMachine object
        """
        kwargs["power_on"] = kwargs.pop("power_on", True)
        kwargs["template"] = False
        start_timeout = kwargs.pop("start_timeout", 120)

        new_vm = self._clone(vm_name, timeout=timeout, **kwargs)
        if kwargs["power_on"]:
            desired_state = VmState.RUNNING
        else:
            desired_state = VmState.STOPPED
        new_vm.wait_for_state(desired_state, timeout=start_timeout)
        return new_vm


class VMWareSystem(System, VmMixin, TemplateMixin):
    """Client to Vsphere API

    Args:
        hostname: The hostname of the system.
        username: The username to connect with.
        password: The password to connect with.

    See also:

        vSphere Management SDK API docs
        https://developercenter.vmware.com/web/dp/doc/preview?id=155

    """
    _api = None

    _stats_available = {
        'num_vm': lambda self: len(self.list_vms()),
        'num_host': lambda self: len(self.list_host()),
        'num_cluster': lambda self: len(self.list_cluster()),
        'num_template': lambda self: len(self.list_templates()),
        'num_datastore': lambda self: len(self.list_datastore()),
    }

    can_suspend = True
    can_pause = False

    def __init__(self, hostname, username, password, **kwargs):
        super(VMWareSystem, self).__init__(**kwargs)
        self.hostname = hostname
        self.username = username
        self.password = password
        self._service_instance = None
        self._content = None
        self._vm_obj_cache = {}  # stores pyvmomi vm obj's we have previously pulled
        self.kwargs = kwargs

    @property
    def _identifying_attrs(self):
        return {'hostname': self.hostname}

    @property
    def can_suspend(self):
        return True

    @property
    def can_pause(self):
        return False

    def _start_keepalive(self):
        """
        Send a 'current time' request to vCenter every 10 min as a
        connection keep-alive
        """
        def _keepalive():
            while True:
                self.logger.debug(
                    "vCenter keep-alive: %s", self.service_instance.CurrentTime()
                )
                time.sleep(600)

        t = threading.Thread(target=_keepalive)
        t.daemon = True
        t.start()

    def _create_service_instance(self):
        """
        Create service instance and start a keep-alive thread

        See https://github.com/vmware/pyvmomi/issues/347 for why this is needed.
        """
        try:
            # Disable SSL cert verification
            context = ssl._create_unverified_context()
            context.verify_mode = ssl.CERT_NONE
            si = SmartConnect(
                host=self.hostname,
                user=self.username,
                pwd=self.password,
                sslContext=context
            )
        except Exception:
            self.logger.error("Failed to connect to vCenter")
            raise

        # Disconnect at teardown
        atexit.register(Disconnect, si)

        self.logger.info(
            "Connected to vCenter host %s as user %s",
            self.hostname, self.username
        )

        self._start_keepalive()
        return si

    @threaded_cached_property
    def service_instance(self):
        """An instance of the service"""
        self.logger.debug("Attempting to initiate vCenter service instance")
        return self._create_service_instance()

    @threaded_cached_property
    def content(self):
        self.logger.debug("calling RetrieveContent()... this might take awhile")
        return self.service_instance.RetrieveContent()

    @property
    def version(self):
        """The product version"""
        return LooseVersion(self.content.about.version)

    @property
    def default_resource_pool(self):
        return self.kwargs.get("default_resource_pool")

    def get_obj_list(self, vimtype, folder=None):
        """Get a list of objects of type ``vimtype``"""
        folder = folder or self.content.rootFolder
        container = self.content.viewManager.CreateContainerView(folder, [vimtype], True)
        return container.view

    def get_obj(self, vimtype, name, folder=None):
        """Get an object of type ``vimtype`` with name ``name`` from Vsphere"""
        obj = None
        for item in self.get_obj_list(vimtype, folder):
            if item.name == name:
                obj = item
                break
        return obj

    def _search_folders_for_vm(self, name):
        # First get all VM folders
        container = self.content.viewManager.CreateContainerView(
            self.content.rootFolder, [vim.Folder], True)
        folders = container.view
        container.Destroy()

        # Now search each folder for VM
        vm = None
        for folder in folders:
            vm = self.content.searchIndex.FindChild(folder, name)
            if vm:
                break

        return vm

    def _build_filter_spec(self, begin_entity, property_spec):
        """Build a search spec for full inventory traversal, adapted from psphere"""
        # Create selection specs
        selection_specs = [vmodl.query.PropertyCollector.SelectionSpec(name=ss)
                           for ss in SELECTION_SPECS]
        # Create traversal specs
        traversal_specs = []
        for spec_values in TRAVERSAL_SPECS:
            spec = vmodl.query.PropertyCollector.TraversalSpec()
            spec.name = spec_values['name']
            spec.type = spec_values['type']
            spec.path = spec_values['path']
            if spec_values.get('select_indices'):
                spec.selectSet = [selection_specs[i] for i in spec_values['select_indices']]
            traversal_specs.append(spec)
        # Create an object spec
        obj_spec = vmodl.query.PropertyCollector.ObjectSpec()
        obj_spec.obj = begin_entity
        obj_spec.selectSet = traversal_specs
        # Create a filter spec
        filter_spec = vmodl.query.PropertyCollector.FilterSpec()
        filter_spec.propSet = [property_spec]
        filter_spec.objectSet = [obj_spec]
        return filter_spec

    def get_updated_obj(self, obj):
        """
        Build a filter spec based on ``obj`` and return the updated object.

        Args:
             obj (pyVmomi.ManagedObject): The managed object to update, will be a specific subclass

        """
        # Set up the filter specs
        property_spec = vmodl.query.PropertyCollector.PropertySpec(type=type(obj), all=True)
        object_spec = vmodl.query.PropertyCollector.ObjectSpec(obj=obj)
        filter_spec = vmodl.query.PropertyCollector.FilterSpec()
        filter_spec.propSet = [property_spec]
        filter_spec.objectSet = [object_spec]
        # Get updates based on the filter
        property_collector = self.content.propertyCollector
        try:
            filter_ = property_collector.CreateFilter(filter_spec, True)
        except vmodl.fault.ManagedObjectNotFound:
            self.logger.warning('ManagedObjectNotFound when creating filter from spec {}'
                                .format(filter_spec))
            return
        update = property_collector.WaitForUpdates(None)
        if not update or not update.filterSet or not update.filterSet[0]:
            self.logger.warning('No object found when updating %s', str(obj))
            return
        if filter_:
            filter_.Destroy()
        return update.filterSet[0].objectSet[0].obj

    def _get_vm_or_template(self, name, force=False):
        """
        Find a VM or template with name 'name'

        Instead of using self._get_obj, this uses more efficient ways of
        searching for the VM since we can often have lots of VM's on the
        provider to sort through.

        Args:
            name (string): The name of the VM/template
            force (bool): Ignore the cache when updating
        Returns:
            VMWareVirtualMachine object, VMWareTemplate object, or None
        """
        if not name:
            raise ValueError('Invalid name: {}'.format(name))
        if name not in self._vm_obj_cache or force:
            self.logger.debug(
                "Searching all vm folders for vm/template '%s'", name)
            vm_obj = self._search_folders_for_vm(name)
            if not vm_obj:
                raise VMInstanceNotFound(name)
        else:
            vm_obj = self.get_updated_obj(self._vm_obj_cache[name])

        # If vm_obj is not found, return None.
        # Check if vm_obj.config is None as well, and also return None if that's the case.
        # Reason:
        #
        # https://github.com/vmware/pyvmomi/blob/master/docs/vim/VirtualMachine.rst
        # The virtual machine configuration is not guaranteed to be available
        # For example, the configuration information would be unavailable if the
        # server is unable to access the virtual machine files on disk, and is
        # often also unavailable during the initial phases of virtual machine creation.
        #
        # In such cases, from a wrapanapi POV, we'll treat the VM as if it doesn't exist
        if not vm_obj or not vm_obj.config:
            return None
        elif vm_obj.config.template:
            entity_cls = VMWareTemplate
        else:
            entity_cls = VMWareVirtualMachine

        self._vm_obj_cache[name] = vm_obj
        return entity_cls(system=self, name=name, raw=vm_obj)

    def get_vm(self, name, force=False):
        vm = self._get_vm_or_template(name, force)
        if not vm:
            raise VMInstanceNotFound(name)
        if isinstance(vm, VMWareTemplate):
            raise Exception("Looking for VM but found template of name '{}'".format(name))
        return vm

    def _list_vms_or_templates(self, template=False, inaccessible=False):
        """
        Obtains a list of all VMs or templates on the system.

        Args:
            template: A boolean describing if a list of templates should be returned

        Returns: A list of vim.VirtualMachine objects
        """
        # Use some pyVmomi internals to get vm propsets back directly with requested properties,
        # so we skip the network overhead of returning full managed objects
        property_spec = vmodl.query.PropertyCollector.PropertySpec()
        property_spec.all = False
        property_spec.pathSet = [
            'name', 'config.template',
            'config.uuid', 'runtime.connectionState'
        ]
        property_spec.type = vim.VirtualMachine
        pfs = self._build_filter_spec(self.content.rootFolder, property_spec)
        object_contents = self.content.propertyCollector.RetrieveProperties(specSet=[pfs])

        # Ensure get_template is either True or False to match the config.template property
        get_template = bool(template)

        # Select the vms or templates based on get_template and the returned properties
        obj_list = []
        for object_content in object_contents:
            # Nested property lookups work, but the attr lookup on the
            # vm object still triggers a request even though the vm
            # object already "knows" the answer in its cached object
            # content. So we just pull the value straight out of the cache.
            vm_props = {p.name: p.val for p in object_content.propSet}
            if vm_props.get('config.template') == get_template:
                if (vm_props.get('runtime.connectionState') == "inaccessible" and
                        inaccessible) or vm_props.get(
                            'runtime.connectionState') != "inaccessible":
                    obj_list.append(vm_props['name'])
        return obj_list

    def get_vm_from_ip(self, ip):
        """ Gets the name of a vm from its IP.

        Args:
            ip: The ip address of the vm.
        Returns: The vm name for the corresponding IP."""
        vms = self.content.searchIndex.FindAllByIp(ip=ip, vmSearch=True)
        # As vsphere remembers the last IP a vm had, when we search we get all
        # of them. Consequently we need to store them all in a dict and then sort
        # them to find out which one has the latest boot time. I am going out on
        # a limb and saying that searching for several vms and querying each object
        # is quicker than finding all machines and recording the bootTime and ip address
        # of each, before iterating through all of them to weed out the ones we care
        # about, but I could be wrong.
        boot_times = {}
        for vm in vms:
            if vm.name not in boot_times:
                boot_times[vm.name] = datetime.fromtimestamp(0)
                try:
                    boot_times[vm.name] = vm.summary.runtime.bootTime
                except Exception:
                    pass
        if boot_times:
            newest_boot_time = sorted(boot_times.items(), key=operator.itemgetter(1),
                                      reverse=True)[0]
            newest_vm = newest_boot_time[0]
            return VMWareVirtualMachine(system=self, name=newest_vm.name, raw=newest_vm)
        else:
            raise VMNotFoundViaIP('The requested IP is not known as a VM')

    def is_host_connected(self, host_name):
        host = self.get_obj(vim.HostSystem, name=host_name)
        return host.summary.runtime.connectionState == "connected"

    def create_vm(self, vm_name):
        raise NotImplementedError('This function has not yet been implemented.')

    def list_vms(self, inaccessible=False):
        return [
            VMWareVirtualMachine(system=self, name=obj_name)
            for obj_name in self._list_vms_or_templates(inaccessible=inaccessible)
        ]

    def find_vms(self, *args, **kwargs):
        raise NotImplementedError

    def list_templates(self):
        return [
            VMWareTemplate(system=self, name=obj_name)
            for obj_name in self._list_vms_or_templates(template=True)
        ]

    def find_templates(self, *args, **kwargs):
        raise NotImplementedError

    def create_template(self, *args, **kwargs):
        raise NotImplementedError

    def get_template(self, name, force=False):
        vm = self._get_vm_or_template(name, force)
        if not vm:
            raise NotFoundError("template: {}".format(name))
        if isinstance(vm, VMWareVirtualMachine):
            raise Exception("Looking for template but found VM of name '{}'".format(name))
        return vm

    def list_host(self):
        return [str(h.name) for h in self.get_obj_list(vim.HostSystem)]

    def list_host_datastore_url(self, host_name):
        host = self.get_obj(vim.HostSystem, name=host_name)
        return [str(d.summary.url) for d in host.datastore]

    def list_datastore(self):
        return [str(h.name) for h in self.get_obj_list(vim.Datastore) if h.host]

    def list_cluster(self):
        return [str(h.name) for h in self.get_obj_list(vim.ClusterComputeResource)]

    def list_resource_pools(self):
        return [str(h.name) for h in self.get_obj_list(vim.ResourcePool)]

    def list_networks(self):
        """Fetch the list of network names

        Returns: A list of Network names
        """
        return [str(h.name) for h in self.get_obj_list(vim.Network)]

    def info(self):
        # NOTE: Can't find these two methods in either psphere or suds
        # return '{} {}'.format(self.api.get_server_type(), self.api.get_api_version())
        return '{} {}'.format(self.content.about.apiType, self.content.about.apiVersion)

    def disconnect(self):
        pass

    def _task_wait(self, task):
        """
        Update a task and check its state. If the task state is not ``queued``, ``running`` or
        ``None``, then return the state. Otherwise return None.

        Args:
            task (pyVmomi.vim.Task): The task whose state is being monitored
        Returns:
            string: pyVmomi.vim.TaskInfo.state value if the task is not queued/running/None
        """
        task = self.get_updated_obj(task)
        if task.info.state not in ['queued', 'running', None]:
            return task.info.state

    def get_task_status(self, task):
        """Update a task and return its state, as a vim.TaskInfo.State string wrapper

        Args:
            task (pyVmomi.vim.Task): The task whose state is being returned
        Returns:
            string: pyVmomi.vim.TaskInfo.state value
        """
        task = self.get_updated_obj(task)
        return task.info.state

    def remove_host_from_cluster(self, host_name):
        host = self.get_obj(vim.HostSystem, name=host_name)
        task = host.DisconnectHost_Task()
        status, _ = wait_for(self._task_wait, [task])

        if status != 'success':
            raise HostNotRemoved("Host {} not removed: {}".format(
                host_name, get_task_error_message(task)))

        task = host.Destroy_Task()
        status, _ = wait_for(self._task_wait, [task], fail_condition=None)

        return status == 'success'

    def usage_and_quota(self):
        installed_ram = 0
        installed_cpu = 0
        used_ram = 0
        used_cpu = 0
        for host in self.get_obj_list(vim.HostSystem):
            installed_ram += host.systemResources.config.memoryAllocation.limit
            installed_cpu += host.summary.hardware.numCpuCores

        property_spec = vmodl.query.PropertyCollector.PropertySpec()
        property_spec.all = False
        property_spec.pathSet = ['name', 'config.template']
        property_spec.type = vim.VirtualMachine
        pfs = self._build_filter_spec(self.content.rootFolder, property_spec)
        object_contents = self.content.propertyCollector.RetrieveProperties(specSet=[pfs])
        for vm in object_contents:
            vm_props = {p.name: p.val for p in vm.propSet}
            if vm_props.get('config.template'):
                continue
            if vm.obj.summary.runtime.powerState.lower() != 'poweredon':
                continue
            used_ram += vm.obj.summary.config.memorySizeMB
            used_cpu += vm.obj.summary.config.numCpu

        return {
            # RAM
            'ram_used': used_ram,
            'ram_total': installed_ram,
            'ram_limit': None,
            # CPU
            'cpu_used': used_cpu,
            'cpu_total': installed_cpu,
            'cpu_limit': None,
        }

    def get_network(self, network_name):
        """Fetch the network object from specified network name

        Args:
            network_name: The name of the network from Vmware
        Returns: A object of vim.Network object
        """
        network = self.get_obj(vimtype=vim.Network, name=network_name)
        if not network:
            raise NotFoundError
        return network
