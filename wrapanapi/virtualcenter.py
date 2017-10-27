# coding: utf-8
"""Backend management system classes

Used to communicate with providers without using CFME facilities
"""
try:
    # In Fedora 22, we see SSL errors when connecting to vSphere, this prevents the error.
    import ssl
    ssl._create_default_https_context = ssl._create_unverified_context
except AttributeError:
    pass

import operator
import re
import time
from datetime import datetime
from distutils.version import LooseVersion
from functools import partial

import six
import pytz
from wait_for import wait_for, TimedOutError
from pyVmomi import vim, vmodl
from pyVim.connect import SmartConnect, Disconnect

from base import WrapanapiAPIBaseVM, VMInfo
from exceptions import (VMInstanceNotCloned, VMInstanceNotSuspended, VMNotFoundViaIP,
    HostNotRemoved, VMInstanceNotFound, VMCreationDateError)


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
    if hasattr(task.info.error, 'message'):
        message = str(task.info.error.message)
    elif hasattr(task.info.error, 'localizedMessage'):
        message = str(task.info.error.localizedMessage)
    elif hasattr(task.info.error, 'msg'):
        message = str(task.info.error.msg)
    else:
        message = 'Unknown error type: {}'.format(task.info.error)
    return message


class VMWareSystem(WrapanapiAPIBaseVM):
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
        'num_vm': lambda self: len(self.list_vm()),
        'num_host': lambda self: len(self.list_host()),
        'num_cluster': lambda self: len(self.list_cluster()),
        'num_template': lambda self: len(self.list_template()),
        'num_datastore': lambda self: len(self.list_datastore()),
    }
    POWERED_ON = 'poweredOn'
    POWERED_OFF = 'poweredOff'
    SUSPENDED = 'suspended'

    def __init__(self, hostname, username, password, **kwargs):
        super(VMWareSystem, self).__init__(kwargs)
        self.hostname = hostname
        self.username = username
        self.password = password
        self._service_instance = None
        self._content = None
        self._vm_cache = {}
        self.kwargs = kwargs

    def __del__(self):
        """Disconnect from the API when the object is deleted"""
        # This isn't the best place for this, but this class doesn't know when it is no longer in
        # use, and we need to do some sort of disconnect based on the pyVmomi documentation.
        if self._service_instance:
            Disconnect(self._service_instance)

    @property
    def service_instance(self):
        """An instance of the service"""
        if not self._service_instance:
            self._service_instance = SmartConnect(host=self.hostname, user=self.username,
                                                  pwd=self.password)
        return self._service_instance

    @property
    def content(self):
        """The content node"""
        if not self._content:
            self._content = self.service_instance.RetrieveContent()
        return self._content

    @property
    def version(self):
        """The product version"""
        return LooseVersion(self.content.about.version)

    @property
    def default_resource_pool(self):
        return self.kwargs.get("default_resource_pool")

    def _get_obj_list(self, vimtype, folder=None):
        """Get a list of objects of type ``vimtype``"""
        folder = folder or self.content.rootFolder
        container = self.content.viewManager.CreateContainerView(folder, [vimtype], True)
        return container.view

    def _get_obj(self, vimtype, name, folder=None):
        """Get an object of type ``vimtype`` with name ``name`` from Vsphere"""
        obj = None
        for item in self._get_obj_list(vimtype, folder):
            if item.name == name:
                obj = item
                break
        return obj

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

    def _get_updated_obj(self, obj):
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
        filter_ = property_collector.CreateFilter(filter_spec, True)
        update = property_collector.WaitForUpdates(None)
        if not update or not update.filterSet or not update.filterSet[0]:
            self.logger.warning('No object found when updating %s', str(obj))
            return
        if filter_:
            filter_.Destroy()
        return update.filterSet[0].objectSet[0].obj

    def _get_vm(self, vm_name, force=False):
        """Returns a vm from the VI object.

        Args:
            vm_name (string): The name of the VM
            force (bool): Ignore the cache when updating
        Returns:
             pyVmomi.vim.VirtualMachine: VM object
        """
        if vm_name not in self._vm_cache or force:
            vm = self._get_obj(vim.VirtualMachine, vm_name)
            if not vm:
                raise VMInstanceNotFound(vm_name)
            self._vm_cache[vm_name] = vm
        else:
            self._vm_cache[vm_name] = self._get_updated_obj(self._vm_cache[vm_name])
        return self._vm_cache[vm_name]

    def _get_resource_pool(self, resource_pool_name=None):
        """ Returns a resource pool managed object for a specified name.

        Args:
            resource_pool_name (string): The name of the resource pool. If None, first one will be
        picked.
        Returns:
             pyVmomi.vim.ResourcePool: The managed object of the resource pool.
        """
        if resource_pool_name is not None:
            return self._get_obj(vim.ResourcePool, resource_pool_name)
        elif self.default_resource_pool is not None:
            return self._get_obj(vim.ResourcePool, self.default_resource_pool)
        else:
            return self._get_obj_list(vim.ResourcePool)[0]

    def _task_wait(self, task):
        """
        Update a task and check its state. If the task state is not ``queued``, ``running`` or
        ``None``, then return the state. Otherwise return None.

        Args:
            task (pyVmomi.vim.Task): The task whose state is being monitored
        Returns:
            string: pyVmomi.vim.TaskInfo.state value if the task is not queued/running/None
        """
        task = self._get_updated_obj(task)
        if task.info.state not in ['queued', 'running', None]:
            return task.info.state

    def _task_status(self, task):
        """Update a task and return its state, as a vim.TaskInfo.State string wrapper

        Args:
            task (pyVmomi.vim.Task): The task whose state is being returned
        Returns:
            string: pyVmomi.vim.TaskInfo.state value
        """
        task = self._get_updated_obj(task)
        return task.info.state

    def does_vm_exist(self, name):
        """ Checks if a vm exists or not.

        Args:
            name: The name of the requested vm.
        Returns: A boolean, ``True`` if the vm exists, ``False`` if not.
        """
        try:
            return self._get_vm(name) is not None
        except VMInstanceNotFound:
            return False

    def current_ip_address(self, vm_name):
        ipv4_re = r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}'
        try:
            vm = self._get_vm(vm_name)
            ip_address = vm.summary.guest.ipAddress
            if not re.match(ipv4_re, ip_address) or ip_address == '127.0.0.1':
                ip_address = None
            return ip_address
        except (AttributeError, TypeError):
            # AttributeError: vm doesn't have an ip address yet
            # TypeError: ip address wasn't a string
            return None

    def get_ip_address(self, vm_name, timeout=600):
        """ Returns the first IP address for the selected VM.

        Args:
            vm_name: The name of the vm to obtain the IP for.
            timeout: The IP address wait timeout.
        Returns: A string containing the first found IP that isn't the loopback device.
        """
        try:
            ip_address, tc = wait_for(lambda: self.current_ip_address(vm_name),
                fail_condition=None, delay=5, num_sec=timeout,
                message="get_ip_address from vsphere")
        except TimedOutError:
            ip_address = None
        return ip_address

    def _get_list_vms(self, get_template=False, inaccessible=False):
        """ Obtains a list of all VMs on the system.

        Optional flag to obtain template names too.

        Args:
            get_template: A boolean describing if it should return template names also.
        Returns: A list of VMs.
        """
        # Use some pyVmomi internals to get vm propsets back directly with requested properties,
        # so we skip the network overhead of returning full managed objects
        property_spec = vmodl.query.PropertyCollector.PropertySpec()
        property_spec.all = False
        property_spec.pathSet = ['name', 'config.template', 'config.uuid',
            'runtime.connectionState']
        property_spec.type = vim.VirtualMachine
        pfs = self._build_filter_spec(self.content.rootFolder, property_spec)
        object_contents = self.content.propertyCollector.RetrieveProperties(specSet=[pfs])

        # Ensure get_template is either True or False to match the config.template property
        get_template = bool(get_template)

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

    def all_vms(self):
        property_spec = vmodl.query.PropertyCollector.PropertySpec()
        property_spec.all = False
        property_spec.pathSet = ['name', 'config.template']
        property_spec.type = 'VirtualMachine'
        pfs = self._build_filter_spec(self.content.rootFolder, property_spec)
        object_contents = self.content.propertyCollector.RetrieveProperties(specSet=[pfs])
        result = []
        for vm in object_contents:
            vm_props = {p.name: p.val for p in vm.propSet}
            if vm_props.get('config.template'):
                continue
            try:
                ip = str(vm.obj.summary.guest.ipAddress)
            except AttributeError:
                ip = None
            try:
                uuid = str(vm.obj.summary.config.uuid)
            except AttributeError:
                uuid = None
            result.append(
                VMInfo(
                    uuid,
                    str(vm.obj.summary.config.name),
                    str(vm.obj.summary.runtime.powerState),
                    ip,
                )
            )
        return result

    def get_vm_guid(self, vm_name):
        vm = self._get_vm(vm_name)
        try:
            return str(vm.summary.config.uuid)
        except AttributeError:
            return None

    def get_vm_name_from_ip(self, ip):
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
                except:
                    pass
        if boot_times:
            newest_boot_time = sorted(boot_times.items(), key=operator.itemgetter(1),
                                      reverse=True)[0]
            return newest_boot_time[0]
        else:
            raise VMNotFoundViaIP('The requested IP is not known as a VM')

    def start_vm(self, vm_name):
        self.wait_vm_steady(vm_name)
        if self.is_vm_running(vm_name):
            self.logger.info(" vSphere VM %s is already running" % vm_name)
            return True
        else:
            self.logger.info(" Starting vSphere VM %s" % vm_name)
            vm = self._get_vm(vm_name)
            vm.PowerOnVM_Task()
            self.wait_vm_running(vm_name)
            return True

    def stop_vm(self, vm_name):
        self.wait_vm_steady(vm_name)
        if self.is_vm_stopped(vm_name):
            self.logger.info(" vSphere VM %s is already stopped" % vm_name)
            return True
        else:
            self.logger.info(" Stopping vSphere VM %s" % vm_name)
            vm = self._get_vm(vm_name)
            if self.is_vm_suspended(vm_name):
                self.logger.info(
                    " Resuming suspended VM %s before stopping." % vm_name
                )
                vm.PowerOnVM_Task()
                self.wait_vm_running(vm_name)
            vm.PowerOffVM_Task()
            self.wait_vm_stopped(vm_name)
            return True

    def delete_vm(self, vm_name):
        self.wait_vm_steady(vm_name)
        self.logger.info(" Deleting vSphere VM %s" % vm_name)
        vm = self._get_vm(vm_name)
        self.stop_vm(vm_name)

        task = vm.Destroy_Task()

        try:
            wait_for(lambda: self._task_status(task) == 'success', delay=3, num_sec=600)
            return self._task_status(task) == 'success'
        except TimedOutError:
            return False

    def is_host_connected(self, host_name):
        host = self._get_obj(vim.HostSystem, name=host_name)
        return host.summary.runtime.connectionState == "connected"

    def create_vm(self, vm_name):
        raise NotImplementedError('This function has not yet been implemented.')

    def restart_vm(self, vm_name):
        self.logger.info(" Restarting vSphere VM %s" % vm_name)
        return self.stop_vm(vm_name) and self.start_vm(vm_name)

    def list_vm(self, inaccessible=False):
        return self._get_list_vms(inaccessible=inaccessible)

    def list_template(self):
        return self._get_list_vms(get_template=True)

    def list_flavor(self):
        raise NotImplementedError('This function is not supported on this platform.')

    def list_host(self):
        return [str(h.name) for h in self._get_obj_list(vim.HostSystem)]

    def list_host_datastore_url(self, host_name):
        host = self._get_obj(vim.HostSystem, name=host_name)
        return [str(d.summary.url) for d in host.datastore]

    def list_datastore(self):
        return [str(h.name) for h in self._get_obj_list(vim.Datastore) if h.host]

    def list_cluster(self):
        return [str(h.name) for h in self._get_obj_list(vim.ClusterComputeResource)]

    def list_resource_pools(self):
        return [str(h.name) for h in self._get_obj_list(vim.ResourcePool)]

    def info(self):
        # NOTE: Can't find these two methods in either psphere or suds
        # return '{} {}'.format(self.api.get_server_type(), self.api.get_api_version())
        return '{} {}'.format(self.content.about.apiType, self.content.about.apiVersion)

    def connect(self):
        pass

    def disconnect(self):
        pass

    def vm_status(self, vm_name):
        return str(self._get_vm(vm_name, force=True).runtime.powerState)

    def vm_creation_time(self, vm_name):
        """Detect the vm_creation_time either via uptime if non-zero, or by last boot time

        The API provides no sensible way to actually get this value. The only way in which
        vcenter API MAY have this is by filtering through events

        Return tz-naive datetime object
        """
        vm = self._get_vm(vm_name)

        filter_spec = vim.event.EventFilterSpec(
            entity=vim.event.EventFilterSpec.ByEntity(
                entity=vm, recursion=vim.event.EventFilterSpec.RecursionOption.self),
            eventTypeId=['VmDeployedEvent', 'VmCreatedEvent'])
        collector = self.content.eventManager.CreateCollectorForEvents(filter=filter_spec)
        collector.SetCollectorPageSize(1000)  # max allowed value
        events = collector.latestPage
        collector.DestroyCollector()  # limited number of collectors allowed per client

        if events:
            creation_time = events.pop().createdTime  # datetime object
        else:
            # no events found for VM, fallback to last boot time
            creation_time = vm.runtime.bootTime
            if not creation_time:
                raise VMCreationDateError('Could not find a creation date for {}'.format(vm_name))
        # localize and make tz-naive
        return creation_time.astimezone(pytz.UTC)

    def get_vm_host_name(self, vm_name):
        vm = self._get_vm(vm_name)
        return str(vm.runtime.host.name)

    def get_vm_datastore_path(self, vm_name, vm_config_datastore):
        vm = self._get_vm(vm_name)
        datastore_url = [str(datastore.url)
                         for datastore in vm.config.datastoreUrl
                         if datastore.name in vm_config_datastore]
        return datastore_url.pop()

    def get_vm_config_files_path(self, vm_name):
        vm = self._get_vm(vm_name)
        vmfilespath = vm.config.files.vmPathName
        return str(vmfilespath)

    def in_steady_state(self, vm_name):
        return self.vm_status(vm_name) in {self.POWERED_ON, self.POWERED_OFF, self.SUSPENDED}

    def is_vm_running(self, vm_name):
        return self.vm_status(vm_name) == self.POWERED_ON

    def wait_vm_running(self, vm_name, num_sec=240):
        self.logger.info(" Waiting for vSphere VM %s to change status to ON" % vm_name)
        wait_for(self.is_vm_running, [vm_name], num_sec=num_sec)

    def is_vm_stopped(self, vm_name):
        return self.vm_status(vm_name) == self.POWERED_OFF

    def wait_vm_stopped(self, vm_name, num_sec=240):
        self.logger.info(" Waiting for vSphere VM %s to change status to OFF" % vm_name)
        wait_for(self.is_vm_stopped, [vm_name], num_sec=num_sec)

    def is_vm_suspended(self, vm_name):
        return self.vm_status(vm_name) == self.SUSPENDED

    def wait_vm_suspended(self, vm_name, num_sec=360):
        self.logger.info(" Waiting for vSphere VM %s to change status to SUSPENDED" % vm_name)
        wait_for(self.is_vm_suspended, [vm_name], num_sec=num_sec)

    def suspend_vm(self, vm_name):
        self.wait_vm_steady(vm_name)
        self.logger.info(" Suspending vSphere VM %s" % vm_name)
        vm = self._get_vm(vm_name)
        if self.is_vm_stopped(vm_name):
            raise VMInstanceNotSuspended(vm_name)
        else:
            vm.SuspendVM_Task()
            self.wait_vm_suspended(vm_name)
            return True

    def rename_vm(self, vm_name, new_vm_name):
        vm = self._get_vm(vm_name)
        task = vm.Rename_Task(newName=new_vm_name)
        # Cycle until the new named vm is found
        # That must happen or the error state can come up too
        while not self.does_vm_exist(new_vm_name):
            task = self._get_updated_obj(task)
            if task.info.state == "error":
                return vm_name  # Old vm name if error
            time.sleep(0.5)
        else:
            # The newly renamed VM is found
            return new_vm_name

    @staticmethod
    def _progress_log_callback(logger, source, destination, progress):
        logger.info("Provisioning progress {}->{}: {}".format(
            source, destination, str(progress)))

    def _pick_datastore(self, allowed_datastores):
        # Pick a datastore by space
        possible_datastores = [
            ds for ds in self._get_obj_list(vim.Datastore)
            if ds.name in allowed_datastores and ds.summary.accessible and
            ds.summary.multipleHostAccess and ds.overallStatus != "red"]
        possible_datastores.sort(
            key=lambda ds: float(ds.summary.freeSpace) / float(ds.summary.capacity),
            reverse=True)
        if not possible_datastores:
            raise Exception("No possible datastores!")
        return possible_datastores[0]

    def clone_vm(self, source, destination, resourcepool=None, datastore=None, power_on=True,
                 sparse=False, template=False, provision_timeout=1800, progress_callback=None,
                 allowed_datastores=None, cpu=None, ram=None, **kwargs):
        """Clone a VM"""
        try:
            vm = self._get_obj(vim.VirtualMachine, name=destination)
            if vm and vm.name == destination:
                raise Exception("VM already present!")
        except VMInstanceNotFound:
            pass

        if progress_callback is None:
            progress_callback = partial(self._progress_log_callback, self.logger,
                source, destination)

        source_template = self._get_vm(source)

        vm_clone_spec = vim.VirtualMachineCloneSpec()
        vm_reloc_spec = vim.VirtualMachineRelocateSpec()
        # DATASTORE
        if isinstance(datastore, six.string_types):
            vm_reloc_spec.datastore = self._get_obj(vim.Datastore, name=datastore)
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

        vm_reloc_spec.host = None
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

        task = source_template.CloneVM_Task(folder=folder, name=destination, spec=vm_clone_spec)

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
            else:
                store[0] = self._get_updated_obj(store[0])
                return False

        wait_for(_check, num_sec=provision_timeout, delay=4)

        if task.info.state != 'success':
            self.logger.error('Clone VM failed: %s', get_task_error_message(task))
            raise VMInstanceNotCloned(source)
        else:
            return destination

    def mark_as_template(self, vm_name, **kwargs):
        self._get_obj(vim.VirtualMachine, name=vm_name).MarkAsTemplate()  # Returns None

    def deploy_template(self, template, **kwargs):
        kwargs["power_on"] = kwargs.pop("power_on", True)
        kwargs["template"] = False
        destination = kwargs.pop("vm_name")
        start_timeout = kwargs.pop("timeout", 1800)
        self.clone_vm(template, destination, **kwargs)
        if kwargs["power_on"]:
            self.wait_vm_running(destination, num_sec=start_timeout)
        else:
            self.wait_vm_stopped(destination, num_sec=start_timeout)
        return destination

    def remove_host_from_cluster(self, host_name):
        host = self._get_obj(vim.HostSystem, name=host_name)
        task = host.DisconnectHost_Task()
        status, t = wait_for(self._task_wait, [task])

        if status != 'success':
            raise HostNotRemoved("Host {} not removed: {}".format(
                host_name, get_task_error_message(task)))

        task = host.Destroy_Task()
        status, t = wait_for(self._task_wait, [task], fail_condition=None)

        return status == 'success'

    def vm_hardware_configuration(self, vm_name):
        vm = self._get_vm(vm_name)
        return {
            'ram': vm.config.hardware.memoryMB,
            'cpu': vm.config.hardware.numCPU,
        }

    def usage_and_quota(self):
        installed_ram = 0
        installed_cpu = 0
        used_ram = 0
        used_cpu = 0
        for host in self._get_obj_list(vim.HostSystem):
            installed_ram += host.systemResources.config.memoryAllocation.limit
            installed_cpu += host.summary.hardware.numCpuCores

        property_spec = vmodl.query.PropertyCollector.PropertySpec()
        property_spec.all = False
        property_spec.pathSet = ['name', 'config.template']
        property_spec.type = 'VirtualMachine'
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

    def add_disk_to_vm(self, vm_name, capacity_in_kb, provision_type=None, unit=None):
        """
        Create a disk on the given datastore (by name)

        Community Example used
        https://github.com/vmware/pyvmomi-community-samples/blob/master/samples/add_disk_to_vm.py

        Return task type from Task.result or Task.error
        https://github.com/vmware/pyvmomi/blob/master/docs/vim/TaskInfo.rst

        Args:
            vm_name (string): name of the vm to add disk to
            capacity_in_kb (int): capacity of the new drive in Kilobytes
            provision_type (string): 'thin' or 'thick', will default to thin if invalid option
            unit (int): The unit number of the disk to add, use to override existing disk. Will
                search for next available unit number by default

        Returns:
            (bool, task_result): Tuple containing boolean True if task ended in success,
                                 and the contents of task.result or task.error depending on state
        """
        provision_type = provision_type if provision_type in ['thick', 'thin'] else 'thin'
        vm = self._get_vm(vm_name=vm_name)

        # if passed unit matches existing device unit, match these values too
        key = None
        controller_key = None
        unit_number = None
        virtual_disk_devices = [
            device for device
            in vm.config.hardware.device if isinstance(device, vim.vm.device.VirtualDisk)]
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
        task = vm.ReconfigVM_Task(spec=vm_spec)

        def task_complete(task_obj):
            status = task_obj.info.state
            return status not in ['running', 'queued']

        try:
            wait_for(task_complete, [task])
        except TimedOutError:
            self.logger.exception('Task did not go to success state: {}'.format(task))
        finally:
            if task.info.state == 'success':
                result = (True, task.info.result)
            elif task.info.state == 'error':
                result = (False, task.info.error)
            else:  # shouldn't happen
                result = (None, None)
            return result
