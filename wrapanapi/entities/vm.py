"""
Methods/classes pertaining to performing actions on a VM/instance

"""
from abc import ABCMeta, abstractmethod, abstractproperty

from wait_for import wait_for, TimedOutError

from .entity import Entity, EntityMixin


class VmState(object):
    """
    Represents a state for a VM/instance on the provider system.
    """
    RUNNING = 'running'
    STOPPED = 'stopped'
    PAUSED = 'paused'
    SUSPENDED = 'suspended'
    DELETED = 'deleted'
    UNKNOWN = 'unknown'


class Vm(Entity):
    """
    Represents a single VM/instance on a management system.

    Must be implemented by each system type
    """
    __metaclass__ = ABCMeta

    def __init__(self, system, name, *args, **kwargs):
        super(Vm, self).__init__(system)
        self._name = name

    @abstractproperty
    def name(self):
        """
        Returns name of Vm
        """

    @property
    def exists(self):
        """Checks if the VM/instance of this name exists

        Returns:
            True if it exists
            False if not
        Raises:
            NotImplementedError if system's list_vms() method undefined.
        """
        for vm in self.system.list_vms():
            if self.name == vm.name:
                return True
        return False

    @staticmethod
    @abstractproperty
    def state_map():
        """
        Returns dict which maps state strings returned by system to a VMState.
        """
        return {
            'running': VmState.RUNNING,
            'stopped': VmState.STOPPED,
            'paused': VmState.PAUSED,
            'suspended': VmState.SUSPENDED,
            'deleted': VmState.DELETED,
        }

    @staticmethod
    def _api_state_to_vmstate(api_state):
        try:
            return Vm.state_map[api_state]
        except KeyError:
            raise KeyError(
                'Unknown VM state received from system: {}'.format(api_state))

    @abstractproperty
    def state(self):
        """
        Returns VMState object representing the VM's current state
        """

    @property
    def is_running(self):
        """Return True if VM is running."""
        return self.state == VmState.RUNNING

    @property
    def is_stopped(self):
        """Return True if VM is stopped."""
        return self.state == VmState.STOPPED

    @property
    def is_paused(self):
        """Return True if VM is paused."""
        return self.state == VmState.PAUSED

    @property
    def is_suspended(self):
        """Return True if VM is suspended."""
        return self.state == VmState.SUSPENDED

    @abstractproperty
    def ip(self):
        """
        Returns IP address of the VM/instance
        """

    @abstractproperty
    def creation_time(self):
        """
        Returns creation time of VM/instance
        """

    def wait_for_state(self, state, num_sec):
        """
        Waits for a VM to be in the desired state

        Args:
            state: desired VMState
            num_sec: number of seconds before timeout
        """
        # TODO: copy from cfme

    def ensure_state(self, state, num_sec):
        """
        Perform the actions required to get the VM to the desired state.

        Args:
            state: desired VMState
            num_sec: number of seconds before timeout
        """
        # TODO: copy from cfme

    @property
    def in_steady_state(self):
        """
        Return whether the virtual machine is in a steady state

        Returns: boolean
        """
        return self.state in [VmState.RUNNING, VmState.STOPPED, VmState.SUSPENDED]

    def wait_for_steady_state(self, num_sec=None):
        """
        Waits for the system's steady_wait_time for VM to reach a steady state

        Args:
            num_sec: Time to wait to override default steady_wait_time
        """
        try:
            return wait_for(
                self.in_steady_state,
                num_sec=num_sec if num_sec else self.system.steady_wait_time,
                delay=2,
                message="VM/Instance %s in steady state" % self.name
            )
        except TimedOutError:
            self.logger.exception(
                "VM '{}' stuck in '{}' while waiting for steady state.".format(
                    self.name, self.state))
            raise

    @abstractmethod
    def start(self):
        """
        Starts the VM/instance. Blocks until task completes.

        Returns: True if vm action has been initiated properly
        """

    @abstractmethod
    def stop(self):
        """
        Stops the VM/instance. Blocks until task completes.

        Returns: True if vm action has been initiated properly
        """

    @abstractmethod
    def restart(self):
        """
        Restarts the VM/instance. Blocks until task completes.

        Returns: True if vm action has been initiated properly
        """

    def suspend(self):
        """
        Suspends the VM/instance.  Blocks until task completes.

        Returns: True if vm action has been initiated properly
        """
        raise NotImplementedError('suspend not implemented.')

    def pause(self, vm_name):
        """
        Pauses the VM/instance.  Blocks until task completes.

        Returns: True if vm action has been initiated properly
        """
        raise NotImplementedError('pause not implemented.')

    def clone(self, vm_name):
        """
        Clones the VM to a new VM of name 'vm_name'.

        Args:
            vm_name: The name of the new VM
        Returns: VM object for the new VM
        """
        raise NotImplementedError('clone not implemented.')

    def get_hardware_configuration(self):
        """Return hardware configuration of the VM."""
        raise NotImplementedError(
            'Provider {} does not implement get_hardware_configuration'
            .format(type(self.system).__name__)
        )

    def set_meta_value(self, key, value):
        """
        Set meta value for VM/instance.
        
        Args:
            key: key
            value: value
        """
        raise NotImplementedError('set_meta_value not implemented')

    def get_meta_value(self, key):
        """
        Get meta value for VM/instance.

        Args:
            key: key
        """
        raise NotImplementedError('get_meta_value not implemented')


class CloudInstance(Vm):
    """
    Adds a few additional properties/methods pertaining to VMs hosted
    on a cloud platform.
    """
    @abstractproperty
    def type(self):
        """
        Return type or flavor of the VM

        E.g. 'm1.micro' in ec2
        """


class VmMixin(EntityMixin):
    """
    Defines methods or properties a wrapanapi.systems.System that manages Vm's should have
    """
    __metaclass__ = ABCMeta

    @classmethod
    @abstractproperty
    def can_suspend(cls):
        """Indicates whether this system can suspend VM's/instances."""
        return False

    @classmethod
    @abstractproperty
    def can_pause(cls):
        """Indicates whether this system can pause VM's/instances."""
        return False

    @classmethod
    @abstractproperty
    def steady_wait_time(cls):
        """Returns default secs to wait for VM to move into a steady state."""
        return 120

    @abstractmethod
    def get_vm(self, *args, **kwargs):
        """
        Get VM from system

        Returns:
            Instance of wrapanapi.entities.Vm if it exists
        """

    @abstractmethod
    def create_vm(self, *args, **kwargs):
        """
        Create VM on system

        Returns:
            wrapanapi.entities.Vm for newly created virtual machine
        """

    @abstractmethod
    def list_vms(self, *args, **kwargs):
        """
        List VMs on system

        Returns:
            list of wrapanapi.entities.Vm
        """

    @abstractmethod
    def find_vms(self, *args, **kwargs):
        """
        Find a VM on system based on args/kwargs

        Returns:
            list of wrapanapi.entities.Vm
        """

    @abstractmethod
    def does_vm_exist(self, name, *args, **kwargs):
        """
        Return True if VM with 'name' exists

        Returns:
            True if the VM exists
            False if not
        """