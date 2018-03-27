"""
Methods/classes pertaining to performing actions on a VM/instance

"""
from abc import ABCMeta, abstractmethod, abstractproperty

from wait_for import wait_for, TimedOutError

from .entity import Entity


class VMState(object):
    """
    Represents a state for a VM/instance on the provider system.
    """
    RUNNING = 'running'
    STOPPED = 'stopped'
    PAUSED = 'paused'
    SUSPENDED = 'suspended'
    DELETED = 'deleted'


class VM(Entity):
    """
    Represents a single VM/instance on a management system.

    Must be implemented by each system type
    """
    __metaclass__ = ABCMeta

    def __init__(self, system, name, *args, **kwargs):
        super(VM, self).__init__(system)
        self.name = name

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

    @abstractproperty
    def state_map(self):
        """
        Returns dict which maps state strings returned by system to a VMState.
        """
        return {
            VMState.RUNNING: 'running',
            VMState.STOPPED: 'stopped',
            VMState.PAUSED: 'paused',
            VMState.SUSPENDED: 'suspended',
            VMState.DELETED: 'deleted',
        }

    @abstractproperty
    def state(self):
        """Returns VMState object representing the VM's current state"""

    @abstractproperty
    def ip(self):
        """Returns IP address of the VM/instance"""

    @abstractmethod
    def wait_for_state(self, state, num_sec):
        """Waits for a VM to be in the desired state

        Args:
            state: desired VMState
            num_sec: number of seconds before timeout
        """

    @abstractmethod
    def ensure_state(self, state, num_sec):
        """Perform the actions required to get the VM to the desired state.

        Args:
            state: desired VMState
            num_sec: number of seconds before timeout
        """

    def in_steady_state(self):
        """Return whether the virtual machine is in a steady state

        Returns: boolean
        """
        return self.state in [VMState.RUNNING, VMState.STOPPED, VMState.SUSPENDED]

    def wait_for_steady_state(self, num_sec=None):
        """
        Waits for the system's steady_wait_time for VM to reach a steady state

        Args:
            num_sec: Time to wait to override default steady_wait_time
        """
        try:
            return wait_for(
                self.in_steady_state(),
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
        """Starts the VM/instance. Blocks until task completes.

        Returns: True if vm action has been initiated properly
        """

    @abstractmethod
    def stop(self):
        """Stops the VM/instance. Blocks until task completes.

        Returns: True if vm action has been initiated properly
        """

    @abstractmethod
    def restart(self):
        """Restarts the VM/instance. Blocks until task completes.

        Returns: True if vm action has been initiated properly
        """

    def suspend(self):
        """Suspend the VM/instance.  Blocks until task completes.

        Returns: True if vm action has been initiated properly
        """
        raise NotImplementedError('suspend not implemented.')

    def pause(self, vm_name):
        """Pauses the VM/instance.  Blocks until task completes.

        Returns: True if vm action has been initiated properly
        """
        raise NotImplementedError('pause not implemented.')

    def clone(self, vm_name):
        """Clone the VM to a new VM of name 'vm_name'.

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