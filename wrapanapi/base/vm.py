"""
Methods/classes pertaining to performing actions on a VM/instance

"""
from abc import ABCMeta, abstractmethod, abstractproperty

from wait_for import wait_for, TimedOutError

from .entity import BaseEntity


class VMState(object):
    """
    Represents a state for a VM/instance on the provider system.
    """
    RUNNING = 'running'
    STOPPED = 'stopped'
    PAUSED = 'paused'
    SUSPENDED = 'suspended'
    DELETED = 'deleted'


class BaseVM(BaseEntity):
    """
    Represents a single VM/instance on a management system.

    Must be implemented by each system type
    """
    __metaclass__ = ABCMeta

    @property
    def exists(self):
        """Checks if the VM/instance of this name exists

        Returns:
            True if it exists
            False if not
            False if system's list_templates() method is not implemented.
        """
        try:
            return (self.name in v.name for v in self.system.list_vms())
        except NotImplementedError:
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
        raise NotImplementedError

    @abstractproperty
    def ip(self):
        """Returns IP address of the VM/instance"""
        raise NotImplementedError

    @abstractmethod
    def wait_for_state(self, state, num_sec):
        """Waits for a VM to be in the desired state

        Args:
            state: desired VMState
            num_sec: number of seconds before timeout
        """
        raise NotImplementedError('wait_for_state not implemented.')

    @abstractmethod
    def ensure_state(self, state, num_sec):
        """Perform the actions required to get the VM to the desired state.

        Args:
            state: desired VMState
            num_sec: number of seconds before timeout
        """
        raise NotImplementedError('ensure_state not implemented.')

    def in_steady_state(self):
        """Return whether the virtual machine is in a steady state

        Returns: boolean
        """
        return self.state in [VMState.RUNNING, VMState.STOPPED, VMState.SUSPENDED]

    def wait_for_steady_state(self):
        """
        Waits for the system's steady_wait_time for VM to reach a steady state
        """
        try:
            return wait_for(
                lambda: self.in_steady_state(),
                num_sec=self.system.steady_wait_time,
                delay=2,
                message="VM/Instance %s in steady state" % self.name
            )
        except TimedOutError:
            self.logger.exception(
                "VM {} got stuck in {} state when waiting for steady state.".format(
                    self.name, self.state))
            raise

    @abstractmethod
    def start(self):
        """Starts the VM/instance. Blocks until task completes.

        Returns: True if vm action has been initiated properly
        """
        raise NotImplementedError('start not implemented.')

    @abstractmethod
    def stop(self):
        """Stops the VM/instance. Blocks until task completes.

        Returns: True if vm action has been initiated properly
        """
        raise NotImplementedError('stop_vm not implemented.')

    @abstractmethod
    def restart(self):
        """Restarts the VM/instance. Blocks until task completes.

        Returns: True if vm action has been initiated properly
        """
        raise NotImplementedError('restart_vm not implemented.')

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
        raise NotImplementedError(
            'Provider {} does not implement get_hardware_configuration'
            .format(type(self.system).__name__)
        )