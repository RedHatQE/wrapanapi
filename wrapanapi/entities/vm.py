"""
wrapanapi.entities.vm

Methods/classes pertaining to performing actions on a VM/instance
"""
from abc import ABCMeta, abstractmethod, abstractproperty
import time

from cached_property import cached_property_with_ttl
from wait_for import wait_for, TimedOutError

from wrapanapi.const import CACHED_PROPERTY_TTL
from wrapanapi.exceptions import MultipleItemsError, NotFoundError
from wrapanapi.entities.base import Entity, EntityMixin


class VmState(object):
    """
    Represents a state for a VM/instance on the provider system.

    Implementations of ``Vm`` should map to these states
    """
    RUNNING = 'VmState.RUNNING'
    STOPPED = 'VmState.STOPPED'
    PAUSED = 'VmState.PAUSED'
    SUSPENDED = 'VmState.SUSPENDED'
    DELETED = 'VmState.DELETED'
    STARTING = 'VmState.STARTING'
    STOPPING = 'VmState.STOPPING'
    ERROR = 'VmState.ERROR'
    UNKNOWN = 'VmState.UNKNOWN'
    SHELVED = 'VmState.SHELVED'
    SHELVED_OFFLOADED = 'VmState.SHELVED_OFFLOADED'

    @classmethod
    def valid_states(cls):
        return [
            var_val for _, var_val in vars(cls).items()
            if isinstance(var_val, basestring) and var_val.startswith('VmState.')
        ]


class Vm(Entity):
    """
    Represents a single VM/instance on a management system.
    """
    __metaclass__ = ABCMeta

    # Implementations must define a dict which maps API states returned by the
    # system to a VmState. Example:
    #    {'running': VmState.RUNNING, 'shutdown': VmState.STOPPED}
    state_map = None

    def __init__(self, *args, **kwargs):
        """
        Verify the required class variables are implemented during init

        Since abc has no 'abstract class property' concept, this is the approach taken.
        """
        state_map = getattr(self, 'state_map')
        if (not state_map or not isinstance(state_map, dict) or
                not all(value in VmState.valid_states() for value in state_map.values())):
            raise NotImplementedError(
                "property '{}' not properly implemented in class '{}'"
                .format('state_map', self.__class__.__name__)
            )
        super(Vm, self).__init__(*args, **kwargs)

    def _api_state_to_vmstate(self, api_state):
        """
        Use the state_map for this instance to map a state string into a VmState constant
        """
        try:
            return self.state_map[api_state]
        except KeyError:
            self.logger.warn(
                "Unmapped VM state '%s' received from system, mapped to '%s'",
                api_state, VmState.UNKNOWN
            )
            return VmState.UNKNOWN

    @property
    def exists(self):
        """Override entity 'exists' method to check if VM state is deleted."""
        try:
            state = self._get_state()
            exists = True
        except NotFoundError:
            exists = False

        if exists:
            # Even if the VM is retrievable, it does not exist if its state is 'DELETED'
            if state == VmState.DELETED:
                exists = False

        return exists

    @abstractmethod
    def _get_state(self):
        """
        Returns VMState object representing the VM's current state

        Should call self.refresh() first to get the latest status from the API
        """

    @cached_property_with_ttl(ttl=CACHED_PROPERTY_TTL)
    def state(self):
        """
        Returns a cached value of state.

        This avoids repeatedly querying for state for uses like:
        (self.is_running or self.is_stopped or self.is_paused)

        This property returns updated state only if the last time we retrieved state from
        the API is >1sec.

        This is the property that should be used when querying for VM state. _get_state
        shouldn't be the main method utilized.
        """
        return self._get_state()

    @property
    def is_running(self):
        """Return True if VM is running."""
        return self.state == VmState.RUNNING

    @property
    def is_started(self):
        """An alias for ``is_running``."""
        return self.is_running

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

    @property
    def is_starting(self):
        """Return True if VM is starting."""
        return self.state == VmState.STARTING

    @property
    def is_stopping(self):
        """Return True if VM is stopping."""
        return self.state == VmState.STOPPING

    @abstractproperty
    def ip(self):
        """
        Returns IP address of the VM/instance

        Should refresh if necessary to get most up-to-date info
        """

    @abstractproperty
    def creation_time(self):
        """
        Returns creation time of VM/instance
        """

    def wait_for_state(self, state, timeout='6m', delay=15):
        """
        Waits for a VM to be in the desired state

        Args:
            state: desired VmState
            timeout: wait_for timeout value
            delay: delay when looping to check for updated state
        """
        valid_states = self.state_map.values()
        if state not in valid_states:
            self.logger.error(
                "Invalid desired state. Valid states for %s: %s",
                self.__class__.__name__, valid_states
            )
            raise ValueError('Invalid desired state')

        wait_for(
            lambda: self.state == state,
            timeout=timeout,
            delay=delay,
            message="wait for vm {} to reach state '{}'".format(self._log_id, state))

    def _handle_transition(self, in_desired_state, in_state_requiring_prep, in_actionable_state,
                           do_prep, do_action, state, timeout, delay):
        """
        Handles state transition for ensure_state() method

        See that docstring below for explanation of the args. Each arg here is a callable except for
        'state', 'timeout' and 'delay'
        """
        def _transition():
            if in_desired_state():
                # Hacking around some race conditions -- double check that desired state is steady
                time.sleep(CACHED_PROPERTY_TTL + 0.1)
                if in_desired_state():
                    return True
                else:
                    return False
            elif in_state_requiring_prep():
                self.logger.info(
                    "VM %s in state requiring prep. current state: %s, ensuring state: %s)",
                    self._log_id, self.state, state
                )
                do_prep()
                return False
            elif in_actionable_state():
                self.logger.info(
                    "VM %s in actionable state. current state: %s, ensuring state: %s)",
                    self._log_id, self.state, state
                )
                do_action()
                return False

        return wait_for(
            _transition, timeout=timeout, delay=delay,
            message="ensure vm {} reaches state '{}'".format(self._log_id, state)
        )

    def ensure_state(self, state, timeout='6m', delay=5):
        """
        Perform the actions required to get the VM to the desired state.

        State can be one of:
            VmState.RUNNING, VmState.STOPPED, VmState.SUSPENDED, VmState.PAUSED

        Each desired state requires various checks/steps to ensure we "achieve" that state.

        The logic implied while waiting is:
        1. Check if VM is in_desired_state, if so, we're done
        2. If not, check if it is in_state_requiring_prep
            (for example, you can't stop a suspended VM, so we need to prep the VM by starting it)
        3. Move the VM to the correct 'prep state' (calling method do_prep)
        4. Check if the VM is in a state that allows moving to the desired state
            (in_actionable_state should return True)
        5. Perform the action to put the VM into that state (calling method do_action)

        The methods are defined differently for each desired state.

        For some states, step 2 and 3 do not apply, see for example when desired state is
        'running', in those cases just make sure that 'in_state_requiring_prep' always returns
        false.

        Args:
            state: desired VMState
            timeout: wait_for timeout value
            delay: delay when looping to check for new state
        """
        valid_states = self.state_map.values()
        if state not in valid_states:
            self.logger.error(
                "Invalid desired state. Valid states for %s: %s",
                self.__class__.__name__, valid_states
            )
            raise ValueError('Invalid desired state')

        if state == VmState.RUNNING:
            return self._handle_transition(
                in_desired_state=lambda: self.is_running,
                in_state_requiring_prep=lambda: False,
                in_actionable_state=lambda: self.is_stopped or self.is_suspended or self.is_paused,
                do_prep=lambda: None,
                do_action=self.start,
                state=state, timeout=timeout, delay=delay
            )
        elif state == VmState.STOPPED:
            return self._handle_transition(
                in_desired_state=lambda: self.is_stopped,
                in_state_requiring_prep=lambda: self.is_suspended or self.is_paused,
                in_actionable_state=lambda: self.is_running,
                do_prep=self.start,
                do_action=self.stop,
                state=state, timeout=timeout, delay=delay
            )
        elif state == VmState.SUSPENDED:
            if not self.system.can_suspend:
                raise ValueError(
                    'System {} is unable to suspend'.format(self.system.__class__.__name__))
            return self._handle_transition(
                in_desired_state=lambda: self.is_suspended,
                in_state_requiring_prep=lambda: self.is_stopped or self.is_paused,
                in_actionable_state=lambda: self.is_running,
                do_prep=self.start,
                do_action=self.suspend,
                state=state, timeout=timeout, delay=delay
            )
        elif state == VmState.PAUSED:
            if not self.system.can_pause:
                raise ValueError(
                    'System {} is unable to pause'.format(self.system.__class__.__name__))
            return self._handle_transition(
                in_desired_state=lambda: self.is_paused,
                in_state_requiring_prep=lambda: self.is_stopped or self.is_suspended,
                in_actionable_state=lambda: self.is_running,
                do_prep=self.start,
                do_action=self.pause,
                state=state, timeout=timeout, delay=delay
            )
        else:
            raise ValueError("Invalid desired state '{}'".format(state))

    @property
    def in_steady_state(self):
        """
        Return whether the virtual machine is in a steady state

        Returns: boolean
        """
        return self.state in [VmState.RUNNING, VmState.STOPPED, VmState.PAUSED, VmState.SUSPENDED]

    def wait_for_steady_state(self, timeout=None, delay=5):
        """
        Waits for the system's steady_wait_time for VM to reach a steady state

        Args:
            num_sec: Time to wait to override default steady_wait_time
        """
        try:
            return wait_for(
                lambda: self.in_steady_state,
                timeout=timeout if timeout else self.system.steady_wait_time,
                delay=delay,
                message="VM/Instance '{}' in steady state".format(self._log_id)
            )
        except TimedOutError:
            self.logger.exception(
                "VM %s stuck in '%s' while waiting for steady state.", self._log_id, self.state)
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

    def rename(self, name):
        """
        Rename VM/instance. Not supported on all platforms.
        """
        raise NotImplementedError('rename not implemented.')

    def suspend(self):
        """
        Suspends the VM/instance.  Blocks until task completes.

        This method may not always be implemented.

        Returns: True if vm action has been initiated properly
        """
        raise NotImplementedError('suspend not implemented.')

    def pause(self):
        """
        Pauses the VM/instance.  Blocks until task completes.

        This method may not always be implemented.

        Returns: True if vm action has been initiated properly
        """
        raise NotImplementedError('pause not implemented.')

    def clone(self, vm_name, **kwargs):
        """
        Clones the VM to a new VM of name 'vm_name'.

        This method may not always be implemented.

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


class VmMixin(EntityMixin):
    """
    Defines methods or properties a wrapanapi.systems.System that manages Vm's should have
    """
    __metaclass__ = ABCMeta

    # Implementations must define whether this system can suspend (True/False)
    can_suspend = None
    # Implementations must define whether this system can pause (True/False)
    can_pause = None
    # Implementations may override the amount of sec to wait for a VM to reach steady state
    steady_wait_time = 180

    def __init__(self, *args, **kwargs):
        """
        Verify the required class variables are implemented during init

        Since abc has no 'abstract class property' concept, this is the approach taken.
        """
        required_props = ['can_suspend', 'can_pause']
        for prop in required_props:
            prop_value = getattr(self, prop)
            if not isinstance(prop_value, bool):
                raise NotImplementedError(
                    "property '{}' must be implemented in class '{}'"
                    .format(prop, self.__class__.__name__)
                )

    @abstractproperty
    def can_suspend(self):
        """Return True if this system can suspend VM's/instances, False if not."""

    @abstractproperty
    def can_pause(self):
        """Return True if this system can pause VM's/instances, False if not."""

    @abstractmethod
    def get_vm(self, name, **kwargs):
        """
        Get VM from system with name 'name'

        This should return only ONE matching entity. If multiple entities match
        the criteria, a MultipleItemsError should be raised

        Returns:
            wrapanapi.entities.Vm if it exists
        Raises:
            wrapanapi.exceptions.MultipleItemsError if multiple matches are found
        """

    @abstractmethod
    def create_vm(self, name, **kwargs):
        """
        Create VM on system with name 'name'

        Returns:
            wrapanapi.entities.Vm for newly created VM
        """

    @abstractmethod
    def list_vms(self, **kwargs):
        """
        List VMs on system

        Returns:
            list of wrapanapi.entities.Vm
        """

    @abstractmethod
    def find_vms(self, name, **kwargs):
        """
        Find VMs on system based on name or other filters in kwargs

        Should return an empty list if no matches were found

        Returns:
            list of wrapanapi.entities.Vm for matches found
        """

    def does_vm_exist(self, name):
        """
        Checks if a VM with 'name' exists on the system

        If multiple VMs with the same name exist, this still returns 'True'
        """
        try:
            return bool(self.get_vm(name))
        except MultipleItemsError:
            return True
        except NotFoundError:
            return False
