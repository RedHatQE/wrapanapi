"""
wrapanapi.entities.server

Implements classes and methods related to actions performed on (physical) servers
"""
import six
from abc import ABCMeta, abstractmethod

from wrapanapi.entities.base import Entity


class ServerState(object):
    """
    Represents a state for a server on the provider system.

    Implementations of ``Server`` should map to these states
    """
    ON = 'ServerState.On'
    OFF = 'ServerState.Off'
    POWERING_ON = 'ServerState.PoweringOn'
    POWERING_OFF = 'ServerState.PoweringOff'
    UNKNOWN = 'ServerState.Unknown'

    @classmethod
    def valid_states(cls):
        return [
            var_val for _, var_val in vars(cls).items()
            if isinstance(var_val, six.string_types) and var_val.startswith('ServerState.')
        ]


class Server(six.with_metaclass(ABCMeta, Entity)):
    """
    Represents a single server on a management system.
    """
    # Implementations must define a dict which maps API states returned by the
    # system to a ServerState. Example:
    #    {'On': ServerState.ON, 'Off': ServerState.OFF}
    state_map = None

    def __init__(self, *args, **kwargs):
        """
        Verify the required class variables are implemented during init

        Since abc has no 'abstract class property' concept, this is the approach taken.
        """
        state_map = self.state_map
        if (not state_map or not isinstance(state_map, dict) or
                not all(value in ServerState.valid_states() for value in state_map.values())):
            raise NotImplementedError(
                "property '{}' not properly implemented in class '{}'"
                .format('state_map', self.__class__.__name__)
            )
        super(Server, self).__init__(*args, **kwargs)

    def _api_state_to_serverstate(self, api_state):
        """
        Use the state_map for this instance to map a state string into a ServerState constant
        """
        try:
            return self.state_map[api_state]
        except KeyError:
            self.logger.warn(
                "Unmapped Server state '%s' received from system, mapped to '%s'",
                api_state, ServerState.UNKNOWN
            )
            return ServerState.UNKNOWN

    @abstractmethod
    def _get_state(self):
        """
        Return ServerState object representing the server's current state.

        Should call self.refresh() first to get the latest status from the API
        """

    def delete(self):
        """Remove the entity on the provider. Not supported on servers."""
        raise NotImplementedError("Deleting not supported for servers")

    def cleanup(self):
        """
        Remove the entity on the provider and any of its associated resources.

        Not supported on servers.
        """
        raise NotImplementedError("Cleanup not supported for servers")

    @property
    def is_on(self):
        """Return True if the server is powered on."""
        return self._get_state() == ServerState.ON

    @property
    def is_off(self):
        """Return True if the server is powered off."""
        return self._get_state() == ServerState.OFF

    @property
    def is_powering_on(self):
        """Return True if the server is in the process of being powered on."""
        return self._get_state() == ServerState.POWERING_ON

    @property
    def is_powering_off(self):
        """Return True if the server is in the process of powered off."""
        return self._get_state() == ServerState.POWERING_OFF
