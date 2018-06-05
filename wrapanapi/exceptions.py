class ActionNotSupported(Exception):
    """Raised when an action is not supported."""

    pass


class ActionTimedOutError(Exception):
    pass


class ForwardingRuleNotFound(Exception):
    """Raised if a Forwarding Rule for loadbalancers not found."""

    def __init__(self, forwarding_rule_name):
        self.vm_name = forwarding_rule_name


class ImageNotFoundError(Exception):
    pass


class InvalidValueException(Exception):
    """Raises when invalid value provided. E.g. invalid OpenShift project name"""

    pass


class LabelNotFoundException(Exception):
    """Raised when trying to delete a label which doesn't exist"""

    def __init__(self, label_key):
        self._label_key = label_key

    def __str__(self):
        return 'Could not delete label "{}" (label does not exist).'.format(
            self._label_key
        )


class KeystoneVersionNotSupported(Exception):
    """Raised when inappropriate version of Keystone is provided for Openstack system"""

    def __init__(self, ver):
        self.version = ver

    def __str__(self):
        return "Provided version of Keystone is not supported: {}".format(self.version)


class MultipleImagesError(Exception):
    pass


class NoMoreFloatingIPs(Exception):
    """Raised when provider runs out of FIPs."""


class MultipleInstancesError(Exception):
    def __init__(self, value):
        self.value = value

    def __str__(self):
        return repr(self.value)


class RestClientException(Exception):
    pass


class RequestFailedException(Exception):
    """Raised if some request returned unexpected status code"""

    pass


class ResourceAlreadyExistsException(Exception):
    """Raised when trying to create a resource that already exists"""

    pass


class NetworkNameNotFound(Exception):
    pass


class UncreatableResourceException(Exception):
    """Raised when trying to create uncreatable resource"""

    def __init__(self, resource):
        self.resource = resource

    def __str__(self):
        return "{} is not creatable resource"


class VMInstanceNotCloned(Exception):
    """Raised if a VM or instance is not found."""

    def __init__(self, template):
        self.template = template

    def __str__(self):
        return "Could not clone %s" % self.template


class VMInstanceNotFound(Exception):
    """Raised if a VM or instance is not found."""

    def __init__(self, vm_name):
        self.vm_name = vm_name

    def __str__(self):
        return "Could not find a VM/instance named %s." % self.vm_name


class ItemNotFound(Exception):
    """Raised if an item is not found."""

    def __init__(self, name, item_type):
        self.name = name
        self.item_type = item_type

    def __str__(self):
        return "Could not find a {} named {}.".format(self.item_type, self.name)


class VMInstanceNotSuspended(Exception):
    """Raised if a VM or instance is not able to be suspended."""

    def __init__(self, vm_name):
        self.vm_name = vm_name

    def __str__(self):
        return "Could not suspend %s because it's not running." % self.vm_name


class VMNotFoundViaIP(Exception):
    """
    Raised if a specific VM cannot be found.
    """

    pass


class HostNotRemoved(Exception):
    """Raised when :py:mod:`utils.mgmt_system` fails to remove host from cluster"""


class VMError(Exception):
    """Raised when a VM goes to the ERROR state."""


class VMCreationDateError(Exception):
    """Raised when we cannot determine a creation date for a VM"""

    pass
