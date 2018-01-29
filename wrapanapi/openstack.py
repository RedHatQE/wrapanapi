# coding: utf-8
"""Backend management system classes

Used to communicate with providers without using CFME facilities
"""
from __future__ import absolute_import
from contextlib import contextmanager
from datetime import datetime
from functools import partial

import json
import time
import pytz
from cinderclient.v2 import client as cinderclient
from cinderclient import exceptions as cinder_exceptions
from heatclient import client as heat_client
from keystoneauth1.identity import Password
from keystoneauth1.session import Session
from keystoneclient import client as keystone_client
from novaclient import client as osclient
from novaclient import exceptions as os_exceptions
from novaclient.client import SessionClient
from novaclient.v2.floating_ips import FloatingIP
from novaclient.v2.servers import Server
from requests.exceptions import Timeout
from wait_for import wait_for

from .base import WrapanapiAPIBaseVM, VMInfo
from .exceptions import (
    NoMoreFloatingIPs, NetworkNameNotFound, VMInstanceNotFound, VMNotFoundViaIP,
    ActionTimedOutError, VMError, KeystoneVersionNotSupported
)


# TODO The following monkeypatch nonsense is criminal, and would be
# greatly simplified if openstack made it easier to specify a custom
# client class. This is a trivial PR that they're likely to accept.

# Note: This same mechanism may be required for keystone and cinder
# clients, but hopefully won't be.

# monkeypatch method to add retry support to openstack
def _request_timeout_handler(self, url, method, retry_count=0, **kwargs):
    try:
        # Use the original request method to do the actual work
        return SessionClient.request(self, url, method, **kwargs)
    except Timeout:
        if retry_count >= 3:
            self._cfme_logger.error('nova request timed out after {} retries'.format(retry_count))
            raise
        else:
            # feed back into the replaced method that supports retry_count
            retry_count += 1
            self._cfme_logger.info('nova request timed out; retry {}'.format(retry_count))
            return self.request(url, method, retry_count=retry_count, **kwargs)


class OpenstackSystem(WrapanapiAPIBaseVM):
    """Openstack management system

    Uses novaclient.

    Args:
        tenant: The tenant to log in with.
        username: The username to connect with.
        password: The password to connect with.
        auth_url: The authentication url.

    """

    _stats_available = {
        'num_vm': lambda self: len(self._get_all_instances(True)),
        'num_template': lambda self: len(self.list_template()),
    }

    states = {
        'paused': ('PAUSED',),
        'running': ('ACTIVE',),
        'stopped': ('SHUTOFF',),
        'suspended': ('SUSPENDED',),
    }

    can_suspend = True
    can_pause = True

    def __init__(self, **kwargs):
        super(OpenstackSystem, self).__init__(kwargs)
        self.tenant = kwargs['tenant']
        self.username = kwargs['username']
        self.password = kwargs['password']
        self.auth_url = kwargs['auth_url']
        self.keystone_version = kwargs.get('keystone_version', 2)
        if int(self.keystone_version) not in (2, 3):
            raise KeystoneVersionNotSupported(self.keystone_version)
        self.domain_id = kwargs['domain_id'] if self.keystone_version == 3 else None
        self._session = None
        self._api = None
        self._kapi = None
        self._capi = None
        self._tenant_api = None
        self._stackapi = None

    @property
    def session(self):
        if not self._session:
            auth_kwargs = dict(auth_url=self.auth_url, username=self.username,
                               password=self.password, project_name=self.tenant)
            if self.keystone_version == 3:
                auth_kwargs.update(dict(user_domain_id=self.domain_id,
                                        project_domain_name=self.domain_id))
            pass_auth = Password(**auth_kwargs)
            self._session = Session(auth=pass_auth, verify=False)
        return self._session

    @property
    def api(self):
        if not self._api:
            self._api = osclient.Client('2', session=self.session, service_type="compute",
                                        timeout=30)
            # replace the client request method with our version that
            # can handle timeouts; uses explicit binding (versus
            # replacing the method directly on the HTTPClient class)
            # so we can still call out to HTTPClient's original request
            # method in the timeout handler method
            self._api.client._cfme_logger = self.logger
            self._api.client.request = _request_timeout_handler.__get__(self._api.client,
                                                                        SessionClient)
        return self._api

    @property
    def kapi(self):
        if not self._kapi:
            self._kapi = keystone_client.Client(session=self.session)
        return self._kapi

    @property
    def tenant_api(self):
        if not self._tenant_api:
            if self.keystone_version == 2:
                self._tenant_api = self.kapi.tenants
            elif self.keystone_version == 3:
                self._tenant_api = self.kapi.projects

        return self._tenant_api

    @property
    def capi(self):
        if not self._capi:
            self._capi = cinderclient.Client(session=self.session, service_type="volume")
        return self._capi

    @property
    def stackapi(self):
        if not self._stackapi:
            heat_endpoint = self.kapi.session.auth.auth_ref.service_catalog.url_for(
                service_type='orchestration'
            )
            self._stackapi = heat_client.Client('1', heat_endpoint,
                                                token=self.kapi.session.auth.auth_ref.auth_token,
                                                insecure=True)
        return self._stackapi

    def _get_tenants(self):

        if self.keystone_version == 3:
            return self.tenant_api.list()
        real_tenants = []
        tenants = self.tenant_api.list()
        for tenant in tenants:
            users = tenant.list_users()
            user_list = [user.name for user in users]
            if self.username in user_list:
                real_tenants.append(tenant)
        return real_tenants

    def _get_tenant(self, **kwargs):
        return self.tenant_api.find(**kwargs).id

    def _get_user(self, **kwargs):
        return self.kapi.users.find(**kwargs).id

    def _get_role(self, **kwargs):
        return self.kapi.roles.find(**kwargs).id

    def add_tenant(self, tenant_name, description=None, enabled=True, user=None, roles=None,
                   domain=None):
        params = dict(description=description,
                      enabled=enabled)
        if self.keystone_version == 2:
            params['tenant_name'] = tenant_name
        elif self.keystone_version == 3:
            params['name'] = tenant_name
            params['domain'] = domain
        tenant = self.tenant_api.create(**params)
        if user and roles:
            if self.keystone_version == 3:
                raise NotImplementedError('Role assignments for users are not implemented yet for '
                                          'Keystone V3')
            user = self._get_user(name=user)
            for role in roles:
                role_id = self._get_role(name=role)
                tenant.add_user(user, role_id)
        return tenant.id

    def list_tenant(self):
        return [i.name for i in self._get_tenants()]

    def remove_tenant(self, tenant_name):
        tid = self._get_tenant(name=tenant_name)
        self.tenant_api.delete(tid)

    def start_vm(self, instance_name):
        self.logger.info(" Starting OpenStack instance %s" % instance_name)
        if self.is_vm_running(instance_name):
            return True

        instance = self._find_instance_by_name(instance_name)
        if self.is_vm_suspended(instance_name):
            instance.resume()
        elif self.is_vm_paused(instance_name):
            instance.unpause()
        else:
            instance.start()
        wait_for(lambda: self.is_vm_running(instance_name), message="start %s" % instance_name)
        return True

    def stop_vm(self, instance_name):
        self.logger.info(" Stopping OpenStack instance %s" % instance_name)
        if self.is_vm_stopped(instance_name):
            return True

        instance = self._find_instance_by_name(instance_name)
        instance.stop()
        wait_for(lambda: self.is_vm_stopped(instance_name), message="stop %s" % instance_name)
        return True

    def create_vm(self):
        raise NotImplementedError('create_vm not implemented.')

    def delete_vm(self, instance_name, delete_fip=True):
        self.logger.info(" Deleting OpenStack instance {}".format(instance_name))
        instance = self._find_instance_by_name(instance_name)
        if delete_fip:
            self.unassign_and_delete_floating_ip(instance)
        else:
            self.unassign_floating_ip(instance)
        self.logger.info(" Deleting OpenStack instance {} in progress now.".format(instance_name))
        instance.delete()
        wait_for(lambda: not self.does_vm_exist(instance_name), timeout='3m', delay=5)
        return True

    def restart_vm(self, instance_name):
        self.logger.info(" Restarting OpenStack instance %s" % instance_name)
        return self.stop_vm(instance_name) and self.start_vm(instance_name)

    def list_vm(self, **kwargs):
        instance_list = self._get_all_instances()
        return [instance.name for instance in instance_list]

    def list_template(self):
        template_list = self.api.images.list()
        return [template.name for template in template_list]

    def list_flavor(self):
        flavor_list = self.api.flavors.list()
        return [flavor.name for flavor in flavor_list]

    def list_volume(self):  # TODO: maybe names? Could not get it to work via API though ...
        volume_list = self.capi.volumes.list()
        return [volume.id for volume in volume_list]

    def list_network(self):
        network_list = self.api.networks.list()
        return [network.label for network in network_list]

    def info(self):
        return '%s %s' % (self.api.client.service_type, self.api.client.version)

    def disconnect(self):
        pass

    def vm_status(self, vm_name):
        """Retrieve Instance status.

        Raises:
            :py:class:`wrapanapi.exceptions.VMError
        """
        inst = self._find_instance_by_name(vm_name)
        if inst.status != "ERROR":
            return inst.status
        if not hasattr(inst, "fault"):
            raise VMError("Instance {} in error state!".format(vm_name))
        raise VMError("Instance {} error {}: {} | {}".format(
            vm_name, inst.fault["code"], inst.fault["message"], inst.fault["created"]))

    def create_volume(self, size_gb, **kwargs):
        volume = self.capi.volumes.create(size_gb, **kwargs).id
        wait_for(lambda: self.capi.volumes.get(volume).status == "available", num_sec=60, delay=0.5)
        return volume

    def delete_volume(self, *ids, **kwargs):
        wait = kwargs.get("wait", True)
        timeout = kwargs.get("timeout", 180)
        for id in ids:
            self.capi.volumes.find(id=id).delete()
        if not wait:
            return
        # Wait for them
        wait_for(
            lambda: all(map(lambda id: not self.volume_exists(id), ids)),
            delay=0.5, num_sec=timeout)

    def volume_exists(self, id):
        try:
            self.capi.volumes.get(id)
            return True
        except cinder_exceptions.NotFound:
            return False

    def get_volume(self, id):
        return self.capi.volumes.get(id)

    @contextmanager
    def with_volume(self, *args, **kwargs):
        """Creates a context manager that creates a single volume with parameters defined via params
        and destroys it after exiting the context manager

        For arguments description, see the :py:meth:`OpenstackSystem.create_volume`.
        """
        volume = self.create_volume(*args, **kwargs)
        try:
            yield volume
        finally:
            self.delete_volume(volume)

    @contextmanager
    def with_volumes(self, *configurations, **kwargs):
        """Similar to :py:meth:`OpenstackSystem.with_volume`, but with multiple volumes.

        Args:
            *configurations: Can be either :py:class:`int` (taken as a disk size), or a tuple.
                If it is a tuple, then first element is disk size and second element a dictionary
                of kwargs passed to :py:meth:`OpenstackSystem.create_volume`. Can be 1-n tuple, it
                can cope with that.
        Keywords:
            n: How many copies of single configuration produce? Useful when you want to create eg.
                10 identical volumes, so you specify only one configuration and set n=10.

        Example:

            .. code-block:: python

               with mgmt.with_volumes(1, n=10) as (d0, d1, d2, d3, d4, d5, d6, d7, d8, d9):
                   pass  # provisions 10 identical 1G volumes

               with mgmt.with_volumes(1, 2) as (d0, d1):
                   pass  # d0 1G, d1 2G

               with mgmt.with_volumes((1, {}), (2, {})) as (d0, d1):
                   pass  # d0 1G, d1 2G same as before but you can see you can pass kwargs through

        """
        n = kwargs.pop("n", None)
        if n is None:
            pass  # Nothing to do
        elif n > 1 and len(configurations) == 1:
            configurations = n * configurations
        elif n != len(configurations):
            raise "n does not equal the length of configurations"
        # now n == len(configurations)
        volumes = []
        try:
            for configuration in configurations:
                if isinstance(configuration, int):
                    size, kwargs = configuration, {}
                elif len(configuration) == 1:
                    size, kwargs = configuration[0], {}
                elif len(configuration) == 2:
                    size, kwargs = configuration
                else:
                    size = configuration[0]
                    kwargs = configuration[1]
                volumes.append(self.create_volume(size, **kwargs))
            yield volumes
        finally:
            self.delete_volume(*volumes)

    def _get_instance_name(self, id):
        return self.api.servers.get(id).name

    def volume_attachments(self, volume_id):
        """Returns a dictionary of ``{instance: device}`` relationship of the volume."""
        volume = self.capi.volumes.get(volume_id)
        result = {}
        for attachment in volume.attachments:
            result[self._get_instance_name(attachment["server_id"])] = attachment["device"]
        return result

    def vm_creation_time(self, vm_name):
        instance = self._find_instance_by_name(vm_name)
        # Example vm.created: 2014-08-14T23:29:30Z
        creation_time = datetime.strptime(instance.created, '%Y-%m-%dT%H:%M:%SZ')
        # create time is UTC, localize it, strip tzinfo
        return creation_time.replace(tzinfo=pytz.UTC)

    def is_vm_running(self, vm_name):
        return self.vm_status(vm_name) in self.states['running']

    def is_vm_stopped(self, vm_name):
        return self.vm_status(vm_name) in self.states['stopped']

    def is_vm_suspended(self, vm_name):
        return self.vm_status(vm_name) in self.states['suspended']

    def is_vm_paused(self, vm_name):
        return self.vm_status(vm_name) in self.states['paused']

    def wait_vm_running(self, vm_name, num_sec=360):
        self.logger.info(" Waiting for OS instance %s to change status to ACTIVE" % vm_name)
        wait_for(self.is_vm_running, [vm_name], num_sec=num_sec)

    def wait_vm_stopped(self, vm_name, num_sec=360):
        self.logger.info(" Waiting for OS instance %s to change status to SHUTOFF" % vm_name)
        wait_for(self.is_vm_stopped, [vm_name], num_sec=num_sec)

    def wait_vm_suspended(self, vm_name, num_sec=720):
        self.logger.info(" Waiting for OS instance %s to change status to SUSPENDED" % vm_name)
        wait_for(self.is_vm_suspended, [vm_name], num_sec=num_sec)

    def wait_vm_paused(self, vm_name, num_sec=720):
        self.logger.info(" Waiting for OS instance %s to change status to PAUSED" % vm_name)
        wait_for(self.is_vm_paused, [vm_name], num_sec=num_sec)

    def suspend_vm(self, instance_name):
        self.logger.info(" Suspending OpenStack instance %s" % instance_name)
        if self.is_vm_suspended(instance_name):
            return True

        instance = self._find_instance_by_name(instance_name)
        instance.suspend()
        wait_for(lambda: self.is_vm_suspended(instance_name), message="suspend %s" % instance_name)

    def pause_vm(self, instance_name):
        self.logger.info(" Pausing OpenStack instance %s" % instance_name)
        if self.is_vm_paused(instance_name):
            return True

        instance = self._find_instance_by_name(instance_name)
        instance.pause()
        wait_for(lambda: self.is_vm_paused(instance_name), message="pause %s" % instance_name)

    def clone_vm(self, source_name, vm_name):
        raise NotImplementedError('clone_vm not implemented.')

    def free_fips(self, pool):
        """Returns list of free floating IPs sorted by ip address."""
        return sorted(self.api.floating_ips.findall(fixed_ip=None, pool=pool), key=lambda ip: ip.ip)

    def deploy_template(self, template, *args, **kwargs):
        """ Deploys an OpenStack instance from a template.

        For all available args, see ``create`` method found here:
        http://docs.openstack.org/developer/python-novaclient/ref/v1_1/servers.html

        Most important args are listed below.

        Args:
            template: The name of the template to use.
            flavour_name: The name of the flavour to use.
            flavour_id: UUID of the flavour to use.
            vm_name: A name to use for the vm.
            network_name: The name of the network if it is a multi network setup (Havanna).
            ram: Override flavour RAM (creates a new flavour if none suitable found)
            cpu: Override flavour VCPU (creates a new flavour if none suitable found)

        Note:
            If assign_floating_ip kwarg is present, then :py:meth:`OpenstackSystem.create_vm` will
            attempt to register a floating IP address from the pool specified in the arg.

            When overriding the ram and cpu, you have to pass a flavour anyway. When a new flavour
            is created from the ram/cpu, other values are taken from that given flavour.
        """
        power_on = kwargs.pop("power_on", True)
        nics = []
        timeout = kwargs.pop('timeout', 900)

        if 'flavour_name' in kwargs:
            flavour = self.api.flavors.find(name=kwargs['flavour_name'])
        elif 'instance_type' in kwargs:
            flavour = self.api.flavors.find(name=kwargs['instance_type'])
        elif 'flavour_id' in kwargs:
            flavour = self.api.flavors.find(id=kwargs['flavour_id'])
        else:
            flavour = self.api.flavors.find(name='m1.tiny')
        ram = kwargs.pop('ram', None)
        cpu = kwargs.pop('cpu', None)
        if ram or cpu:
            # Find or create a new flavour usable for provisioning
            # Keep the parameters from the original flavour
            self.logger.info(
                'RAM/CPU override of flavour %s: RAM %r MB, CPU: %r cores', flavour.name, ram, cpu)
            ram = ram or flavour.ram
            cpu = cpu or flavour.vcpus
            disk = flavour.disk
            ephemeral = flavour.ephemeral
            swap = flavour.swap
            rxtx_factor = flavour.rxtx_factor
            is_public = flavour.is_public
            try:
                new_flavour = self.api.flavors.find(
                    ram=ram, vcpus=cpu,
                    disk=disk, ephemeral=ephemeral, swap=swap,
                    rxtx_factor=rxtx_factor, is_public=is_public)
            except os_exceptions.NotFound:
                # The requested flavor was not found, create a custom one
                self.logger.info('No suitable flavour found, creating a new one.')
                base_flavour_name = '{}-{}M-{}C'.format(flavour.name, ram, cpu)
                flavour_name = base_flavour_name
                counter = 0
                new_flavour = None
                if not swap:
                    # Protect against swap empty string
                    swap = 0
                while new_flavour is None:
                    try:
                        new_flavour = self.api.flavors.create(
                            name=flavour_name,
                            ram=ram, vcpus=cpu,
                            disk=disk, ephemeral=ephemeral, swap=swap,
                            rxtx_factor=rxtx_factor, is_public=is_public)
                    except os_exceptions.Conflict:
                        self.logger.info(
                            'Name %s is already taken, changing the name', flavour_name)
                        counter += 1
                        flavour_name = base_flavour_name + '_{}'.format(counter)
                    else:
                        self.logger.info(
                            'Created a flavour %r with id %r', new_flavour.name, new_flavour.id)
                        flavour = new_flavour
            else:
                self.logger.info('Found a flavour %s', new_flavour.name)
                flavour = new_flavour

        if 'vm_name' not in kwargs:
            vm_name = 'new_instance_name'
        else:
            vm_name = kwargs['vm_name']
        self.logger.info(" Deploying OpenStack template %s to instance %s (%s)" % (
            template, kwargs["vm_name"], flavour.name))
        if len(self.list_network()) > 1:
            if 'network_name' not in kwargs:
                raise NetworkNameNotFound('Must select a network name')
            else:
                net_id = self.api.networks.find(label=kwargs['network_name']).id
                nics = [{'net-id': net_id}]

        image = self.api.images.find(name=template)
        instance = self.api.servers.create(vm_name, image, flavour, nics=nics, *args, **kwargs)
        self.wait_vm_running(vm_name, num_sec=timeout)
        if kwargs.get('floating_ip_pool', None):
            self.assign_floating_ip(instance, kwargs['floating_ip_pool'])

        if power_on:
            self.start_vm(vm_name)

        return vm_name

    def assign_floating_ip(self, instance_or_name, floating_ip_pool, safety_timer=5):
        """Assigns a floating IP to an instance.

        Args:
            instance_or_name: Name of the instance or instance object itself.
            floating_ip_pool: Name of the floating IP pool to take from.
            safety_timer: A timeout after assigning the FIP that is used to detect whether another
                external influence did not steal our FIP. Default is 5.

        Returns:
            The public FIP. Raises an exception in case of error.
        """
        instance = self._instance_or_name(instance_or_name)

        current_ip = self.current_ip_address(instance.name)
        if current_ip is not None:
            return current_ip

        # Why while? Well, this code can cause one peculiarity. Race condition can "steal" a FIP
        # so this will loop until it really get the address. A small timeout is added to ensure
        # the instance really got that address and other process did not steal it.
        # TODO: Introduce neutron client and its create+assign?
        while self.current_ip_address(instance.name) is None:
            free_ips = self.free_fips(floating_ip_pool)
            # We maintain 1 floating IP as a protection against race condition
            # I know it is bad practice, but I did not figure out how to prevent the race
            # condition by openstack saying "Hey, this IP is already assigned somewhere"
            if len(free_ips) > 1:
                # There are 2 and more ips, so we will take the first one (eldest)
                ip = free_ips[0]
                self.logger.info("Reusing {} from pool {}".format(ip.ip, floating_ip_pool))
            else:
                # There is one or none, so create one.
                try:
                    ip = self.api.floating_ips.create(floating_ip_pool)
                except (os_exceptions.ClientException, os_exceptions.OverLimit) as e:
                    self.logger.error("Probably no more FIP slots available: {}".format(str(e)))
                    free_ips = self.free_fips(floating_ip_pool)
                    # So, try picking one from the list (there still might be one)
                    if free_ips:
                        # There is something free. Slight risk of race condition
                        ip = free_ips[0]
                        self.logger.info(
                            "Reused {} from pool {} because no more free spaces for new ips"
                            .format(ip.ip, floating_ip_pool))
                    else:
                        # Nothing can be done
                        raise NoMoreFloatingIPs("Provider {} ran out of FIPs".format(self.auth_url))
                self.logger.info("Created {} in pool {}".format(ip.ip, floating_ip_pool))
            instance.add_floating_ip(ip)

            # Now the grace period in which a FIP theft could happen
            time.sleep(safety_timer)

        self.logger.info("Instance {} got a floating IP {}".format(instance.name, ip.ip))
        return self.current_ip_address(instance.name)

    def unassign_floating_ip(self, instance_or_name):
        """Disassociates the floating IP (if present) from VM.

        Args:
            instance_or_name: Name of the instance or instance object itself.

        Returns:
            None if no FIP was dissociated. Otherwise it will return the Floating IP object.
        """
        instance = self._instance_or_name(instance_or_name)
        ip_addr = self.current_ip_address(instance.name)
        if ip_addr is None:
            return None
        floating_ips = self.api.floating_ips.findall(ip=ip_addr)
        if not floating_ips:
            return None
        floating_ip = floating_ips[0]
        self.logger.info(
            'Detaching floating IP {}/{} from {}'.format(
                floating_ip.id, floating_ip.ip, instance.name))
        instance.remove_floating_ip(floating_ip)
        wait_for(
            lambda: self.current_ip_address(instance.name) is None, delay=1, timeout='1m')
        return floating_ip

    def delete_floating_ip(self, floating_ip):
        """Deletes an existing FIP.

        Args:
            floating_ip: FloatingIP object or an IP address of the FIP.

        Returns:
            True if it deleted a FIP, False if it did not delete it, most probably because it
            does not exist.
        """
        if floating_ip is None:
            # To be able to chain with unassign_floating_ip, which can return None
            return False
        if not isinstance(floating_ip, FloatingIP):
            floating_ip = self.api.floating_ips.findall(ip=floating_ip)
            if not floating_ip:
                return False
            floating_ip = floating_ip[0]
        self.logger.info('Deleting floating IP {}/{}'.format(floating_ip.id, floating_ip.ip))
        floating_ip.delete()
        wait_for(
            lambda: len(self.api.floating_ips.findall(ip=floating_ip.ip)) == 0,
            delay=1, timeout='1m')
        return True

    def unassign_and_delete_floating_ip(self, instance_or_name):
        """Disassociates the floating IP (if present) from VM and deletes it.

        Args:
            instance_or_name: Name of the instance or instance object itself.

        Returns:
            True if it deleted a FIP, False if it did not delete it, most probably because it
            does not exist.
        """
        return self.delete_floating_ip(self.unassign_floating_ip(instance_or_name))

    def _get_instance_networks(self, name):
        instance = self._find_instance_by_name(name)
        return instance._info['addresses']

    def current_ip_address(self, name):
        networks = self._get_instance_networks(name)
        for network_nics in networks.itervalues():
            for nic in network_nics:
                if nic['OS-EXT-IPS:type'] == 'floating':
                    return str(nic['addr'])

    def all_vms(self):
        result = []
        for vm in self._get_all_instances():
            ip = None
            for network_nics in vm._info["addresses"].itervalues():
                for nic in network_nics:
                    if nic['OS-EXT-IPS:type'] == 'floating':
                        ip = str(nic['addr'])
            result.append(VMInfo(
                vm.id,
                vm.name,
                vm.status,
                ip,
            ))
        return result

    def get_vm_name_from_ip(self, ip):
        # unfortunately it appears you cannot query for ip address from the sdk,
        #   unlike curling rest api which does work
        """ Gets the name of a vm from its IP.

        Args:
            ip: The ip address of the vm.
        Returns: The vm name for the corresponding IP."""

        instances = self._get_all_instances()

        for instance in instances:
            addr = self.get_ip_address(instance.name)
            if addr is not None and ip in addr:
                return str(instance.name)
        raise VMNotFoundViaIP('The requested IP is not known as a VM')

    def get_ip_address(self, name, **kwargs):
        return self.current_ip_address(name)

    def _generic_paginator(self, f):
        """A generic paginator for OpenStack services

        Takes a callable and recursively runs the "listing" until no more are returned
        by sending the ```marker``` kwarg to offset the search results. We try to rollback
        up to 10 times in the markers in case one was deleted. If we can't rollback after
        10 times, we give up.
        Possible improvement is to roll back in 5s or 10s, but then we have to check for
        uniqueness and do dup removals.
        """
        lists = []
        marker = None
        while True:
            if not lists:
                temp_list = f()
            else:
                for i in range(min(10, len(lists))):
                    list_offset = -(i + 1)
                    marker = lists[list_offset].id
                    try:
                        temp_list = f(marker=marker)
                        break
                    except os_exceptions.BadRequest:
                        continue
                else:
                    raise Exception("Could not get list, maybe mass deletion after 10 marker tries")
            if temp_list:
                lists.extend(temp_list)
            else:
                break
        return lists

    def _get_all_instances(self, filter_tenants=True):
        call = partial(self.api.servers.list, True, {'all_tenants': True})
        instances = self._generic_paginator(call)
        if filter_tenants:
            # Filter instances based on their tenant ID
            # needed for CFME 5.3 and higher
            tenants = self._get_tenants()
            ids = [tenant.id for tenant in tenants]
            instances = filter(lambda i: i.tenant_id in ids, instances)
        return instances

    def _get_all_templates(self):
        return self.api.images.list()

    def _find_instance_by_name(self, name):
        """
        OpenStack Nova Client does have a find method, but it doesn't
        allow the find method to be used on other tenants. The list()
        method is the only one that allows an all_tenants=True keyword
        """
        instances = self._get_all_instances()
        for instance in instances:
            if instance.name == name:
                return instance
        else:
            raise VMInstanceNotFound(name)

    def _instance_or_name(self, instance_or_name):
        """Works similarly to _find_instance_by_name but allows passing the constructed object."""
        if isinstance(instance_or_name, Server):
            # Object passed
            return instance_or_name
        else:
            # String passed
            return self._find_instance_by_name(instance_or_name)

    def _find_template_by_name(self, name):
        templates = self._get_all_templates()
        for template in templates:
            if template.name == name:
                return template
        else:
            raise VMInstanceNotFound("template {}".format(name))

    def get_template_id(self, name):
        return self._find_template_by_name(name).id

    def does_vm_exist(self, name):
        try:
            self._find_instance_by_name(name)
            return True
        except Exception:
            return False

    def remove_host_from_cluster(self, hostname):
        raise NotImplementedError('remove_host_from_cluster not implemented')

    def get_first_floating_ip(self):
        try:
            self.api.floating_ips.create()
        except os_exceptions.NotFound:
            self.logger.error('No more Floating IPs available, will attempt to grab a free one')
        try:
            first_available_ip = (ip for ip in self.api.floating_ips.list()
                                  if ip.instance_id is None).next()
        except StopIteration:
            return None
        return first_available_ip.ip

    def mark_as_template(self, instance_name, **kwargs):
        """OpenStack marking as template is a little bit more complex than vSphere.

        We have to rename the instance, create a snapshot of the original name and then delete the
        instance."""
        self.logger.info("Marking {} as OpenStack template".format(instance_name))
        instance = self._find_instance_by_name(instance_name)
        original_name = instance.name
        copy_name = original_name + "_copytemplate"
        instance.update(copy_name)
        try:
            self.wait_vm_steady(copy_name)
            if not self.is_vm_stopped(copy_name):
                instance.stop()
                self.wait_vm_stopped(copy_name)
            uuid = instance.create_image(original_name)
            wait_for(lambda: self.api.images.get(uuid).status == "ACTIVE", num_sec=900, delay=5)
            instance.delete()
            wait_for(lambda: not self.does_vm_exist(copy_name), num_sec=180, delay=5)
        except Exception as e:
            self.logger.error(
                "Could not mark {} as a OpenStack template! ({})".format(instance_name, str(e)))
            instance.update(original_name)  # Clean up after ourselves
            raise

    def rename_vm(self, instance_name, new_name):
        instance = self._find_instance_by_name(instance_name)
        try:
            instance.update(new_name)
        except Exception as e:
            self.logger.exception(e)
            return instance_name
        else:
            return new_name

    def delete_template(self, template_name):
        template = self._find_template_by_name(template_name)
        template.delete()
        wait_for(lambda: not self.does_template_exist(template_name), num_sec=120, delay=10)

    def stack_exist(self, stack_name):
        stack = self.stackapi.stacks.get(stack_name)
        if stack:
            return True
        else:
            return False

    def delete_stack(self, stack_name):
        """Deletes stack

        Args:
        stack_name: Unique name of stack
        """

        self.logger.info(" Terminating RHOS stack %s" % stack_name)
        try:
            self.stackapi.stacks.delete(stack_name)
            return True
        except ActionTimedOutError:
            return False

    def set_meta_value(self, instance, key, value):
        instance = self._instance_or_name(instance)
        instance.manager.set_meta_item(
            instance, key, value if isinstance(value, basestring) else json.dumps(value))

    def get_meta_value(self, instance, key):
        instance = self._instance_or_name(instance)
        try:
            data = instance.metadata[key]
            try:
                return json.loads(data)
            except ValueError:
                # Support metadata set by others
                return data
        except KeyError:
            raise KeyError('Metadata {} not found in {}'.format(key, instance.name))

    def vm_hardware_configuration(self, vm_name):
        vm = self._find_instance_by_name(vm_name)
        flavor_id = vm.flavor['id']
        flavor = self.api.flavors.find(id=flavor_id)
        return {'ram': flavor.ram, 'cpu': flavor.vcpus}

    def usage_and_quota(self):
        data = self.api.limits.get().to_dict()['absolute']
        host_cpus = 0
        host_ram = 0
        for hypervisor in self.api.hypervisors.list():
            host_cpus += hypervisor.vcpus
            host_ram += hypervisor.memory_mb
        # -1 == no limit
        return {
            # RAM
            'ram_used': data['totalRAMUsed'],
            'ram_total': host_ram,
            'ram_limit': data['maxTotalRAMSize'] if data['maxTotalRAMSize'] >= 0 else None,
            # CPU
            'cpu_used': data['totalCoresUsed'],
            'cpu_total': host_cpus,
            'cpu_limit': data['maxTotalCores'] if data['maxTotalCores'] >= 0 else None,
        }
