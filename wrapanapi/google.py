# coding: utf-8
"""Backend management system classes

Used to communicate with providers without using CFME facilities
"""
from __future__ import absolute_import
import os
import random
import time
from apiclient.discovery import build
from apiclient.http import MediaFileUpload
from apiclient import errors
from json import dumps as json_dumps

import httplib2
import iso8601
import pytz
from oauth2client.service_account import ServiceAccountCredentials
from wait_for import wait_for

from .base import WrapanapiAPIBaseVM, VMInfo
from .exceptions import VMInstanceNotFound, ImageNotFoundError, ActionNotSupported, \
    ForwardingRuleNotFound

# Retry transport and file IO errors.
RETRYABLE_ERRORS = (httplib2.HttpLib2Error, IOError)
# Number of times to retry failed downloads.
NUM_RETRIES = 5
# Number of bytes to send/receive in each request.
CHUNKSIZE = 2 * 1024 * 1024
# Mimetype to use if one can't be guessed from the file extension.
DEFAULT_MIMETYPE = 'application/octet-stream'

# List of image project which gcr provided from the box. Could be extend in the futute and
# will have impact on total number of templates/images
IMAGE_PROJECTS = ['centos-cloud', 'debian-cloud', 'rhel-cloud', 'suse-cloud', 'ubuntu-os-cloud',
                'windows-cloud', 'opensuse-cloud', 'coreos-cloud', 'google-containers']


class GoogleCloudSystem(WrapanapiAPIBaseVM):
    """
    Client to Google Cloud Platform API

    """
    # gcloud technically does support suspend but the methods are not implemented below
    # for now, 'can_suspend' will be false until those methods are implemented.
    can_suspend = False
    can_pause = False

    _stats_available = {
        'num_vm': lambda self: len(self.all_vms()),
        'num_template': lambda self: len(self.list_image()),
    }

    default_scope = ['https://www.googleapis.com/auth/cloud-platform']
    states = {
        'running': ('RUNNING',),
        'stopped': ('TERMINATED',),
        'starting': ('STAGING'),
        'stopping': ('STOPPING'),
    }

    def __init__(self, project=None, zone=None, file_type=None, **kwargs):
        """
            The last three argumets are optional and required only if you want
            to use json or p12 files.
            By default, we expecting that service_account arg contains service account data.

            Args:
                project: name of the project, so called project_id
                zone: zone of cloud
                service_account: service_account_content

                scope: compute engine, container engine, sqlservice end etc
                file_path: path to json or p12 file
                file_type: p12 or json
                client_email: Require for p12 file

            Returns: A :py:class:`GoogleCloudSystem` object.
        """
        super(GoogleCloudSystem, self).__init__(kwargs)
        self._project = project
        self._zone = zone
        self._region = kwargs.get('region')
        scope = kwargs.get('scope', self.default_scope)

        service_account = kwargs.get('service_account', None)
        if service_account:
            service_account = dict(service_account.items())
            service_account['private_key'] = service_account['private_key'].replace('\\n', '\n')
            credentials = ServiceAccountCredentials.from_json_keyfile_dict(
                service_account, scopes=scope)
        elif file_type == 'json':
            file_path = kwargs.get('file_path', None)
            credentials = ServiceAccountCredentials.from_json_keyfile_name(
                file_path, scopes=scope)
        elif file_type == 'p12':
            file_path = kwargs.get('file_path', None)
            client_email = kwargs.get('client_email', None)
            credentials = ServiceAccountCredentials.from_p12_keyfile(
                client_email, file_path, scopes=scope)
        http_auth = credentials.authorize(httplib2.Http())
        self._compute = build('compute', 'v1', http=http_auth)
        self._storage = build('storage', 'v1', http=http_auth)
        self._instances = self._compute.instances()
        self._forwarding_rules = self._compute.forwardingRules()
        self._buckets = self._storage.buckets()

    def _get_zone_instances(self, zone):
        return self._instances.list(project=self._project, zone=zone).execute()

    def _get_all_buckets(self):
        return self._buckets.list(project=self._project).execute()

    def _get_all_forwarding_rules(self):
        results = []
        results.extend(self._forwarding_rules.list(project=self._project, region=self._zone).
                       execute().get('items', []))
        return results

    def _get_all_images(self):
        images = self._compute.images()
        result = []
        for image_project in IMAGE_PROJECTS:
            result.extend(images.list(project=image_project).execute().get('items', []))
        result.extend(images.list(project=self._project).execute().get('items', []))
        return result

    def get_private_images(self):
        images = self._compute.images()
        return images.list(project=self._project).execute()

    def list_vm(self):
        """List VMs in this GCE zone - filtered from all_vms

        Returns:
            List of VM names in the object's zone
        """
        instances = self.all_vms(by_zone=True)
        return [instance.name for instance in instances]

    def list_bucket(self):
        buckets = self._get_all_buckets()
        return [bucket.get('name') for bucket in buckets.get('items', [])]

    def list_forwarding_rules(self):
        rules = self._get_all_forwarding_rules()
        return [forwarding_rule.get('name') for forwarding_rule in rules]

    def list_image(self):
        images = self._get_all_images()
        return [image.get('name') for image in images]

    def _find_instance_by_name(self, instance_name):
        try:
            instance = self._instances.get(
                project=self._project, zone=self._zone, instance=instance_name).execute()
            return instance
        except Exception as e:
            self.logger.error(e)
            self.logger.info("Searching instance {} in all other zones".format(instance_name))
            zones = self._compute.zones().list(project=self._project).execute()
            for zone in zones.get('items', []):
                zone_name = zone.get('name', None)
                for instance in self._get_zone_instances(zone_name).get('items', []):
                    if instance['name'] == instance_name:
                        return instance
            self.logger.error("Instance {} not found in any of the zones".format(instance_name))
            raise VMInstanceNotFound(instance_name)

    def _find_forwarding_rule_by_name(self, forwarding_rule_name):
        try:
            forwarding_rule = self._forwarding_rules.get(
                project=self._project, zone=self._zone,
                forwardingRule=forwarding_rule_name).execute()
            return forwarding_rule
        except Exception:
            raise ForwardingRuleNotFound

    def get_image_by_name(self, image_name):
        try:
            image = self._compute.images().get(project=self._project, image=image_name).execute()
            return image
        except Exception:
            raise ImageNotFoundError(image_name)

    def _nested_operation_wait(self, operation_name, zone=True):
        if not zone:
            result = self._compute.globalOperations().get(
                project=self._project,
                operation=operation_name).execute()
        else:
            result = self._compute.zoneOperations().get(
                project=self._project,
                zone=self._zone,
                operation=operation_name).execute()

        if result['status'] == 'DONE':
            self.logger.info("The operation {} -> DONE".format(operation_name))
            if 'error' in result:
                self.logger.error("Error during {} operation.".format(operation_name))
                self.logger.error("Detailed information about error {}".format(result['error']))
                raise Exception(result['error'])
            return True

        return False

    def create_bucket(self, bucket_name):
        """ Create bucket
        Args:
            bucket_name: Unique name of bucket
        """
        if not self.bucket_exists(bucket_name):
            self._buckets.insert(
                project=self._project, body={"name": "{}".format(bucket_name)}).execute()
            self.logger.info("Bucket {} was created".format(bucket_name))
        else:
            self.logger.info("Bucket {} was not created, exists already".format(bucket_name))

    def create_image(self, image_name, bucket_url, timeout=360):
        """ Create image from file
        Args:
            image_name: Unique name of image
            bucket_url: url to image file in bucket
            timeout: time to wait for operation
        """
        images = self._compute.images()
        data = {
            "name": image_name,
            "rawDisk": {"source": bucket_url}
        }
        operation = images.insert(project=self._project, body=data).execute()
        wait_for(self._nested_operation_wait,
                 func_kwargs={'operation_name': operation['name'], 'zone': False},
                 delay=0.5, num_sec=timeout, message=" Creating image {}".format(image_name))

    def delete_image(self, image_name, timeout=360):
        """ Delete image
        Args:
            image_name: Unique name of image
            timeout: time to wait for operation (default=360)
        """
        operation = self._compute.images().delete(project=self._project, image=image_name).execute()

        wait_for(self._nested_operation_wait,
                 func_kwargs={'operation_name': operation['name'], 'zone': False},
                 delay=0.5, num_sec=timeout, message=" Deleting image {}".format(image_name))

    def delete_bucket(self, bucket_name):
        """ Delete bucket
        Args:
            bucket_name: Name of bucket
        """
        if self.bucket_exists(bucket_name):
            self._buckets.delete(bucket=bucket_name).execute()
            self.logger.info("Bucket {} was deleted".format(bucket_name))
        else:
            self.logger.info("Bucket {} was not deleted, not found".format(bucket_name))

    def bucket_exists(self, bucket_name):
        try:
            self._buckets.get(bucket=bucket_name).execute()
            return True
        except errors.HttpError as error:
            if "Not Found" in error.content:
                self.logger.info("Bucket {} was not found".format(bucket_name))
                return False
            if "Invalid bucket name" in error.content:
                self.logger.info("Incorrect bucket name {} was specified".format(bucket_name))
                return False
            raise error

    def get_file_from_bucket(self, bucket_name, file_name):
        if self.bucket_exists(bucket_name):
            try:
                data = self._storage.objects().get(bucket=bucket_name, object=file_name).execute()
                return data
            except errors.HttpError as error:
                if "Not Found" in error.content:
                    self.logger.info(
                        "File {} was not found in bucket {}".format(file_name, bucket_name))
                else:
                    raise error
        return {}

    def delete_file_from_bucket(self, bucket_name, file_name):
        if self.bucket_exists(bucket_name):
            try:
                data = self._storage.objects().delete(bucket=bucket_name,
                                                      object=file_name).execute()
                return data
            except errors.HttpError as error:
                if "No such object" in error.content:
                    self.logger.info(
                        "File {} was not found in bucket {}".format(file_name, bucket_name))
                else:
                    raise error
        return {}

    def upload_file_to_bucket(self, bucket_name, file_path):
        def handle_progressless_iter(error, progressless_iters):
            if progressless_iters > NUM_RETRIES:
                self.logger.info('Failed to make progress for too many consecutive iterations.')
                raise error

            sleeptime = random.random() * (2 ** progressless_iters)
            self.logger.info(
                'Caught exception ({}). Sleeping for {} seconds before retry #{}.'.format(
                    str(error), sleeptime, progressless_iters))

            time.sleep(sleeptime)

        self.logger.info('Building upload request...')
        media = MediaFileUpload(file_path, chunksize=CHUNKSIZE, resumable=True)
        if not media.mimetype():
            media = MediaFileUpload(file_path, DEFAULT_MIMETYPE, resumable=True)

        blob_name = os.path.basename(file_path)
        if not self.bucket_exists(bucket_name):
            self.logger.error("Bucket {} doesn't exists".format(bucket_name))
            raise "Bucket doesn't exist"

        request = self._storage.objects().insert(
            bucket=bucket_name, name=blob_name, media_body=media)
        self.logger.info('Uploading file: {}, to bucket: {}, blob: {}'.format(
            file_path, bucket_name, blob_name))

        progressless_iters = 0
        response = None
        while response is None:
            error = None
            try:
                progress, response = request.next_chunk()
                if progress:
                    self.logger.info('Upload {}%'.format(100 * progress.progress()))
            except errors.HttpError as error:
                if error.resp.status < 500:
                    raise
            except RETRYABLE_ERRORS as error:
                if error:
                    progressless_iters += 1
                    handle_progressless_iter(error, progressless_iters)
                else:
                    progressless_iters = 0

        self.logger.info('Upload complete!')
        self.logger.info('Uploaded Object:')
        self.logger.info(json_dumps(response, indent=2))
        return (True, blob_name)

    def deploy_template(self, template, **kwargs):

        template_link = self.get_image_by_name(template)['selfLink']

        instance_name = kwargs['vm_name']
        self.logger.info("Creating {} instance".format(instance_name))

        machine_type = kwargs.get('machine_type',
            "zones/{}/machineTypes/n1-standard-1".format(self._zone))
        script = kwargs.get('startup_script_data', "#!/bin/bash")
        timeout = kwargs.get('timeout', 180)

        config = {
            'name': instance_name,
            'machineType': machine_type,

            # Specify the boot disk and the image to use as a source.
            'disks': [
                {
                    'boot': True,
                    'autoDelete': True,
                    'initializeParams': {
                        'sourceImage': template_link,
                    }
                }
            ],

            # Specify a network interface with NAT to access the public
            # internet.
            'networkInterfaces': [{
                'network': 'global/networks/default',
                'accessConfigs': [
                    {'type': 'ONE_TO_ONE_NAT', 'name': 'External NAT'}
                ]
            }],

            # Allow the instance to access cloud storage and logging.
            'serviceAccounts': [{
                'email': 'default',
                'scopes': [
                    'https://www.googleapis.com/auth/devstorage.read_write',
                    'https://www.googleapis.com/auth/logging.write'
                ]
            }],

            # Metadata is readable from the instance and allows you to
            # pass configuration from deployment scripts to instances.
            'metadata': {
                'items': [{
                    # Startup script is automatically executed by the
                    # instance upon startup.
                    'key': 'startup-script',
                    'value': script
                }, {
                    # Every project has a default Cloud Storage bucket that's
                    # the same name as the project.
                    'key': 'bucket',
                    'value': self._project
                }]
            },
            'tags': {
                'items': ['https-server']
            }
        }

        if kwargs.get('ssh_key', None):
            ssh_keys = {
                'key': 'ssh-keys',
                'value': kwargs.get('ssh_key', None)
            }
            config['metadata']['items'].append(ssh_keys)

        operation = self._instances.insert(
            project=self._project, zone=self._zone, body=config).execute()
        wait_for(lambda: self._nested_operation_wait(operation['name']), delay=0.5,
            num_sec=timeout, message=" Create {}".format(instance_name))
        return True

    def create_vm(self):
        raise NotImplementedError('create_vm not implemented.')

    def delete_vm(self, instance_name, timeout=250):
        if not self.does_vm_exist(instance_name):
            self.logger.info("The {} instance is not exists, skipping".format(instance_name))
            return True

        self.logger.info("Deleting Google Cloud instance {}".format(instance_name))
        operation = self._instances.delete(
            project=self._project, zone=self._zone, instance=instance_name).execute()
        wait_for(lambda: self._nested_operation_wait(operation['name']), delay=0.5,
            num_sec=timeout, message="Delete {}".format(instance_name))
        return True

    def restart_vm(self, instance_name):
        self.logger.info("Restarting Google Cloud instance {}".format(instance_name))
        operation = self._instances.reset(
            project=self._project, zone=self._zone, instance=instance_name).execute()
        wait_for(lambda: self._nested_operation_wait(operation['name']),
            message="Restart {}".format(instance_name))
        return True

    def stop_vm(self, instance_name):
        if self.is_vm_stopped(instance_name) or not self.does_vm_exist(instance_name):
            self.logger.info("The {} instance is already stopped or doesn't exist, skip termination"
               .format(instance_name))
            return True

        self.logger.info("Stoping Google Cloud instance {}".format(instance_name))
        operation = self._instances.stop(
            project=self._project, zone=self._zone, instance=instance_name).execute()
        wait_for(lambda: self._nested_operation_wait(operation['name']),
            message="Stop {}".format(instance_name), timeout=360)
        return True

    def start_vm(self, instance_name):
        # This method starts an instance that was stopped using the using the
        # instances().stop method.
        if self.is_vm_running(instance_name) or not self.does_vm_exist(instance_name):
            self.logger.info("The {} instance is already running or doesn't exists, skip starting"
               .format(instance_name))
            return True

        self.logger.info("Starting Google Cloud instance {}".format(instance_name))
        operation = self._instances.start(
            project=self._project, zone=self._zone, instance=instance_name).execute()
        wait_for(lambda: self._nested_operation_wait(operation['name']),
            message="Start {}".format(instance_name))
        return True

    def clone_vm(self, source_name, vm_name):
        raise NotImplementedError('clone_vm not implemented.')

    # Get external IP (ephemeral)
    def current_ip_address(self, vm_name):
        zones = self._compute.zones().list(project=self._project).execute()
        for zone in zones.get('items', []):
            zone_name = zone.get('name', None)
            for vm in self._get_zone_instances(zone_name).get('items', []):
                if vm['name'] == vm_name:
                    access_configs = vm.get('networkInterfaces')[0].get('accessConfigs')[0]
                    return access_configs.get('natIP')

    def disconnect(self):
        """Disconnect from the GCE

        GCE service is stateless, so there's nothing to disconnect from
        """
        pass

    def does_vm_exist(self, instance_name):
        try:
            self._find_instance_by_name(instance_name)
            return True
        except Exception:
            return False

    def does_forwarding_rule_exist(self, forwarding_rule_name):
        try:
            self._find_forwarding_rule_by_name(forwarding_rule_name)
            return True
        except Exception:
            return False

    def get_ip_address(self, vm_name):
        return self.current_ip_address(vm_name)

    def info(self):
        raise NotImplementedError('info not implemented.')

    def is_vm_running(self, vm_name):
        return self.vm_status(vm_name) in self.states['running']

    def is_vm_stopped(self, vm_name):
        return self.vm_status(vm_name) in self.states['stopped']

    def is_vm_suspended(self, vm_name):
        raise ActionNotSupported('vm_suspend not supported.')

    # These methods indicate if the vm is in the process of stopping or starting
    def is_vm_stopping(self, vm_name):
        return self.vm_status(vm_name) in self.states['stopping']

    def is_vm_starting(self, vm_name):
        return self.vm_status(vm_name) in self.states['starting']

    def list_flavor(self):
        raise NotImplementedError('list_flavor not implemented.')

    def list_template(self):
        return self.list_image()

    def remove_host_from_cluster(self, hostname):
        raise NotImplementedError('remove_host_from_cluster not implemented.')

    def suspend_vm(self, vm_name):
        raise ActionNotSupported('vm_suspend not supported.')

    def vm_status(self, vm_name):
        if self.does_vm_exist(vm_name):
            return self._find_instance_by_name(vm_name)['status']
        return None

    def wait_vm_running(self, vm_name, num_sec=360):
        self.logger.info("Waiting for instance {} to change status to ACTIVE".format(vm_name))
        wait_for(self.is_vm_running, [vm_name], num_sec=num_sec)

    def wait_vm_stopped(self, vm_name, num_sec=360):
        self.logger.info("Waiting for instance {} to change status to TERMINATED".format(vm_name))
        wait_for(self.is_vm_stopped, [vm_name], num_sec=num_sec)

    def wait_vm_suspended(self, vm_name, num_sec):
        raise ActionNotSupported('vm_suspend not supported.')

    def all_vms(self, by_zone=False):
        """List all VMs in the GCE account, unfiltered by default

        Args:
            by_zone: boolean, True to filter by the current object's zone

        Returns:
            List of VMInfo nametuples
        """
        result = []
        zones = self._compute.zones().list(project=self._project).execute()
        for zone in zones.get('items', []):
            zone_name = zone.get('name', None)
            if by_zone and zone_name != self._zone:
                continue
            for vm in self._get_zone_instances(zone_name).get('items', []):
                if vm['id'] and vm['name'] and vm['status'] and vm.get('networkInterfaces'):

                    result.append(VMInfo(
                        vm['id'],
                        vm['name'],
                        vm['status'],
                        vm.get('networkInterfaces')[0].get('networkIP'),
                    ))
        else:
            self.logger.info('No matching zone found in all_vms with by_zone=True')
        return result

    def vm_creation_time(self, instance_name):
        instance = self._find_instance_by_name(instance_name)
        vm_time_stamp = instance['creationTimestamp']
        creation_time = (iso8601.parse_date(vm_time_stamp))
        return creation_time.astimezone(pytz.UTC)

    def vm_type(self, instance_name):
        instance = self._find_instance_by_name(instance_name)
        if instance.get('machineType', None):
            return instance['machineType'].split('/')[-1]

    def list_network(self):
        self.logger.info("Attempting to List GCE Virtual Private Networks")
        networks = self._compute.networks().list(project=self._project).execute()['items']

        return [net['name'] for net in networks]

    def list_subnet(self):
        self.logger.info("Attempting to List GCE Subnets")
        networks = self._compute.networks().list(project=self._project).execute()['items']
        subnetworks = [net['subnetworks'] for net in networks]
        subnets_names = []

        # Subnetworks is a bi dimensional array, containing urls of subnets.
        # The only way to have the subnet name is to take the last part of the url.
        # self._compute.subnetworks().list() returns just the subnets of the given region,
        # and CFME displays networks with subnets from all regions.
        for urls in subnetworks:
            for url in urls:
                subnets_names.append(url.split('/')[-1])

        return subnets_names

    def list_load_balancer(self):
        self.logger.info("Attempting to List GCE loadbalancers")
        # The result here is different of what is displayed in CFME, because in CFME the
        # forwarding rules are displayed instead of loadbalancers, and the regions are neglected.
        # see: https://bugzilla.redhat.com/show_bug.cgi?id=1547465
        # https://bugzilla.redhat.com/show_bug.cgi?id=1433062
        load_balancers = self._compute.targetPools().list(project=self._project,
                                                          region=self._region).execute()['items']
        return [lb['name'] for lb in load_balancers]

    def list_router(self):
        self.logger.info("Attempting to List GCE routers")
        # routers are not shown on CFME
        # https://bugzilla.redhat.com/show_bug.cgi?id=1543938
        routers = self._compute.routers().list(project=self._project,
                                               region=self._region).execute()['items']
        return [router['name'] for router in routers]

    def list_security_group(self):
        raise NotImplementedError('start_vm not implemented.')
