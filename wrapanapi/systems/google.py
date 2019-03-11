# coding: utf-8
"""
Defines System and Entity classes related to the Google Cloud platform
"""
from __future__ import absolute_import

import os
import random
import time

from oauth2client.service_account import ServiceAccountCredentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from googleapiclient import errors
from json import dumps as json_dumps

import httplib2
import iso8601
import pytz
from wait_for import wait_for

from wrapanapi.entities import (Instance, Template, TemplateMixin, VmMixin,
                                VmState)
from wrapanapi.exceptions import (ImageNotFoundError, MultipleInstancesError,
                                  NotFoundError, VMInstanceNotFound)
from wrapanapi.systems.base import System

# Retry transport and file IO errors.
RETRYABLE_ERRORS = (httplib2.HttpLib2Error, IOError)
# Number of times to retry failed downloads.
NUM_RETRIES = 5
# Number of bytes to send/receive in each request.
CHUNKSIZE = 2 * 1024 * 1024
# Mimetype to use if one can't be guessed from the file extension.
DEFAULT_MIMETYPE = 'application/octet-stream'

# List of image projects which gce provided from the box. Could be extend in the future and
# will have impact on total number of templates/images
IMAGE_PROJECTS = ['centos-cloud', 'debian-cloud', 'rhel-cloud', 'suse-cloud', 'ubuntu-os-cloud',
                  'windows-cloud', 'opensuse-cloud', 'coreos-cloud', 'google-containers']


class GoogleCloudInstance(Instance):
    state_map = {
        'PROVISIONING': VmState.STARTING,
        'STAGING': VmState.STARTING,
        'STOPPING': VmState.STOPPING,
        'RUNNING': VmState.RUNNING,
        'TERMINATED': VmState.STOPPED,
    }

    def __init__(self, system, raw=None, **kwargs):
        """
        Constructor for GoogleCloudInstance

        Args:
            system: GoogleCloudSystem object
            raw: the raw json data for this instance returned by the compute API
            name: the name of the VM
            zone: the zone of the VM
        """
        self._name = raw['name'] if raw else kwargs.get('name')
        self._zone = raw['zone'].split('/')[-1] if raw else kwargs.get('zone')
        if not self._name or not self._zone:
            raise ValueError("missing required kwargs: 'name' and 'zone'")

        super(GoogleCloudInstance, self).__init__(system, raw, **kwargs)

        self._project = self.system._project
        self._api = self.system._compute.instances()

    @property
    def _identifying_attrs(self):
        return {'name': self._name, 'zone': self._zone, 'project': self._project}

    @property
    def uuid(self):
        return self.raw['id']

    @property
    def name(self):
        return self._name

    @property
    def zone(self):
        return self._zone

    def refresh(self):
        try:
            self.raw = self._api.get(
                project=self._project, zone=self._zone, instance=self._name).execute()
        except errors.HttpError as error:
            if error.resp.status == 404:
                raise VMInstanceNotFound(self._name)
            else:
                raise
        return self.raw

    def _get_state(self):
        self.refresh()
        return self._api_state_to_vmstate(self.raw['status'])

    @property
    def ip(self):
        self.refresh()
        return self.raw.get('networkInterfaces')[0].get('networkIP')

    @property
    def ip_external(self):
        self.refresh()
        access_configs = self.raw.get('networkInterfaces')[0].get('accessConfigs')[0]
        return access_configs.get('natIP')

    @property
    def type(self):
        if self.raw.get('machineType', None):
            return self.raw['machineType'].split('/')[-1]
        return None

    @property
    def creation_time(self):
        self.refresh()
        creation_time = iso8601.parse_date(self.raw['creationTimestamp'])
        return creation_time.astimezone(pytz.UTC)

    def delete(self, timeout=360):
        self.logger.info("Deleting Google Cloud instance '%s'", self.name)
        operation = self._api.delete(
            project=self._project, zone=self.zone, instance=self.name).execute()

        wait_for(
            lambda: self.system.is_zone_operation_done(operation['name']), delay=0.5,
            num_sec=timeout, message="Delete {}".format(self.name)
        )

        self.logger.info(
            "DELETE request successful, waiting for instance '%s' to be removed...",
            self.name
        )
        wait_for(
            lambda: not self.exists, delay=0.5, num_sec=timeout,
            message=" instance '{}' to not exist".format(self.name)
        )
        return True

    def cleanup(self):
        return self.delete()

    def restart(self):
        self.logger.info("Restarting Google Cloud instance '%s'", self.name)
        # Use self.stop/self.start vs reset since it's easier to block
        # (the VM stays in RUNNING state when using reset)
        return self.stop() and self.start()

    def stop(self):
        self.logger.info("Stopping Google Cloud instance '%s'", self.name)
        operation = self._api.stop(
            project=self._project, zone=self.zone, instance=self.name).execute()
        wait_for(
            lambda: self.system.is_zone_operation_done(operation['name']),
            message="stop operation done {}".format(self.name), timeout=360
        )
        self.wait_for_state(VmState.STOPPED)
        return True

    def start(self):
        self.logger.info("Starting Google Cloud instance '%s'", self.name)
        operation = self._api.start(
            project=self._project, zone=self.zone, instance=self.name).execute()
        wait_for(
            lambda: self.system.is_zone_operation_done(operation['name']),
            message="start operation done {}".format(self.name)
        )
        self.wait_for_state(VmState.RUNNING)
        return True

    def attach_disk(self, disk_name, zone=None, project=None):
        """Attach disk to instance."""
        if not zone:
            zone = self._zone
        if not project:
            project = self._project

        # Attach disk
        disk_source = "/compute/v1/projects/{}/zones/{}/disks/{}".format(project, zone, disk_name)
        attach_data = {'source': disk_source}
        req = self._api.attachDisk(
            project=project, zone=zone, instance=self.name, body=attach_data)
        operation = req.execute()
        wait_for(lambda: self.system.is_zone_operation_done(operation['name']), delay=0.5,
            num_sec=120, message=" Attach {}".format(disk_name))

        # Get device name of this new disk
        self.refresh()
        device_name = None
        for disk in self.raw['disks']:
            if disk['source'].endswith(disk_source):
                device_name = disk['deviceName']

        self.logger.info('"Instance disks: %s', self.raw['disks'])
        if not device_name:
            raise Exception("Unable to find deviceName for attached disk.")

        # Mark disk for auto-delete
        req = self._api.setDiskAutoDelete(
            project=project, zone=zone, instance=self.name,
            deviceName=device_name, autoDelete=True
        )
        operation = req.execute()
        wait_for(lambda: self.system.is_zone_operation_done(operation['name']), delay=0.5,
            num_sec=120, message=" Set auto-delete {}".format(disk_name))


class GoogleCloudImage(Template):
    def __init__(self, system, raw=None, **kwargs):
        """
        Constructor for GoogleCloudImage

        Args:
            system: GoogleCloudSystem object
            raw: the raw json data for this image returned by the compute API
            name: name of image
            project: project image is located in
        """
        self._name = raw['name'] if raw else kwargs.get('name')
        self._project = kwargs.get('project') or self.system._project
        if not self._name or not self._project:
            raise ValueError("missing required kwargs: 'name' and 'project'")

        super(GoogleCloudImage, self).__init__(system, raw, **kwargs)

        self._api = self.system._compute.images()
        self._instances_api = self.system._compute.instances()

    @property
    def _identifying_attrs(self):
        return {'name': self._name, 'project': self._project}

    @property
    def uuid(self):
        return self.raw['id']

    @property
    def name(self):
        return self._name

    @property
    def project(self):
        return self._project

    def refresh(self):
        try:
            self.raw = self._api.get(project=self._project, image=self._name).execute()
        except errors.HttpError as error:
            if error.resp.status == 404:
                raise ImageNotFoundError(self._name)
            else:
                raise
        return self.raw

    def delete(self, timeout=360):
        if self._project in IMAGE_PROJECTS:
            raise ValueError('Public images cannot be deleted')

        operation = self._api.delete(project=self._project, image=self.name).execute()
        wait_for(
            lambda: self.system.is_global_operation_done(operation['name']), delay=0.5,
            num_sec=timeout, message=" Deleting image {}".format(self.name)
        )
        wait_for(
            lambda: not self.exists, delay=0.5, num_sec=timeout,
            message=" image '{}' to not exist".format(self.name)
        )
        return True

    def cleanup(self):
        return self.delete()

    def deploy(self, vm_name, zone=None, machine_type=None, ssh_key=None,
               startup_script="#!/bin/bash", timeout=180, **kwargs):
        """
        Depoy an instance from this template

        Args:
            zone -- zone to create VM in, defaults to default zone for associated GoogleCloudSystem
            machine_type -- machine type for VM, defaults to 'n1-standard-1'
            ssh_key -- (optional) ssh public key string
            startup_script -- (optional) text of start-up script, defaults to empty bash script
            timeout -- timeout for deploy operation to complete, defaults to 180sec
        Returns:
            True if operation completes successfully
        """
        if kwargs:
            self.logger.warn("deploy() ignored kwargs: %s", kwargs)

        template_link = self.raw['selfLink']

        instance_name = vm_name
        if not zone:
            zone = self.system._zone
        if not machine_type:
            machine_type = 'n1-standard-1'

        full_machine_type = 'zones/{}/machineTypes/{}'.format(zone, machine_type)

        self.logger.info("Creating instance '%s'", instance_name)

        config = {
            'name': instance_name,
            'machineType': full_machine_type,

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
                    'value': startup_script
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

        if ssh_key:
            ssh_keys = {
                'key': 'ssh-keys',
                'value': ssh_key
            }
            config['metadata']['items'].append(ssh_keys)

        operation = self._instances_api.insert(
            project=self._project, zone=zone, body=config).execute()
        wait_for(
            lambda: self.system.is_zone_operation_done(operation['name']), delay=0.5,
            num_sec=timeout, message=" Create {}".format(instance_name)
        )
        instance = GoogleCloudInstance(system=self.system, name=instance_name, zone=zone)
        wait_for(
            lambda: instance.in_steady_state, timeout=timeout,
            delay=0.5, message="Instance {} to reach steady state".format(instance_name)
        )
        return instance


class GoogleCloudSystem(System, TemplateMixin, VmMixin):
    """
    Client to Google Cloud Platform API

    """
    _stats_available = {
        'num_vm': lambda self: len(self.list_vms()),
        'num_template': lambda self: len(self.list_templates()),
    }

    can_suspend = False
    can_pause = False

    default_scope = ['https://www.googleapis.com/auth/cloud-platform']

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
        super(GoogleCloudSystem, self).__init__(**kwargs)
        self._project = project
        self._zone = zone
        self._region = kwargs.get('region')
        scope = kwargs.get('scope', self.default_scope)

        if 'service_account' in kwargs:
            service_account = dict(kwargs.get('service_account').items())
            service_account['private_key'] = service_account['private_key'].replace('\\n', '\n')
            service_account['type'] = service_account.get('type', 'service_account')  # default it
            credentials = ServiceAccountCredentials.from_json_keyfile_dict(service_account,
                                                                           scopes=scope)
        elif file_type == 'json':
            file_path = kwargs.get('file_path', None)
            credentials = ServiceAccountCredentials.from_json_keyfile_name(file_path,
                                                                           scopes=scope)
        elif file_type == 'p12':
            file_path = kwargs.get('file_path', None)
            client_email = kwargs.get('client_email', None)
            credentials = ServiceAccountCredentials.from_p12_keyfile(client_email,
                                                                     file_path,
                                                                     scopes=scope)
        http_auth = credentials.authorize(httplib2.Http())
        self._compute = build('compute', 'v1', http=http_auth)
        self._storage = build('storage', 'v1', http=http_auth)
        self._instances = self._compute.instances()
        self._forwarding_rules = self._compute.forwardingRules()
        self._buckets = self._storage.buckets()

    @property
    def _identifying_attrs(self):
        return {'project': self._project, 'zone': self._zone, 'region': self._region}

    @property
    def can_suspend(self):
        return False

    @property
    def can_pause(self):
        return False

    def _get_all_buckets(self):
        return self._buckets.list(project=self._project).execute()

    def _get_all_forwarding_rules(self):
        results = []
        results.extend(self._forwarding_rules.list(project=self._project, region=self._zone).
                       execute().get('items', []))
        return results

    def info(self):
        return "{}: project={}, zone={}".format(self.__class__.__name__, self._project, self._zone)

    def disconnect(self):
        """
        Disconnect from the GCE

        GCE service is stateless, so there's nothing to disconnect from
        """
        pass

    def list_vms(self, zones=None):
        """
        List all VMs in the GCE account, filtered by zone if desired

        Args:
            zone: List of zones, by default uses the zone set by this system's zone kwarg
                  (i.e. self._zone)

        Returns:
            List of GCEInstance objects
        """
        results = []
        if not zones:
            zones = [self._zone]

        for zone_name in zones:
            zone_instances = self._instances.list(
                project=self._project, zone=zone_name).execute()
            for instance in zone_instances.get('items', []):
                results.append(
                    GoogleCloudInstance(
                        system=self, raw=instance, name=instance['name'], zone=zone_name
                    )
                )

        return results

    def find_vms(self, name, zones=None):
        """
        Find VMs with a given name, filtered by zones if desired

        Args:
            zones: List of zone names, by default does not filter to any zone
        Returns:
            List of GCEInstance objects that match
        """
        results = []
        if not zones:
            zones = [
                zone['name'].split('/')[-1] for zone  # convert url-based name
                in self._compute.zones().list(project=self._project).execute().get('items', [])
            ]

        for zone_name in zones:
            try:
                # Just use get in each zone instead of iterating through all instances
                instance = self._instances.get(
                    project=self._project, zone=zone_name, instance=name).execute()
                results.append(
                    GoogleCloudInstance(
                        system=self, raw=instance, name=instance['name'], zone=zone_name
                    )
                )
            except errors.HttpError as error:
                if error.resp.status == 404:
                    # Getting the instance we found in the list failed, just skip it...
                    pass
                else:
                    raise
        return results

    def get_vm(self, name, zone=None, try_all_zones=False):
        """
        Get a single VM with given name in a specified zone

        By default self._zone is used

        Args:
            name: name of VM
            zone: zone to get VM from, defaults to self._zone
            try_all_zones: if VM not found in 'zone', then continue to look for it
                in all other zones
        Returns:
            GCEInstance object
        Raises:
            VMInstanceNotFound if unable to find vm
            MultipleInstancesError if multiple vm's with the same name found
        """
        if not zone:
            zone = self._zone
        instances = self.find_vms(name, zones=[zone])
        if not instances and try_all_zones:
            self.logger.info("Looking for instance '%s' in all zones", name)
            instances = self.find_vms(name, zones=None)
        if not instances:
            raise VMInstanceNotFound(name)
        elif len(instances) > 1:
            raise MultipleInstancesError(name)
        return instances[0]

    def create_vm(self):
        raise NotImplementedError

    def list_templates(self, include_public=False):
        images = self._compute.images()
        results = []
        projects = [self._project]
        if include_public:
            projects.extend(IMAGE_PROJECTS)
        for project in projects:
            results.extend(
                GoogleCloudImage(system=self, raw=image, project=project, name=image['name'])
                for image in
                images.list(project=project).execute().get('items', [])
            )
        return results

    def get_template(self, name, project=None):
        if not project:
            project = self._project
        try:
            image = self._compute.images().get(project=project, image=name).execute()
            return GoogleCloudImage(system=self, raw=image, project=project, name=name)
        except errors.HttpError as error:
            if error.resp.status == 404:
                raise ImageNotFoundError("'{}' not found in project '{}'".format(name, project))
            else:
                raise

    def find_templates(self, name, include_public=False):
        """
        Find templates with 'name' in any project
        """
        # TODO: Possibly expand this to truly "find" something using regex, filters, etc.
        return [
            image for image in self.list_templates(include_public=include_public)
            if image.name == name
        ]

    def create_template(self, name, bucket_url, timeout=360):
        """
        Create image from file

        Args:
            name: Unique name for new GoogleCloudImage
            bucket_url: url to image file in bucket
            timeout: time to wait for operation
        """
        images = self._compute.images()
        data = {
            "name": name,
            "rawDisk": {"source": bucket_url}
        }
        operation = images.insert(project=self._project, body=data).execute()
        wait_for(
            lambda: self.is_global_operation_done(operation['name']), delay=0.5,
            num_sec=timeout, message=" Creating image {}".format(name)
        )
        return self.get_template(name, self._project)

    def create_disk(self, disk_name, size_gb, zone=None, project=None, disk_type="pd-standard"):
        """
        Create a new disk.

        Args:
            disk_name: name of disk
            size_gb: int for size in GB
            zone: zone to create disk in, default is self._zone
            project: project to create disk in, default is self._project
            disk_type: e.g. pd-ssd, pd-standard -- default is pd-standard
        """
        if not zone:
            zone = self._zone
        if not project:
            project = self._project
        disk_data = {
            'sizeGb': size_gb,
            'type': "zones/{}/diskTypes/{}".format(zone, disk_type),
            'name': disk_name
        }
        req = self._compute.disks().insert(project=project, zone=zone, body=disk_data)
        operation = req.execute()
        wait_for(lambda: self.is_zone_operation_done(operation['name']), delay=0.5,
            num_sec=120, message=" Create {}".format(disk_name))

    def list_bucket(self):
        buckets = self._get_all_buckets()
        return [bucket.get('name') for bucket in buckets.get('items', [])]

    def list_forwarding_rules(self):
        rules = self._get_all_forwarding_rules()
        return [forwarding_rule.get('name') for forwarding_rule in rules]

    def _find_forwarding_rule_by_name(self, forwarding_rule_name):
        try:
            forwarding_rule = self._forwarding_rules.get(
                project=self._project, zone=self._zone,
                forwardingRule=forwarding_rule_name).execute()
            return forwarding_rule
        except Exception:
            raise NotFoundError

    def _check_operation_result(self, result):
        if result['status'] == 'DONE':
            self.logger.info("The operation '%s' -> DONE", result['name'])
            if 'error' in result:
                self.logger.error("Error during operation '%s'", result['name'])
                self.logger.error("Error details: %s", result['error'])
                raise Exception(result['error'])
            return True
        return False

    def is_global_operation_done(self, operation_name):
        result = self._compute.globalOperations().get(
            project=self._project,
            operation=operation_name).execute()
        self._check_operation_result(result)

    def is_zone_operation_done(self, operation_name, zone=None):
        if not zone:
            zone = self._zone
        result = self._compute.zoneOperations().get(
            project=self._project,
            zone=zone,
            operation=operation_name).execute()
        self._check_operation_result(result)

    def create_bucket(self, bucket_name):
        """ Create bucket
        Args:
            bucket_name: Unique name of bucket
        """
        if not self.bucket_exists(bucket_name):
            self._buckets.insert(
                project=self._project, body={"name": "{}".format(bucket_name)}).execute()
            self.logger.info("Bucket '%s' was created", bucket_name)
        else:
            self.logger.info("Bucket '%s' was not created, exists already", bucket_name)

    def delete_bucket(self, bucket_name):
        """ Delete bucket
        Args:
            bucket_name: Name of bucket
        """
        if self.bucket_exists(bucket_name):
            self._buckets.delete(bucket=bucket_name).execute()
            self.logger.info("Bucket '%s' was deleted", bucket_name)
        else:
            self.logger.info("Bucket '%s' was not deleted, not found", bucket_name)

    def bucket_exists(self, bucket_name):
        try:
            self._buckets.get(bucket=bucket_name).execute()
            return True
        except errors.HttpError as error:
            if "Not Found" in error.content:
                self.logger.info("Bucket '%s' was not found", bucket_name)
                return False
            if "Invalid bucket name" in error.content:
                self.logger.info("Incorrect bucket name '%s' specified", bucket_name)
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
                        "File '%s' was not found in bucket '%s'", file_name, bucket_name)
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
                        "File '%s' was not found in bucket '%s'", bucket_name, file_name)
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
                'Caught exception (%s). Sleeping for %d seconds before retry #%d.',
                str(error), sleeptime, progressless_iters
            )

            time.sleep(sleeptime)

        self.logger.info('Building upload request...')
        media = MediaFileUpload(file_path, chunksize=CHUNKSIZE, resumable=True)
        if not media.mimetype():
            media = MediaFileUpload(file_path, DEFAULT_MIMETYPE, resumable=True)

        blob_name = os.path.basename(file_path)
        if not self.bucket_exists(bucket_name):
            self.logger.error("Bucket '%s' doesn't exist", bucket_name)
            raise NotFoundError("bucket {}".format(bucket_name))

        request = self._storage.objects().insert(
            bucket=bucket_name, name=blob_name, media_body=media)
        self.logger.info(
            'Uploading file: %s, to bucket: %s, blob: %s',
            file_path, bucket_name, blob_name
        )

        progressless_iters = 0
        response = None
        while response is None:
            error = None
            try:
                progress, response = request.next_chunk()
                if progress:
                    self.logger.info('Upload progress: %d%%', 100 * progress.progress())
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

    def does_forwarding_rule_exist(self, forwarding_rule_name):
        try:
            self._find_forwarding_rule_by_name(forwarding_rule_name)
            return True
        except errors.HttpError as error:
            if error.resp.status == 404:
                return False
            else:
                raise

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
        raise NotImplementedError('list_security_group not implemented.')
