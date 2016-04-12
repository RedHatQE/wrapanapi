# coding: utf-8
"""Backend management system classes

Used to communicate with providers without using CFME facilities
"""

from base import MgmtSystemAPIBase, VMInfo
from exceptions import VMInstanceNotFound
from apiclient.discovery import build
from httplib2 import Http
from oauth2client.client import flow_from_clientsecrets
from oauth2client.file import Storage
from oauth2client.service_account import ServiceAccountCredentials
from oauth2client.tools import run_flow, argparser
from wait_for import wait_for


class GoogleCloudSystem (MgmtSystemAPIBase):
    """
    Client to Google Cloud Platform API

    """
    default_scope = ['https://www.googleapis.com/auth/compute']
    states = {
        'running': ('RUNNING',),
        'stopped': ('TERMINATED',),
        'starting': ('STAGING'),
        'stopping': ('STOPPING'),
    }

    def __init__(self, project=None, zone=None, scope=None, creds=None,
                 file_path=None, file_type=None, client_email=None, **kwargs):
        """
            The last three argumets are optional and required only if you want
            to use json or p12 files.
            By default, we expecting that creds arg contains service account data.

            Args:
                project: name of the project, so called project_id
                zone: zone of cloud
                scope: compute engine, container engine, sqlservice end etc
                creds: service_account_content

                file_path: path to json or p12 file
                file_type: p12 or json
                client_email: Require for p12 file

            Returns: A :py:class:`GoogleCloudSystem` object.
        """
        super(GoogleCloudSystem, self).__init__(kwargs)
        self._project = project
        self._zone = zone
        if scope is None:
            scope = self.default_scope

        if creds:
            credentials = ServiceAccountCredentials.from_json_keyfile_dict(creds, scopes=scope)
        elif file_type == 'json':
            credentials = ServiceAccountCredentials.from_json_keyfile_name(
                file_path, scopes=scope)
        elif file_type == 'p12':
            credentials = ServiceAccountCredentials.from_p12_keyfile(
                client_email, file_path, scopes=scope)
        http_auth = credentials.authorize(Http())
        self._compute = build('compute', 'v1', http=http_auth)
        self._instances = self._compute.instances()

    def oauth2(self, project=None, zone=None, scope=None, oauth2_storage=None,
            client_secrets=None):

        self._project = project
        self._zone = zone

        # Perform OAuth 2.0 authorization.
        # based on OAuth 2.0 client IDs credentials from client_secretes file
        if client_secrets and scope and oauth2_storage:
            flow = flow_from_clientsecrets(client_secrets, scope=scope)
            storage = Storage(oauth2_storage)
            self._credentials = storage.get()

        if self._credentials is None or self._credentials.invalid:
            self._credentials = run_flow(flow, storage, argparser.parse_args([]))

        if self._credentials is None or self._credentials.invalid:
            raise Exception("Incorrect credentials for Google Cloud System")

        self._compute = build('compute', 'v1', credentials=self._credentials)
        self._instances = self._compute.instances()

    def _get_all_instances(self):
        return self._instances.list(project=self._project, zone=self._zone).execute()

    def list_vm(self):
        instance_list = self._get_all_instances()
        return [instance.get('name') for instance in instance_list.get('items', [])]

    def _find_instance_by_name(self, instance_name):
        try:
            instance = self._instances.get(
                project=self._project, zone=self._zone, instance=instance_name).execute()
            return instance
        except Exception:
            raise VMInstanceNotFound(instance_name)

    def _nested_wait_vm_running(self, operation_name):
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

    def create_vm(self, instance_name='test_instance', source_disk_image=None, machine_type=None,
            startup_script_data=None, timeout=180):
        if self.does_vm_exist(instance_name):
            self.logger.info("The {} instance is already exists, skipping".format(instance_name))
            return True

        self.logger.info("Creating {} instance".format(instance_name))

        if not source_disk_image:
            source_disk_image = "projects/debian-cloud/global/images/debian-7-wheezy-v20150320"

        machine_type = machine_type or ("zones/{}/machineTypes/n1-standard-1".format(self._zone))

        script = startup_script_data or "#!/bin/bash"

        config = {
            'name': instance_name,
            'machineType': machine_type,

            # Specify the boot disk and the image to use as a source.
            'disks': [
                {
                    'boot': True,
                    'autoDelete': True,
                    'initializeParams': {
                        'sourceImage': source_disk_image,
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
            }
        }

        operation = self._instances.insert(
            project=self._project, zone=self._zone, body=config).execute()
        wait_for(lambda: self._nested_wait_vm_running(operation['name']), delay=0.5,
            num_sec=timeout, message=" Create {}".format(instance_name))
        return True

    def delete_vm(self, instance_name, timeout=180):
        if not self.does_vm_exist(instance_name):
            self.logger.info("The {} instance is not exists, skipping".format(instance_name))
            return True

        self.logger.info("Deleting Google Cloud instance {}".format(instance_name))
        operation = self._instances.delete(
            project=self._project, zone=self._zone, instance=instance_name).execute()
        wait_for(lambda: self._nested_wait_vm_running(operation['name']), delay=0.5,
            num_sec=timeout, message="Delete {}".format(instance_name))
        return True

    def restart_vm(self, instance_name):
        self.logger.info("Restarting Google Cloud instance {}".format(instance_name))
        operation = self._instances.reset(
            project=self._project, zone=self._zone, instance=instance_name).execute()
        wait_for(lambda: self._nested_wait_vm_running(operation['name']),
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
        wait_for(lambda: self._nested_wait_vm_running(operation['name']),
            message="Stop {}".format(instance_name))
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
        wait_for(lambda: self._nested_wait_vm_running(operation['name']),
            message="Start {}".format(instance_name))
        return True

    def clone_vm(self, source_name, vm_name):
        raise NotImplementedError('clone_vm not implemented.')

    # Get external IP (ephemeral)
    def current_ip_address(self, vm_name):
        return self.vm_status(vm_name)['natIP']

    def deploy_template(self, template, *args, **kwargs):
        raise NotImplementedError('deploy_template not implemented.')

    def disconnect(self):
        raise NotImplementedError('disconnect not implemented.')

    def does_vm_exist(self, instance_name):
        try:
            self._find_instance_by_name(instance_name)
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
        raise NotImplementedError('is_vm_suspended not implemented.')

    # These methods indicate if the vm is in the process of stopping or starting
    def is_vm_stopping(self, vm_name):
        return self.vm_status(vm_name) in self.states['stopping']

    def is_vm_starting(self, vm_name):
        return self.vm_status(vm_name) in self.states['starting']

    def list_flavor(self):
        raise NotImplementedError('list_flavor not implemented.')

    def list_template(self):
        raise NotImplementedError('list_template not implemented.')

    def remove_host_from_cluster(self, hostname):
        raise NotImplementedError('remove_host_from_cluster not implemented.')

    def suspend_vm(self, vm_name):
        raise NotImplementedError('suspend_vm not implemented.')

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
        raise NotImplementedError('wait_vm_suspended not implemented.')

    def all_vms(self):
        result = []
        for vm in self._get_all_instances().get('items', []):
            if (vm['id'] and vm['name'] and vm['status'] and vm.get('networkInterfaces')):

                result.append(VMInfo(
                    vm['id'],
                    vm['name'],
                    vm['status'],
                    vm.get('networkInterfaces')[0].get('networkIP'),
                ))
        return result
