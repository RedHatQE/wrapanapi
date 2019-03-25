# coding: utf-8
from __future__ import absolute_import

import base64
import os
import re
from datetime import datetime

import pytz
from boto import UserAgent
from boto.ec2 import EC2Connection, elb, get_region
from boto.ec2.elb import ELBConnection
from boto.exception import BotoServerError
from botocore.config import Config
from botocore.exceptions import ClientError
from boto3 import (
    resource as boto3resource,
    client as boto3client
)

from wrapanapi.entities import (Instance, Stack, StackMixin, Template,
                                TemplateMixin, VmMixin, VmState)
from wrapanapi.exceptions import (ActionTimedOutError, ImageNotFoundError,
                                  MultipleImagesError, MultipleInstancesError,
                                  MultipleItemsError, NotFoundError,
                                  VMInstanceNotFound)
from wrapanapi.systems.base import System


def _regions(regionmodule, regionname):
    for region in regionmodule.regions():
        if region.name == regionname:
            return region
    return None


class _TagMixin(object):
    def set_tag(self, key, value):
        self.system.api.create_tags([self.uuid], tags={key: value})

    def get_tag_value(self, key):
        self.refresh()
        return self.raw.tags.get(key)

    def unset_tag(self, key, value):
        self.system.api.delete_tags([self.uuid], tags={key: value})


class EC2Instance(Instance, _TagMixin):
    state_map = {
        'pending': VmState.STARTING,
        'stopping': VmState.STOPPING,
        'shutting-down': VmState.STOPPING,
        'running': VmState.RUNNING,
        'stopped': VmState.STOPPED,
        'terminated': VmState.DELETED
    }

    def __init__(self, system, raw=None, **kwargs):
        """
        Constructor for an EC2Instance tied to a specific system.

        Args:
            system: an EC2System object
            raw: the boto.ec2.instance.Instance object if already obtained, or None
            uuid: unique ID of instance
        """

        self._uuid = raw.id if raw else kwargs.get('uuid')
        if not self._uuid:
            raise ValueError("missing required kwarg: 'uuid'")

        super(EC2Instance, self).__init__(system, raw, **kwargs)

        self._api = self.system.api

    @property
    def _identifying_attrs(self):
        return {'uuid': self._uuid}

    @property
    def name(self):
        return getattr(self.raw, 'name', None) or self.raw.tags.get('Name', self.raw.id)

    @property
    def uuid(self):
        return self._uuid

    def refresh(self):
        self.raw = self.system.get_vm(self._uuid, hide_deleted=False).raw
        return self.raw

    def _get_state(self):
        self.refresh()
        return self._api_state_to_vmstate(self.raw.state)

    @property
    def ip(self):
        self.refresh()
        return self.raw.ip_address

    @property
    def type(self):
        return self.raw.instance_type

    @property
    def creation_time(self):
        self.refresh()
        # Example instance.launch_time: 2014-08-13T22:09:40.000Z
        launch_time = datetime.strptime(self.raw.launch_time, '%Y-%m-%dT%H:%M:%S.%fZ')
        # use replace here to make tz-aware. python doesn't handle single 'Z' as UTC
        return launch_time.replace(tzinfo=pytz.UTC)

    def rename(self, new_name):
        self.logger.info("setting name of EC2 instance %s to %s", self.uuid, new_name)
        self.raw.add_tag('Name', new_name)
        self.refresh()  # update raw
        return new_name

    def delete(self, timeout=240):
        """
        Delete instance. Wait for it to move to 'deleted' state

        Returns:
            True if successful
            False if otherwise, or action timed out
        """
        self.logger.info("terminating EC2 instance '%s'", self.uuid)
        try:
            self.raw.terminate()
            self.wait_for_state(VmState.DELETED, timeout=timeout)
            return True
        except ActionTimedOutError:
            return False

    def cleanup(self):
        return self.delete()

    def start(self, timeout=240):
        """
        Start instance. Wait for it to move to 'running' state

        Returns:
            True if successful
            False if otherwise, or action timed out
        """
        self.logger.info("starting EC2 instance '%s'", self.uuid)
        try:
            self.raw.start()
            self.wait_for_state(VmState.RUNNING, timeout=timeout)
            return True
        except ActionTimedOutError:
            return False

    def stop(self, timeout=360):
        """
        Stop instance. Wait for it to move to 'stopped' state

        Returns:
            True if successful
            False if otherwise, or action timed out
        """
        self.logger.info("stopping EC2 instance '%s'", self.uuid)
        try:
            self.raw.stop()
            self.wait_for_state(VmState.STOPPED, timeout=timeout)
            return True
        except ActionTimedOutError:
            return False

    def restart(self):
        """
        Restart instance

        The action is taken in two separate calls to EC2. A 'False' return can
        indicate a failure of either the stop action or the start action.

        Note: There is a reboot_instances call available on the API, but it provides
            less insight than blocking on stop_vm and start_vm. Furthermore,
            there is no "rebooting" state, so there are potential monitoring
            issues that are avoided by completing these steps atomically

        Returns:
            True if stop and start succeeded
            False if otherwise, or action timed out
        """
        self.logger.info("restarting EC2 instance '%s'", self.uuid)
        stopped = self.stop()
        if not stopped:
            self.logger.error("Stopping instance '%s' failed or timed out", self.uuid)
        started = self.start()
        if not started:
            self.logger.error("Starting instance '%s' failed or timed out", self.uuid)
        return stopped and started


class StackStates(object):
    ACTIVE = ['CREATE_COMPLETE', 'ROLLBACK_COMPLETE', 'CREATE_FAILED',
              'UPDATE_ROLLBACK_COMPLETE'],
    COMPLETE = ['CREATE_COMPLETE', 'UPDATE_ROLLBACK_COMPLETE'],
    FAILED = ['ROLLBACK_COMPLETE', 'CREATE_FAILED', 'ROLLBACK_FAILED', 'DELETE_FAILED',
              'UPDATE_ROLLBACK_FAILED'],
    DELETED = ['DELETE_COMPLETE'],
    IN_PROGRESS = ['CREATE_IN_PROGRESS', 'ROLLBACK_IN_PROGRESS', 'DELETE_IN_PROGRESS',
                   'UPDATE_IN_PROGRESS', 'UPDATE_ROLLBACK_COMPLETE_CLEANUP_IN_PROGRESS',
                   'UPDATE_ROLLBACK_IN_PROGRESS', 'UPDATE_COMPLETE_CLEANUP_IN_PROGRESS',
                   'REVIEW_IN_PROGRESS']
    ALL = ['CREATE_IN_PROGRESS', 'CREATE_FAILED', 'CREATE_COMPLETE', 'ROLLBACK_IN_PROGRESS',
           'ROLLBACK_FAILED', 'ROLLBACK_COMPLETE', 'DELETE_IN_PROGRESS', 'DELETE_FAILED',
           'DELETE_COMPLETE', 'UPDATE_IN_PROGRESS', 'UPDATE_COMPLETE_CLEANUP_IN_PROGRESS',
           'UPDATE_COMPLETE', 'UPDATE_ROLLBACK_IN_PROGRESS', 'UPDATE_ROLLBACK_FAILED',
           'UPDATE_ROLLBACK_COMPLETE_CLEANUP_IN_PROGRESS', 'UPDATE_ROLLBACK_COMPLETE',
           'REVIEW_IN_PROGRESS']


class CloudFormationStack(Stack):
    def __init__(self, system, raw=None, **kwargs):
        """
        Represents a CloudFormation stack

        Args:
            system: instance of EC2System
            raw: raw dict for this stack returned by boto CloudFormation.Client.describe_stacks()
            uuid: the stack ID
        """
        self._uuid = raw['StackId'] if raw else kwargs.get('uuid')
        if not self._uuid:
            raise ValueError("missing required kwarg: 'uuid'")

        super(CloudFormationStack, self).__init__(system, raw, **kwargs)
        self._api = self.system.cloudformation_connection

    @property
    def _identifying_attrs(self):
        return {'uuid': self._uuid}

    @property
    def name(self):
        return self.raw['StackName']

    @property
    def uuid(self):
        return self._uuid

    @property
    def creation_time(self):
        self.refresh()
        return self.raw['CreationTime']

    def get_details(self):
        return self.raw

    def refresh(self):
        """
        Re-pull the data for this stack
        """
        try:
            self.raw = self._api.describe_stacks(StackName=self._uuid)['Stacks'][0]
        except BotoServerError as error:
            if error.status == 404:
                raise NotFoundError('stack {}'.format(self._uuid))
            else:
                raise
        except IndexError:
            raise NotFoundError('stack {}'.format(self._uuid))
        return self.raw

    def delete(self):
        """
        Removes the stack on the provider

        Returns:
            True if delete was successful
            False otherwise
        """
        self.logger.info("terminating EC2 stack '%s', id: '%s'", self.name, self.uuid)
        try:
            self._api.delete_stack(self.uuid)
            return True
        except ActionTimedOutError:
            return False

    def cleanup(self):
        """
        Removes the stack on the provider and any of its associated resources
        """
        return self.delete()


class EC2Image(Template, _TagMixin):
    def __init__(self, system, raw=None, **kwargs):
        """
        Constructor for an EC2Image tied to a specific system.

        Args:
            system: an EC2System object
            raw: the boto.ec2.image.Image object if already obtained, or None
            uuid: unique ID of the image
        """
        self._uuid = raw.id if raw else kwargs.get('uuid')
        if not self._uuid:
            raise ValueError("missing required kwarg: 'uuid'")

        super(EC2Image, self).__init__(system, raw, **kwargs)

        self._api = self.system.api

    @property
    def _identifying_attrs(self):
        return {'uuid': self._uuid}

    @property
    def name(self):
        return self.raw.tags.get('Name') or self.raw.name or self.raw.id

    @property
    def uuid(self):
        return self._uuid

    def refresh(self):
        image = self._api.get_image(self._uuid)
        if not image:
            raise ImageNotFoundError(self._uuid)
        self.raw = image
        return self.raw

    def delete(self):
        """
        Deregister the EC2 image
        """
        return self.raw.deregister()

    def cleanup(self):
        """
        Deregister the EC2 image and delete the snapshot
        """
        return self.raw.deregister(delete_snapshot=True)

    def deploy(self, *args, **kwargs):
        """
        Deploy ec2 instance(s) using this template

        Args/kwargs are passed to EC2System.create_vm(), the image_id arg
        will be this image's ID
        """
        return self.system.create_vm(image_id=self.uuid, *args, **kwargs)


class EC2System(System, VmMixin, TemplateMixin, StackMixin):
    """EC2 Management System, powered by boto

    Wraps the EC2 API

    Instead of username and password, accepts access_key_id and
    secret_access_key, the AWS analogs to those ideas. These are passed, along
    with any kwargs, straight through to boto's EC2 connection factory. This
    allows customization of the EC2 connection, to connect to another region,
    for example.

    For the purposes of the EC2 system, a VM's instance ID is its name because
    EC2 instances don't have to have unique names.

    Args:
        *kwargs: Arguments to connect, usually, username, password, region.
    Returns: A :py:class:`EC2System` object.
    """

    _stats_available = {
        'num_vm': lambda self: len(self.list_vms(hide_deleted=False)),
        'num_template': lambda self: len(self.list_templates()),
    }

    can_suspend = False
    can_pause = False

    def __init__(self, **kwargs):
        super(EC2System, self).__init__(**kwargs)
        self._username = kwargs.get('username')
        self._password = kwargs.get('password')
        connection_config = Config(
            signature_version='s3v4',
            retries=dict(
                max_attempts=10
            )
        )

        self._region_name = kwargs.get('region')
        self._region = get_region(self._region_name)
        self.api = EC2Connection(self._username, self._password, region=self._region)

        self.sqs_connection = boto3client(
            'sqs', aws_access_key_id=self._username, aws_secret_access_key=self._password,
            region_name=self._region_name, config=connection_config
        )

        self.elb_connection = ELBConnection(
            self._username, self._password, region=_regions(
                regionmodule=elb, regionname=self._region_name)
        )

        self.s3_connection = boto3resource(
            's3', aws_access_key_id=self._username, aws_secret_access_key=self._password,
            region_name=self._region_name, config=connection_config
        )

        self.ec2_connection = boto3client(
            'ec2', aws_access_key_id=self._username, aws_secret_access_key=self._password,
            region_name=self._region_name, config=connection_config
        )

        self.ecr_connection = boto3client(
            'ecr', aws_access_key_id=self._username, aws_secret_access_key=self._password,
            region_name=self._region_name, config=connection_config
        )

        self.cloudformation_connection = boto3client(
            'cloudformation', aws_access_key_id=self._username,
            aws_secret_access_key=self._password, region_name=self._region_name,
            config=connection_config
        )

        self.sns_connection = boto3client('sns', region_name=self._region_name)

        self.kwargs = kwargs

    @property
    def _identifying_attrs(self):
        return {
            'username': self._username, 'password': self._password, 'region': self._region_name
        }

    @property
    def can_suspend(self):
        return False

    @property
    def can_pause(self):
        return False

    def disconnect(self):
        """Disconnect from the EC2 API -- NOOP

        AWS EC2 service is stateless, so there's nothing to disconnect from
        """
        pass

    def info(self):
        """Returns the current versions of boto and the EC2 API being used"""
        return '%s %s' % (UserAgent, self.api.APIVersion)

    def _get_instances(self, **kwargs):
        """
        Gets instance reservations and parses instance objects
        """
        reservations = self.api.get_all_instances(**kwargs)
        instances = list()
        for reservation in reservations:
            for instance in reservation.instances:
                instances.append(
                    EC2Instance(system=self, raw=instance)
                )
        return instances

    @staticmethod
    def _add_filter_for_terminated(kwargs_dict):
        new_filter = {
            'instance-state-name': [
                api_state for api_state, vm_state in EC2Instance.state_map.items()
                if vm_state is not VmState.DELETED
            ]
        }
        if 'filters' not in kwargs_dict:
            kwargs_dict['filters'] = new_filter
        else:
            kwargs_dict['filters'].update(new_filter)
        return kwargs_dict

    def find_vms(self, name=None, id=None, filters=None, hide_deleted=True):
        """
        Find instance on ec2 system

        Supported queries include searching by name tag, id, or passing
        in a specific filters dict to the system API. You can only
        select one of these methods.

        Args:
            name (str): name of instance (which is a tag)
            id (str): id of instance
            filters (dict): filters to pass along to system.api.get_all_instances()
            hide_deleted: do not list an instance if it has been terminated

        Returns:
            List of EC2Instance objects that match
        """
        # Validate args
        filled_args = [arg for arg in (name, id, filters,) if arg]
        if not filled_args or len(filled_args) > 1:
            raise ValueError(
                "You must select one of these search methods: name, id, or filters")

        if id:
            kwargs = {'instance_ids': [id]}
        elif filters:
            kwargs = {'filters': filters}
        elif name:
            # Quick validation that the instance name isn't actually an ID
            pattern = re.compile(r'^i-\w{8,17}$')
            if pattern.match(name):
                # Switch to using the id search method
                kwargs = {'instance_ids': [name]}
            else:
                kwargs = {'filters': {'tag:Name': name}}

        if hide_deleted:
            self._add_filter_for_terminated(kwargs)

        instances = self._get_instances(**kwargs)

        return instances

    def get_vm(self, name, hide_deleted=True):
        """
        Get a single EC2Instance with name or id equal to 'name'

        Must be a unique name

        Args:
            name: name or id of instance
        Returns:
            EC2Instance object
        Raises:
            VMInstanceNotFound if no instance exists with this name/id
            MultipleInstancesError if name is not unique
        """
        instances = self.find_vms(name=name, hide_deleted=hide_deleted)
        if not instances:
            raise VMInstanceNotFound(name)
        elif len(instances) > 1:
            raise MultipleInstancesError('Instance name "%s" is not unique' % name)
        return instances[0]

    def list_vms(self, hide_deleted=True):
        """
        Returns a list of instances currently active on EC2 (not terminated)
        """
        kwargs = {}
        if hide_deleted:
            self._add_filter_for_terminated(kwargs)
        return [inst for inst in self._get_instances(**kwargs)]

    def create_vm(self, image_id, min_count=1, max_count=1, instance_type='t1.micro',
                  vm_name='', **kwargs):
        """
        Creates aws instances.

        TODO:
            Check whether instances were really created.
            Add additional arguments to be able to modify settings for instance creation.
        Args:
            image_id: ID of AMI
            min_count: Minimal count of instances - useful only if creating thousand of instances
            max_count: Maximal count of instances - defaults to 1
            instance_type: Type of instances, catalog of instance types is here:
                https://aws.amazon.com/ec2/instance-types/
                Defaults to 't1.micro' which is the least expensive instance type

            vm_name: Name of instances, can be blank

        Returns:
            List of EC2Instance objects for all instances created
        """
        self.logger.debug("ec2.create_vm() -- Ignored kwargs: %s", kwargs)
        self.logger.info("Creating instances[%d] with name %s,type %s and image ID: %s ",
                         max_count, vm_name, instance_type, image_id)
        try:
            result = self.ec2_connection.run_instances(
                ImageId=image_id, MinCount=min_count,
                MaxCount=max_count, InstanceType=instance_type, TagSpecifications=[
                    {
                        'ResourceType': 'instance',
                        'Tags': [
                            {
                                'Key': 'Name',
                                'Value': vm_name,
                            },
                        ]
                    },
                ]
            )
        except Exception:
            self.logger.exception("Create of instance '%s' failed.", vm_name)
            raise

        try:
            instances_json = result['Instances']
            instance_ids = [entry['InstanceId'] for entry in instances_json]
        except KeyError:
            self.logger.exception("Unable to parse all InstanceId's from response json")
            raise

        instances = [EC2Instance(system=self, uuid=uuid) for uuid in instance_ids]
        for instance in instances:
            self.logger.info(
                "Waiting for instance '%s' to reach steady state", instance.uuid)
            instance.wait_for_steady_state()
        if len(instances) == 1:
            return instances[0]
        else:
            return instances

    def list_stacks(self, stack_status_filter=StackStates.ACTIVE):
        """
        Returns a list of Stack objects

        stack_status_filter:  list of stack statuses to filter for. See ``StackStates``
        """
        stack_list = [
            CloudFormationStack(system=self, uuid=stack_summary['StackId'])
            for stack_summary in self.cloudformation_connection.list_stacks()['StackSummaries']
            if stack_summary['StackStatus'] in stack_status_filter
        ]
        return stack_list

    def find_stacks(self, name=None, id=None):
        """
        Return list of all stacks with given name or id

        According to boto3 docs, you can use name or ID in these situations:

        "Running stacks: You can specify either the stack's name or its unique stack ID.
        Deleted stacks: You must specify the unique stack ID."

        If 'name' kwarg is given and we fail to locate the stack initially, we will retry with
        'list_stacks' to get the list of all stacks with this name (even if they are deleted)

        If 'id' kwarg is given and we hit an error finding it, we don't call list_stacks. This
        is the more efficient kwarg to use if you are searching specifically by id.

        Args:
            name: name to search for
            id: id to search for
        Returns:
            List of CloudFormationStack objects
        """
        if not name and not id:
            raise ValueError('missing one of required kwargs: name, id')

        if name:
            searching_by_name = True
            name_or_id = name
        elif id:
            searching_by_name = False
            name_or_id = id

        stack_list = []
        try:
            # Try to find by name/id directly by using describe_stacks
            stack_list = [
                CloudFormationStack(system=self, uuid=stack['StackId'], raw=stack)
                for stack
                in self.cloudformation_connection.describe_stacks(StackName=name_or_id)['Stacks']
            ]
        except ClientError as error:
            # Stack not found, if searching by name, look through deleted stacks...
            if searching_by_name and 'Stack with id {} does not exist'.format(name) in str(error):
                stack_list = [
                    CloudFormationStack(system=self, uuid=stack_summary['StackId'])
                    for stack_summary
                    in self.cloudformation_connection.list_stacks()['StackSummaries']
                    if stack_summary['StackName'] == name
                ]
        return stack_list

    def get_stack(self, name):
        """
        Get single stack if it exists

        Args:
            name: unique name or id of the stack
        Returns:
            CloudFormationStack object
        """
        stacks = self.find_stacks(name)
        if not stacks:
            raise NotFoundError("Stack with name {} not found".format(name))
        elif len(stacks) > 1:
            raise MultipleItemsError("Multiple stacks with name {} found".format(name))
        return stacks[0]

    def list_templates(self, executable_by_me=True, owned_by_me=True, public=False):
        """
        List images on ec2 of image-type 'machine'

        Args:
            executable_by_me: search images executable by me (default True)
            owned_by_me: search images owned only by me (default True)
            public: search public images (default False)
        """
        img_filter = {'image-type': 'machine'}

        if not any([public, executable_by_me, owned_by_me]):
            raise ValueError(
                "One of the following must be 'True': owned_by_me, executable_by_me, public")

        images = []
        if public:
            images.extend(self.api.get_all_images(filters=img_filter))
        if executable_by_me:
            images.extend(self.api.get_all_images(executable_by=['self'], filters=img_filter))
        if owned_by_me:
            images.extend(self.api.get_all_images(owners=['self'], filters=img_filter))

        return [EC2Image(system=self, raw=image) for image in set(images)]

    def find_templates(self, name=None, id=None, executable_by_me=True, owned_by_me=True,
                       public=False, filters=None):
        """
        Find image on ec2 system

        Supported queries include searching by name, id, or passing
        in a specific filters dict to the system API. You can only
        select one of these methods.

        Args:
            name (str): name of image
            id (str): id of image
            filters (dict): filters to pass along to system.api.get_all_images()
            executable_by_me: search images executable by me (default True)
            owned_by_me: search images owned only by me (default True)
            public: search public images (default False)

        Returns:
            List of EC2Image objects that match
        """
        # Validate args
        filled_args = [arg for arg in (name, id, filters,) if arg]
        if not filled_args or len(filled_args) > 1:
            raise ValueError(
                "You must select one of these search methods: name, id, or filters")

        if id:
            kwargs = {'image_ids': [id]}
        elif filters:
            kwargs = {'filters': filters}
        elif name:
            # Quick validation that the image name isn't actually an ID
            if name.startswith('ami-'):
                # Switch to using the id search method
                kwargs = {'image_ids': [name]}
            else:
                kwargs = {'filters': {'name': name}}

        if not any([public, executable_by_me, owned_by_me]):
            raise ValueError(
                "One of the following must be 'True': owned_by_me, executable_by_me, public")

        images = []
        if public:
            images.extend(self.api.get_all_images(**kwargs))
        if executable_by_me:
            images.extend(self.api.get_all_images(executable_by=['self'], **kwargs))
        if owned_by_me:
            images.extend(self.api.get_all_images(owners=['self'], **kwargs))

        return [EC2Image(system=self, raw=image) for image in set(images)]

    def get_template(self, name_or_id):
        matches = self.find_templates(name=name_or_id)
        if not matches:
            raise ImageNotFoundError('Unable to find image {}'.format(name_or_id))
        elif len(matches) > 1:
            raise MultipleImagesError(
                'Image name {} returned more than one image '
                'Use the ami-ID or remove duplicates from EC2'.format(name_or_id))
        return matches[0]

    def create_template(self, *args, **kwargs):
        raise NotImplementedError

    # TODO: Move everything below here into the entity/class-based structure

    def create_s3_bucket(self, bucket_name):
        self.logger.info("Creating bucket: '%s'", bucket_name)
        try:
            self.s3_connection.create_bucket(Bucket=bucket_name, CreateBucketConfiguration={
                'LocationConstraint': self.kwargs.get('region')})
            self.logger.info("Success: Bucket was successfully created.")
            return True
        except Exception:
            self.logger.exception("Error: Bucket was not successfully created.")
            return False

    def list_s3_bucket_names(self):
        return [bucket.name for bucket in self.s3_connection.buckets.all()]

    def upload_file_to_s3_bucket(self, bucket_name, file_path, file_name):
        bucket = self.s3_connection.Bucket(bucket_name)
        self.logger.info("uploading file '%s' to bucket: '%s'", file_path, bucket_name)
        if os.path.isfile(file_path):
            try:
                bucket.upload_file(file_path, file_name)
                self.logger.info("Success: uploading file completed")
                return True
            except Exception:
                self.logger.exception("File upload failed.")
                return False
        else:
            self.logger.error("Error: File to upload does not exist.")
            return False

    def object_exists_in_bucket(self, bucket_name, object_key):
        bucket = self.s3_connection.Bucket(name=bucket_name)
        objects = [o for o in bucket.objects.all() if o.key == object_key]
        return any(objects)

    def delete_s3_buckets(self, bucket_names):
        """ Deletes specified bucket(s) with keys """
        deleted_list = []
        if isinstance(bucket_names, (set, list, tuple)):
            buckets = [self.s3_connection.Bucket(obj_name) for obj_name in bucket_names]
        else:
            raise ValueError("Object is not iterable.")
        for bucket in buckets:
            self.logger.info("Trying to delete bucket '%s'", bucket.name)
            keys = [obj.key for obj in bucket.objects.all()]
            try:
                if keys:
                    self.delete_objects_from_s3_bucket(bucket.name, keys)
                bucket.delete()
                deleted_list.append(bucket.name)
                self.logger.info("Success: bucket '%s' was deleted.", bucket.name)
            except Exception as e:
                self.logger.exception("Bucket '%s' deletion failed due to %s", bucket.name,
                                      e.message)
        return deleted_list

    def delete_objects_from_s3_bucket(self, bucket_name, object_keys):
        """Delete each of the given object_keys from the given bucket"""
        if not isinstance(object_keys, list):
            raise ValueError("object_keys argument must be a list of key strings")
        bucket = self.s3_connection.Bucket(name=bucket_name)
        try:
            bucket.delete_objects(
                Delete={'Objects': [{'Key': object_key} for object_key in object_keys]})
            return True
        except Exception:
            self.logger.exception(
                "Deleting object keys %s from Bucket '%s' failed", object_keys, bucket_name)
            return False

    def get_all_disassociated_addresses(self):
        return [
            addr for addr
            in self.api.get_all_addresses()
            if not addr.instance_id and not addr.network_interface_id]

    def release_vpc_address(self, alloc_id):
        self.logger.info(" Releasing EC2 VPC EIP '%s'", str(alloc_id))
        try:
            self.api.release_address(allocation_id=alloc_id)
            return True

        except ActionTimedOutError:
            return False

    def release_address(self, address):
        self.logger.info(" Releasing EC2-CLASSIC EIP '%s'", address)
        try:
            self.api.release_address(public_ip=address)
            return True

        except ActionTimedOutError:
            return False

    def get_all_unattached_volumes(self):
        return [volume for volume in self.api.get_all_volumes() if not
                volume.attach_data.status]

    def delete_sqs_queue(self, queue_url):
        self.logger.info(" Deleting SQS queue '%s'", queue_url)
        try:
            self.sqs_connection.delete_queue(QueueUrl=queue_url)
            return True
        except ActionTimedOutError:
            return False

    def get_all_unused_loadbalancers(self):
        return [
            loadbalancer for loadbalancer
            in self.elb_connection.get_all_load_balancers()
            if not loadbalancer.instances]

    def delete_loadbalancer(self, loadbalancer):
        self.logger.info(" Deleting Elastic Load Balancer '%s'", loadbalancer.name)
        try:
            self.elb_connection.delete_load_balancer(loadbalancer.name)
            return True

        except ActionTimedOutError:
            return False

    def get_all_unused_network_interfaces(self):
        return [eni for eni in self.api.get_all_network_interfaces() if eni.status == "available"]

    def import_image(self, s3bucket, s3key, format="vhd", description=None):
        self.logger.info(
            " Importing image %s from %s bucket with description %s in %s started successfully.",
            s3key, s3bucket, description, format
        )
        try:
            result = self.ec2_connection.import_image(DiskContainers=[
                {
                    'Description': description if description is not None else s3key,
                    'Format': format,
                    'UserBucket': {
                        'S3Bucket': s3bucket,
                        'S3Key': s3key
                    }
                }
            ])
            task_id = result.get("ImportTaskId")
            return task_id

        except Exception:
            self.logger.exception("Import of image '%s' failed.", s3key)
            return False

    def copy_image(self, source_region, source_image, image_id):
        self.logger.info(
            " Copying image %s from region %s to region %s with image id %s",
            source_image, source_region, self.kwargs.get('region'), image_id
        )
        try:
            copy_image = self.ec2_connection.copy_image(
                SourceRegion=source_region, SourceImageId=source_image, Name=image_id)
            return copy_image.image_id

        except Exception:
            self.logger.exception("Copy of image '%s' failed.", source_image)
            return False

    def get_import_image_task(self, task_id):
        result = self.ec2_connection.describe_import_image_tasks(ImportTaskIds=[task_id])
        result_task = result.get("ImportImageTasks")
        return result_task[0]

    def get_image_id_if_import_completed(self, task_id):
        result = self.get_import_image_task(task_id)
        result_status = result.get("Status")
        if result_status == 'completed':
            return result.get("ImageId")
        else:
            return False

    def list_topics(self):
        return self.sns_connection.list_topics()

    def get_arn_if_topic_exists(self, topic_name):
        topics = self.list_topics()

        # There is no way to get topic_name, so it
        # has to be parsed from ARN, which looks
        # like this: arn:aws:sns:sa-east-1:ACCOUNT_NUM:AWSConfig_topic

        topic_found = [
            t.get('TopicArn')
            for t in topics.get('Topics')
            if t.get('TopicArn').split(':')[-1] == topic_name
        ]
        if topic_found:
            return topic_found[0]
        else:
            return False

    def delete_topic(self, arn):
        self.logger.info(" Deleting SNS Topic '%s'", arn)
        try:
            self.sns_connection.delete_topic(TopicArn=arn)
            return True

        except Exception:
            self.logger.exception("Delete of topic '%s' failed.", arn)
            return False

    def volume_exists_and_available(self, volume_name=None, volume_id=None):
        """
        Method for checking existence and availability state for volume

        Args:
            volume_name: Name of volume, if not set volume_id must be set
            volume_id: ID of volume in format vol-random_chars, if not set volume_name must be set

        Returns:
            True if volume exists and is available.
            False if volume doesn't exist or is not available.
        """
        if volume_id:
            try:
                response = self.ec2_connection.describe_volumes(
                    VolumeIds=[volume_id],
                    Filters=[
                        {
                            'Name': 'status',
                            'Values': ['available']
                        }
                    ]
                )
                if response.get('Volumes'):
                    return True
                else:
                    return False
            except Exception:
                return False
        elif volume_name:
            response = self.ec2_connection.describe_volumes(
                Filters=[
                    {
                        'Name': 'status',
                        'Values': ['available']
                    },
                    {
                        'Name': 'tag:Name',
                        'Values': [volume_name]
                    }
                ]
            )
            if response.get('Volumes'):
                return True
            else:
                return False
        else:
            raise TypeError("Neither volume_name nor volume_id were specified.")

    def snapshot_exists(self, snapshot_name=None, snapshot_id=None):
        """
        Method for checking existence of snapshot.

        Args:
            snapshot_name: Name of snapshot, if not set snapshot_id must be set.
            snapshot_id: Id of snapshot in format snap-random_chars, if not set snapshot_name
            must be set.

        Returns:
            True if snapshot exists.
            False if snapshot doesn't exist.
        """
        if snapshot_id:
            try:
                response = self.ec2_connection.describe_snapshots(SnapshotIds=[snapshot_id])
                if response.get('Snapshots'):
                    return True
                else:
                    return False
            except Exception:
                return False
        elif snapshot_name:
            response = self.ec2_connection.describe_snapshots(
                Filters=[
                    {
                        'Name': 'tag:Name',
                        'Values': [snapshot_name]
                    }
                ]
            )
            if response.get('Snapshots'):
                return True
            else:
                return False
        else:
            raise TypeError("Neither snapshot_name nor snapshot_id were specified.")

    def copy_snapshot(self, source_snapshot_id, source_region=None):
        """
        This method is not working properly because of bug in boto3.
        It creates new snapshot with empty size and error.
        Args:
            source_snapshot_id: Id of source snapshot in format snap-random_chars
            source_region: Source region, if not set then ec2_connection region

        Returns:
            True when snapshot copy started successfully.
            False when snapshot copy didn't start.
        """
        if not source_region:
            source_region = self.kwargs.get('region')
        try:
            self.ec2_connection.copy_snapshot(
                SourceRegion=source_region, SourceSnapshotId=source_snapshot_id,
                DestinationRegion=source_region
            )
            return True
        except Exception:
            self.logger.exception("Copy snapshot with id '%s' failed.", source_snapshot_id)
            return False

    def list_load_balancer(self):
        self.logger.info("Attempting to List EC2 Load Balancers")
        return [loadbalancer.name for loadbalancer in self.elb_connection.get_all_load_balancers()]

    def list_network(self):
        self.logger.info("Attempting to List EC2 Virtual Private Networks")
        networks = self.ec2_connection.describe_network_acls()['NetworkAcls']
        # EC2 api does not return the tags of the networks.... so returns only the IDs.
        return [vpc_id['VpcId'] for vpc_id in networks]

    def list_subnet(self):
        self.logger.info("Attempting to List EC2 Subnets")
        subnets = self.ec2_connection.describe_subnets()['Subnets']
        subnets_names = []

        # Subnets are not having mandatory tags names. They can have multiple tags, but only the tag
        # 'Name' will be taken as the subnet name. If not tag is given, CFME displays the SubnetId
        for subnet in subnets:
            if 'Tags' in subnet and subnet['Tags']:
                for tag in subnet['Tags']:
                    if 'Name' in tag.values():
                        subnets_names.append(tag['Value'])
            else:
                subnets_names.append(subnet['SubnetId'])
        return subnets_names

    def list_security_group(self):
        self.logger.info("Attempting to List EC2 security groups")
        return [sec_gp.name for sec_gp in self.api.get_all_security_groups()]

    def list_router(self):
        route_tables = self.ec2_connection.describe_route_tables()['RouteTables']
        routers_names = []

        # Routers names are tags which are not mandatory, and tag with key called Name will be
        # used to name the router. If no tag name is provided, the routerTableId will be
        # displayed as name in CFME.
        for route in route_tables:
            if route['Tags']:
                for tag in route['Tags']:
                    if 'Name' in tag.values():
                        routers_names.append(tag['Value'])
            else:
                routers_names.append(route['RouteTableId'])

        return routers_names

    def list_own_snapshots(self):
        self.logger.info("Attempting to List Own Snapshots")
        return self.ec2_connection.describe_snapshots(OwnerIds=["self"]).get("Snapshots")

    def delete_snapshot(self, snapshot_id):
        # Deletes snapshot, impossible when as AMI root snapshot or as Volume root snapshot
        try:
            self.ec2_connection.delete_snapshot(SnapshotId=snapshot_id)
            return True
        except Exception:
            return False

    def list_queues_with_creation_timestamps(self):
        # Returns dict with queue_urls as keys and creation timestamps as values
        # Max 1000 queues listed with list_queues()
        queue_list = self.sqs_connection.list_queues().get("QueueUrls")
        queue_dict = {}
        if queue_list:
            for queue_url in queue_list:
                try:
                    response = self.sqs_connection.get_queue_attributes(
                        QueueUrl=queue_url, AttributeNames=['CreatedTimestamp'])
                    queue_dict[queue_url] = response.get("Attributes").get("CreatedTimestamp")
                except Exception:
                    pass
        return queue_dict

    def get_registry_data(self):
        # Returns dict with docker registry url and token
        data = self.ecr_connection.get_authorization_token()
        if data['ResponseMetadata']['HTTPStatusCode'] >= 400:
            raise NotFoundError("couldn't get registry details. please check environment setup")

        try:
            first_registry = data['authorizationData'][0]
            username, password = base64.b64decode(first_registry['authorizationToken']).split(':')
            return {'username': username,
                    'password': password,
                    'registry': first_registry['proxyEndpoint']}
        except (IndexError, KeyError):
            raise NotFoundError("couldn't get registry details. please check environment setup")
