# coding: utf-8
from __future__ import absolute_import

import os
import re
from datetime import datetime

import boto
import boto3
import pytz
from boto import cloudformation, sqs
from boto.cloudformation import CloudFormationConnection
from boto.ec2 import EC2Connection, elb, get_region
from boto.ec2.elb import ELBConnection
from boto.sqs import connection
from botocore.client import Config
from wait_for import wait_for

from wrapanapi.entities import (Instance, Stack, StackMixin, Template,
                                TemplateMixin, VmMixin, VmState)
from wrapanapi.exceptions import (ActionNotSupported, ActionTimedOutError,
                                  ImageNotFoundError, MultipleImagesError,
                                  MultipleInstancesError, MultipleItemsError,
                                  NotFoundError, VMInstanceNotFound)
from wrapanapi.systems import System


def _regions(regionmodule, regionname):
    for region in regionmodule.regions():
        if region.name == regionname:
            return region
    return None


class EC2Instance(Instance):
    @staticmethod
    @property
    def state_map():
        return {
            'pending': VmState.STARTING,
            'stopping': VmState.STOPPING,
            'shutting-down': VmState.STOPPING,
            'running': VmState.RUNNING,
            'stopped': VmState.STOPPED,
            'terminated': VmState.DELETED
        }

    def __init__(self, system, id, raw=None):
        """
        Constructor for an EC2Instance tied to a specific system.

        Args:
            system: an EC2System object
            raw: the boto.ec2.instance.Instance object if already obtained, or None
        """
        super(EC2Instance, self).__init__(system)
        self.id = id
        self._raw = raw

    @property
    def raw(self):
        """
        Returns raw boto.ec2.instance.Instance object associated with this instance
        """
        if not self._raw:
            self._raw = self.system.get_vm(self.id)
        return self._raw

    @property
    def name(self):
        return self.raw.tags.get('Name', self.raw.id)

    def refresh(self):
        self.raw.update(validate=True)

    @property
    def exists(self):
        try:
            self.refresh()
            return True
        except ValueError:
            return False

    @property
    def state(self):
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
        # Example instance.launch_time: 2014-08-13T22:09:40.000Z
        launch_time = datetime.strptime(self.raw.launch_time, '%Y-%m-%dT%H:%M:%S.%fZ')
        # use replace here to make tz-aware. python doesn't handle single 'Z' as UTC
        return launch_time.replace(tzinfo=pytz.UTC)

    def rename(self, new_name):
        self.logger.info("setting name of EC2 instance %s to %s" % (self.id, new_name))
        self.raw.add_tag('Name', new_name)
        return new_name

    def delete(self):
        """
        Delete instance. Wait up to 90sec for it to move to 'deleted' state

        Returns:
            True if successful
            False if otherwise, or action timed out
        """
        self.logger.info("terminating EC2 instance {}".format(self.id))
        try:
            self.raw.terminate()
            self.wait_for_state(VmState.DELETED, num_sec=90)
            return True
        except ActionTimedOutError:
            return False

    def cleanup(self):
        return self.delete()

    def start(self):
        """
        Start instance. Wait up to 90sec for it to move to 'running' state

        Returns:
            True if successful
            False if otherwise, or action timed out
        """
        self.logger.info("starting EC2 instance '{}'".format(self.id))
        try:
            self.raw.start()
            self.wait_for_state(VmState.RUNNING, num_sec=90)
            return True
        except ActionTimedOutError:
            return False

    def stop(self):
        """
        Stop instance. Wait up to 360sec for it to move to 'stopped' state

        Returns:
            True if successful
            False if otherwise, or action timed out
        """
        self.logger.info("stopping EC2 instance {}".format(self.id))
        try:
            self.raw.stop()
            self.wait_for_state(VmState.STOPPED, num_sec=360)
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
        self.logger.info("restarting EC2 instance {}".format(self.id))
        stopped = self.stop()
        if not stopped:
            self.logger.error("Stopping instance {} failed or timed out".format(self.id))
        started = self.start()
        if not started:
            self.logger.error("Starting instance {} failed or timed out".format(self.id))
        return stopped and started


class StackStates(object):
    ACTIVE = ('CREATE_COMPLETE', 'ROLLBACK_COMPLETE', 'CREATE_FAILED',
              'UPDATE_ROLLBACK_COMPLETE'),
    COMPLETE = ('CREATE_COMPLETE', 'UPDATE_ROLLBACK_COMPLETE'),
    FAILED = ('ROLLBACK_COMPLETE', 'CREATE_FAILED', 'ROLLBACK_FAILED', 'DELETE_FAILED',
              'UPDATE_ROLLBACK_FAILED'),
    DELETED = ('DELETE_COMPLETE',),
    IN_PROGRESS = ('CREATE_IN_PROGRESS', 'ROLLBACK_IN_PROGRESS', 'DELETE_IN_PROGRESS',
                   'UPDATE_IN_PROGRESS', 'UPDATE_ROLLBACK_COMPLETE_CLEANUP_IN_PROGRESS',
                   'UPDATE_ROLLBACK_IN_PROGRESS', 'UPDATE_COMPLETE_CLEANUP_IN_PROGRESS',
                   'REVIEW_IN_PROGRESS')


class CloudFormationStack(Stack):
    def __init__(self, system, name, id):
        super(CloudFormationStack, self).__init__(system, name)
        self.id = id

    @property
    def exists(self):
        """
        Checks if this stack exists on the system
        """
        try:
            return len(self.get_details()) > 0
        except boto.exception.BotoServerError as e:
            if e.message == 'Stack with id {} does not exist'.format(self.id):
                return False
            else:
                raise

    def get_details(self):
        """
        Returns list of dicts with info about this stack
        """
        return self.system.stackapi.describe_stacks(self.id)[0]

    def refresh(self):
        """
        Re-pull the data for this stack
        """
        details = self.get_details()
        self.name = details.stack_name

    def delete(self):
        """
        Removes the stack on the provider

        Returns:
            True if delete was successful
            False otherwise
        """
        self.logger.info("terminating EC2 stack {}, id: {}" .format(self.name, self.id))
        try:
            self.system.stackapi.delete_stack(self.id)
            return True
        except ActionTimedOutError:
            return False

    def cleanup(self):
        """
        Removes the stack on the provider and any of its associated resources
        """
        return self.delete()


class EC2Image(Template):
    def __init__(self, system, id, raw=None):
        """
        Constructor for an EC2Image tied to a specific system.

        Args:
            system: an EC2System object
            raw: the boto.ec2.image.Image object if already obtained, or None
        """
        super(EC2Image, self).__init__(system)
        self.id = id
        self._raw = raw

    @property
    def raw(self):
        """
        Returns raw boto.ec2.image.Image object associated with this instance
        """
        if not self._raw:
            self._raw = self.system.get_template(id=self.id)
        return self._raw

    @property
    def name(self):
        return self.raw.tags.get('Name', self.raw.id)

    def refresh(self):
        self.raw.update(validate=True)

    @property
    def exists(self):
        try:
            self.refresh()
            return True
        except ValueError:
            return False

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
        return self.system.create_vm(image_id=self.id, *args, **kwargs)


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
        'num_vm': lambda self: len(self.list_vms()),
        'num_template': lambda self: len(self.list_templates()),
    }


    # Possible stack states for reference


    can_suspend = False

    def __init__(self, **kwargs):
        super(EC2System, self).__init__(**kwargs)
        username = kwargs.get('username')
        password = kwargs.get('password')

        regionname = kwargs.get('region')
        region = get_region(kwargs.get('region'))
        self.api = EC2Connection(username, password, region=region)
        self.sqs_connection = connection.SQSConnection(username, password, region=_regions(
            regionmodule=sqs, regionname=regionname))
        self.elb_connection = ELBConnection(username, password, region=_regions(
            regionmodule=elb, regionname=regionname))
        self.s3_connection = boto3.resource('s3', aws_access_key_id=username,
            aws_secret_access_key=password, region_name=regionname, config=Config(
                signature_version='s3v4'))
        self.ec2_connection = boto3.client('ec2', aws_access_key_id=username,
            aws_secret_access_key=password, region_name=regionname,
                config=Config(signature_version='s3v4'))
        self.stackapi = CloudFormationConnection(username, password, region=_regions(
            regionmodule=cloudformation, regionname=regionname))
        self.sns_connection = boto3.client('sns', region_name=regionname)
        self.kwargs = kwargs

    def disconnect(self):
        """Disconnect from the EC2 API -- NOOP

        AWS EC2 service is stateless, so there's nothing to disconnect from
        """
        pass

    def info(self):
        """Returns the current versions of boto and the EC2 API being used"""
        return '%s %s' % (boto.UserAgent, self.api.APIVersion)

    def _get_instances(self, **kwargs):
        """
        Gets instance reservations and parses instance objects
        """
        reservations = self.api.get_all_instances(**kwargs)
        instances = list()
        for reservation in reservations:
            for instance in reservation.instances:
                instances.append(
                    EC2Instance(system=self, id=instance.id, raw=instance)
                )
        return instances

    def find_vms(self, name=None, id=None, filters=None):
        """
        Find instance on ec2 system

        Supported queries include searching by name tag, id, or passing
        in a specific filters dict to the system API. You can only
        select one of these methods.

        Args:
            name (str): name of instance (which is a tag)
            id (str): id of instance
            filters (dict): filters to pass along to system.api.get_all_instances()

        Returns:
            List of EC2Instance objects that match
        """ 
        # Validate args
        filled_args = (arg for arg in (name, id, filters) if arg)
        if len(filled_args) > 1 or len(filled_args) == 0:
            raise ValueError(
                "You must select one of these search methods: name, id, or filters")

        if id:
            kwargs = {'instance_ids': [id]}
        elif filters:
            kwargs = {'filters': filters}
        elif name:
            # Quick validation that the instance name isn't actually an ID
            pattern = re.compile('^i-\w{8,17}$')
            if pattern.match(name):
                # Switch to using the id search method
                kwargs = {'instance_ids': [name]}
            else:
                kwargs = {'filters': {'tag:Name': name}}

        instances = self._get_instances(**kwargs)

        return instances

    def get_vm(self, name):
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
        instances = self.find_vms(name=name)
        if not instances:
            raise VMInstanceNotFound(name)
        elif len(instances) > 1:
            raise MultipleInstancesError('Instance name "%s" is not unique' % name)
        return instances[0]

    def does_vm_exist(self, name):
        try:
            self.get_vm(name)
            return True
        except MultipleInstancesError:
            return True
        except VMInstanceNotFound:
            return False

    def list_vms(self, include_terminated=True):
        """
        Returns a list of instances currently active on EC2 (not terminated)
        """
        instances = list()
        if include_terminated:
            instances = [inst for inst in self._get_instances()]
        else:
            instances = [
                inst for inst in self._get_instances()
                if inst.raw.state != 'terminated'
            ]
        return instances

    def create_vm(self, image_id, min_count=1, max_count=1, instance_type='t1.micro', vm_name=''):
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
        self.logger.info("Creating instances[%d] with name %s,type %s and image ID: %s ",
                         max_count, vm_name, instance_type, image_id)
        try:
            result = self.ec2_connection.run_instances(ImageId=image_id, MinCount=min_count,
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
            self.logger.exception("Create of {} instance failed.".format(vm_name))
            raise

        try:
            instances_json = result['Instances']
            instance_ids = [entry['InstanceId'] for entry in instances_json]
        except KeyError:
            self.logger.exception("Unable to parse all InstanceId's from response json")
            raise

        instances = [EC2Instance(system=self, id=id) for id in instance_ids]
        if len(instances) == 1:
            return instances[0]
        else:
            return instances

    def list_stacks(self, stack_status_filter=StackStates.ACTIVE):
        """
        Returns a list of Stack objects

        stack_status_filter:  filters stacks in certain status. Check StackStates for details.
        """
        stack_list = [
            CloudFormationStack(system=self, name=stack.stack_name, id=stack.stack_id)
            for stack in self.stackapi.list_stacks(stack_status_filter)
        ]
        return stack_list

    def find_stacks(self, name):
        """
        Return list of all stacks with given name

        Args:
            name: name or id to search for
        Returns:
            List of CloudFormationStack objects
        """
        stack_list = [
            CloudFormationStack(system=self, name=stack.stack_name, id=stack.stack_id)
            for stack in self.stackapi.describe_stacks(name)
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
        if len(stacks) > 1:
            raise MultipleItemsError("Multiple stacks with name {} found".format(name))
        if len(stacks) == 0:
            raise NotFoundError("Stack with name {} not found".format(name))
        return stacks[0]

    def list_templates(self):
        private_images = self.api.get_all_images(owners=['self'],
                                                 filters={'image-type': 'machine'})
        shared_images = self.api.get_all_images(executable_by=['self'],
                                                filters={'image-type': 'machine'})
        combined_images = list(set(private_images) | set(shared_images))
        return [EC2Image(system=self, id=image.id, raw=image) for image in combined_images]

    def find_templates(self, name=None, id=None, filters=None):
        """
        Find image on ec2 system

        Supported queries include searching by name, id, or passing
        in a specific filters dict to the system API. You can only
        select one of these methods.

        Args:
            name (str): name of image
            id (str): id of image
            filters (dict): filters to pass along to system.api.get_all_images()

        Returns:
            List of EC2Image objects that match
        """ 
        # Validate args
        filled_args = (arg for arg in (name, id, filters) if arg)
        if len(filled_args) > 1 or len(filled_args) == 0:
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
                kwargs = {'filters': {'Name': name}}

        images = self.api.get_all_images(**kwargs)

        return [EC2Image(system=self, id=image.id, raw=image) for image in images]

    def get_template(self, name_or_id):
        matches = self.find_templates(name=name_or_id)
        if len(matches) == 0:
            raise ImageNotFoundError('Unable to find image {}'.format(name_or_id))
        if len(matches) > 1:
            raise MultipleImagesError(
                'Image name {} returned more than one image '
                'Use the ami-ID or remove duplicates from EC2'.format(name_or_id))
        return matches[0]

    def create_template(self, *args, **kwargs):
        raise NotImplementedError

    # TODO: Move everything below here into the entity/class-based structure

    def create_s3_bucket(self, bucket_name):
        self.logger.info("Creating bucket: {}".format(bucket_name))
        try:
            self.s3_connection.create_bucket(Bucket=bucket_name, CreateBucketConfiguration={
                'LocationConstraint': self.kwargs.get('region')})
            self.logger.info("Success: Bucket was successfully created.")
            return True
        except Exception:
            self.logger.exception("Error: Bucket was not successfully created.")
            return False

    def upload_file_to_s3_bucket(self, bucket_name, file_path, file_name):
        bucket = self.s3_connection.Bucket(bucket_name)
        self.logger.info("uploading file {} to bucket: {}".format(file_path, bucket_name))
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

    def delete_s3_bucket(self, bucket_name):
        """TODO: Force delete - delete all objects and then bucket"""
        bucket = self.s3_connection.Bucket(bucket_name)
        self.logger.info("Trying to delete bucket {}".format(bucket_name))
        try:
            bucket.delete()
            self.logger.info("Success: bucket {} was deleted.".format(bucket_name))
            return True
        except Exception:
            self.logger.exception("Bucket {} deletion failed".format(bucket_name))
            return False

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
                'Deleting object keys {} from Bucket "{}" failed'.format(object_keys, bucket_name))
            return False

    def get_all_disassociated_addresses(self):
        return [
            addr for addr
            in self.api.get_all_addresses()
            if not addr.instance_id and not addr.network_interface_id]

    def release_vpc_address(self, alloc_id):
        self.logger.info(" Releasing EC2 VPC EIP {}".format(str(alloc_id)))
        try:
            self.api.release_address(allocation_id=alloc_id)
            return True

        except ActionTimedOutError:
            return False

    def release_address(self, address):
        self.logger.info(" Releasing EC2-CLASSIC EIP {}".format(address))
        try:
            self.api.release_address(public_ip=address)
            return True

        except ActionTimedOutError:
            return False

    def get_all_unattached_volumes(self):
        return [volume for volume in self.api.get_all_volumes() if not
                volume.attach_data.status]

    def delete_sqs_queue(self, queue_name):
        self.logger.info(" Deleting SQS queue {}".format(queue_name))
        try:
            queue = self.sqs_connection.get_queue(queue_name=queue_name)
            if queue:
                self.sqs_connection.delete_queue(queue=queue)
                return True
            else:
                return False

        except ActionTimedOutError:
            return False

    def get_all_unused_loadbalancers(self):
        return [
            loadbalancer for loadbalancer
            in self.elb_connection.get_all_load_balancers()
            if not loadbalancer.instances]

    def delete_loadbalancer(self, loadbalancer):
        self.logger.info(" Deleting Elastic Load Balancer {}".format(loadbalancer.name))
        try:
            self.elb_connection.delete_load_balancer(loadbalancer.name)
            return True

        except ActionTimedOutError:
            return False

    def get_all_unused_network_interfaces(self):
        return [eni for eni in self.api.get_all_network_interfaces() if eni.status == "available"]

    def import_image(self, s3bucket, s3key, format="vhd", description=None):
        self.logger.info(" Importing image %s from %s bucket with description %s in %s started "
            "successfully.", s3key, s3bucket, description, format)
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
            self.logger.exception("Import of {} image failed.".format(s3key))
            return False

    def copy_image(self, source_region, source_image, image_id):
        self.logger.info(" Copying image %s from region %s to region %s with image id %s",
            source_image, source_region, self.kwargs.get('region'), image_id)
        try:
            copy_image = self.ec2_connection.copy_image(
                SourceRegion=source_region, SourceImageId=source_image, Name=image_id)
            return copy_image.image_id

        except Exception:
            self.logger.exception("Copy of {} image failed.".format(source_image))
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
        self.logger.info(" Deleting SNS Topic {} ".format(arn))
        try:
            self.sns_connection.delete_topic(TopicArn=arn)
            return True

        except Exception:
            self.logger.exception("Delete of {} topic failed.".format(arn))
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
            self.logger.exception("Copy snapshot with id {} failed.".format(source_snapshot_id))
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
            if 'Tags' in subnet and len(subnet['Tags']):
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
            if len(route['Tags']):
                for tag in route['Tags']:
                    if 'Name' in tag.values():
                        routers_names.append(tag['Value'])
            else:
                routers_names.append(route['RouteTableId'])

        return routers_names
