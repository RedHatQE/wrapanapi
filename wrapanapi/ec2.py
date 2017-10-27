# coding: utf-8
from datetime import datetime

import boto
from boto.ec2 import EC2Connection, get_region
from boto.cloudformation import CloudFormationConnection
from boto.sqs import connection
from boto import sqs
from boto.ec2 import elb
from boto import cloudformation
from boto.ec2.elb import ELBConnection
import boto3
from botocore.client import Config
import pytz
import re
from wait_for import wait_for
import os

from base import WrapanapiAPIBaseVM
from exceptions import (
    ActionTimedOutError, ActionNotSupported,
    MultipleInstancesError, VMInstanceNotFound,
    MultipleImagesError, ImageNotFoundError
)


def _regions(regionmodule, regionname):
    for region in regionmodule.regions():
        if region.name == regionname:
            return region
    return None


class EC2System(WrapanapiAPIBaseVM):
    """EC2 Management System, powered by boto

    Wraps the EC2 API and mimics the behavior of other implementors of
    MgmtServiceAPIBase for us in VM control testing

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
        'num_vm': lambda self: len(self.list_vm()),
        'num_template': lambda self: len(self.list_template()),
    }

    states = {
        'running': ('running',),
        'stopped': ('stopped', 'terminated'),
        'suspended': (),
        'deleted': ('terminated',),
    }

    can_suspend = False

    def __init__(self, **kwargs):
        super(EC2System, self).__init__(kwargs)
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

    def list_vm(self, include_terminated=True):
        """Returns a list from instance IDs currently active on EC2 (not terminated)"""
        instances = None
        if include_terminated:
            instances = [inst for inst in self._get_all_instances()]
        else:
            instances = [inst for inst in self._get_all_instances() if inst.state != 'terminated']
        return [i.tags.get('Name', i.id) for i in instances]

    def list_template(self):
        private_images = self.api.get_all_images(owners=['self'],
                                                 filters={'image-type': 'machine'})
        shared_images = self.api.get_all_images(executable_by=['self'],
                                                filters={'image-type': 'machine'})
        combined_images = list(set(private_images) | set(shared_images))
        # Try to pull the image name (might not exist), falling back on ID (must exist)
        return map(lambda i: i.name or i.id, combined_images)

    def list_flavor(self):
        raise NotImplementedError('This function is not supported on this platform.')

    def vm_status(self, instance_id):
        """Returns the status of the requested instance

        Args:
            instance_id: ID of the instance to inspect
        Returns: Instance status.

        See this `page <http://docs.aws.amazon.com/AWSEC2/latest/APIReference/
        ApiReference-ItemType-InstanceStateType.html>`_ for possible return values.

        """
        instance = self._get_instance(instance_id)
        return instance.state

    def vm_type(self, instance_id):
        """Returns the instance type of the requested instance
            e.g. m1.medium, m3.medium etc..

                Args:
                    instance_id: ID of the instance to inspect
                Returns: Instance type.
        """
        instance = self._get_instance(instance_id)
        return instance.instance_type

    def vm_creation_time(self, instance_id):
        instance = self._get_instance(instance_id)
        # Example instance.launch_time: 2014-08-13T22:09:40.000Z
        launch_time = datetime.strptime(instance.launch_time, '%Y-%m-%dT%H:%M:%S.%fZ')
        # use replace here to make tz-aware. python doesn't handle single 'Z' as UTC
        return launch_time.replace(tzinfo=pytz.UTC)

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
            List of created aws instances' IDs.
        """
        self.logger.info(" Creating instances[%d] with name %s,type %s and image ID: %s ",
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
            instances = result.get('Instances')
            instance_ids = []
            for instance in instances:
                instance_ids.append(instance.get('InstanceId'))
            return instance_ids
        except Exception:
            self.logger.exception("Create of {} instance failed.".format(vm_name))
            return None

    def delete_vm(self, instance_id):
        """Deletes the an instance

        Args:
            instance_id: ID of the instance to act on
        Returns: Whether or not the backend reports the action completed
        """
        self.logger.info(" Terminating EC2 instance %s" % instance_id)
        instance_id = self._get_instance_id_by_name(instance_id)
        try:
            self.api.terminate_instances([instance_id])
            self._block_until(instance_id, self.states['deleted'])
            return True
        except ActionTimedOutError:
            return False

    def describe_stack(self, stack_name):
        """Describe stackapi

        Returns the description for the specified stack
        Args:
            stack_name: Unique name of stack
        """
        result = []
        stacks = self.stackapi.describe_stacks(stack_name)
        result.extend(stacks)
        return result

    def stack_exist(self, stack_name):
        stacks = [stack for stack in self.describe_stack(stack_name)
            if stack.stack_name == stack_name]
        if stacks:
            return bool(stacks)

    def delete_stack(self, stack_name):
        """Deletes stack

        Args:
            stack_name: Unique name of stack
        """
        self.logger.info(" Terminating EC2 stack {}" .format(stack_name))
        try:
            self.stackapi.delete_stack(stack_name)
            return True
        except ActionTimedOutError:
            return False

    def start_vm(self, instance_id):
        """Start an instance

        Args:
            instance_id: ID of the instance to act on
        Returns: Whether or not the backend reports the action completed
        """
        self.logger.info(" Starting EC2 instance %s" % instance_id)
        instance_id = self._get_instance_id_by_name(instance_id)
        try:
            self.api.start_instances([instance_id])
            self._block_until(instance_id, self.states['running'])
            return True
        except ActionTimedOutError:
            return False

    def stop_vm(self, instance_id):
        """Stop an instance

        Args:
            instance_id: ID of the instance to act on
        Returns: Whether or not the backend reports the action completed
        """
        self.logger.info(" Stopping EC2 instance %s" % instance_id)
        instance_id = self._get_instance_id_by_name(instance_id)
        try:
            self.api.stop_instances([instance_id])
            self._block_until(instance_id, self.states['stopped'], timeout=360)
            return True
        except ActionTimedOutError:
            return False

    def restart_vm(self, instance_id):
        """Restart an instance

        Args:
            instance_id: ID of the instance to act on
        Returns: Whether or not the backend reports the action completed

        The action is taken in two separate calls to EC2. A 'False' return can
        indicate a failure of either the stop action or the start action.

        Note: There is a reboot_instances call available on the API, but it provides
            less insight than blocking on stop_vm and start_vm. Furthermore,
            there is no "rebooting" state, so there are potential monitoring
            issues that are avoided by completing these steps atomically
        """
        self.logger.info(" Restarting EC2 instance %s" % instance_id)
        return self.stop_vm(instance_id) and self.start_vm(instance_id)

    def is_vm_state(self, instance_id, state):
        return self.vm_status(instance_id) in state

    def is_vm_running(self, instance_id):
        """Is the VM running?

        Args:
            instance_id: ID of the instance to inspect
        Returns: Whether or not the requested instance is running
        """
        try:
            running = self.vm_status(instance_id) in self.states['running']
            return running
        except:
            return False

    def wait_vm_running(self, instance_id, num_sec=360):
        self.logger.info(" Waiting for EC2 instance %s to change status to running" % instance_id)
        wait_for(self.is_vm_running, [instance_id], num_sec=num_sec)

    def is_vm_stopped(self, instance_id):
        """Is the VM stopped?

        Args:
            instance_id: ID of the instance to inspect
        Returns: Whether or not the requested instance is stopped
        """
        return self.vm_status(instance_id) in self.states['stopped']

    def wait_vm_stopped(self, instance_id, num_sec=360):
        self.logger.info(
            " Waiting for EC2 instance %s to change status to stopped or terminated" % instance_id
        )
        wait_for(self.is_vm_stopped, [instance_id], num_sec=num_sec)

    def suspend_vm(self, instance_id):
        """Suspend a VM: Unsupported by EC2

        Args:
            instance_id: ID of the instance to act on
        Raises:
            ActionNotSupported: The action is not supported on the system
        """
        raise ActionNotSupported()

    def is_vm_suspended(self, instance_id):
        """Is the VM suspended? We'll never know because EC2 don't support this.

        Args:
            instance_id: ID of the instance to inspect
        Raises:
            ActionNotSupported: The action is not supported on the system
        """
        raise ActionNotSupported()

    def wait_vm_suspended(self, instance_id, num_sec):
        """We would wait forever - EC2 doesn't support this.

        Args:
            instance_id: ID of the instance to wait for
        Raises:
            ActionNotSupported: The action is not supported on the system
        """
        raise ActionNotSupported()

    def clone_vm(self, source_name, vm_name):
        raise NotImplementedError('This function has not yet been implemented.')

    def deploy_template(self, template, *args, **kwargs):
        """Instantiate the requested template image (ami id)

        Accepts args/kwargs from boto's
        :py:meth:`run_instances<boto:boto.ec2.connection.EC2Connection.run_instances>` method

        Most important args are listed below.

        Args:
            template: Template name (AMI ID) to instantiate
            vm_name: Name of the instance (Name tag to set)
            instance_type: Type (flavor) of the instance

        Returns: Instance ID of the created instance

        Note: min_count and max_count args will be forced to '1'; if you're trying to do
              anything fancier than that, you might be in the wrong place

        """
        # Enforce create_vm only creating one VM
        self.logger.info(" Deploying EC2 template %s" % template)

        # strip out kwargs that ec2 doesn't understand
        timeout = kwargs.pop('timeout', 900)
        vm_name = kwargs.pop('vm_name', None)
        power_on = kwargs.pop('power_on', True)

        # Make sure we only provision one VM
        kwargs.update({'min_count': 1, 'max_count': 1})

        # sanity-check inputs
        if 'instance_type' not in kwargs:
            kwargs['instance_type'] = 'm1.small'
        if not template.startswith('ami'):
            # assume this is a lookup by name, get the ami id
            template = self._get_ami_id_by_name(template)

        # clone!
        reservation = self.api.run_instances(template, *args, **kwargs)
        instances = self._get_instances_from_reservations([reservation])
        # Should have only made one VM; return its ID for use in other methods
        self.wait_vm_running(instances[0].id, num_sec=timeout)

        if vm_name:
            self.set_name(instances[0].id, vm_name)
        if power_on:
            self.start_vm(instances[0].id)
        return instances[0].id

    def set_name(self, instance_id, new_name):
        self.logger.info("Setting name of EC2 instance %s to %s" % (instance_id, new_name))
        instance = self._get_instance(instance_id)
        instance.add_tag('Name', new_name)
        return new_name

    def get_name(self, instance_id):
        return self._get_instance(instance_id).tags.get('Name', instance_id)

    def _get_instance(self, instance_id):
        instance_id = self._get_instance_id_by_name(instance_id)
        reservations = self.api.get_all_instances([instance_id])
        instances = self._get_instances_from_reservations(reservations)
        if len(instances) > 1:
            raise MultipleInstancesError

        try:
            return instances[0]
        except KeyError:
            return None

    def current_ip_address(self, instance_id):
        return str(self._get_instance(instance_id).ip_address)

    def get_ip_address(self, instance_id, **kwargs):
        return self.current_ip_address(instance_id)

    def _get_instance_id_by_name(self, instance_name):
        # Quick validation that the instance name isn't actually an ID
        # If people start naming their instances in such a way to break this,
        # check, that would be silly, but we can upgrade to regex if necessary.
        pattern = re.compile('^i-\w{8,17}$')
        if pattern.match(instance_name):
            return instance_name

        # Filter by the 'Name' tag
        filters = {
            'tag:Name': instance_name,
        }
        reservations = self.api.get_all_instances(filters=filters)
        instances = self._get_instances_from_reservations(reservations)
        if not instances:
            raise VMInstanceNotFound(instance_name)
        elif len(instances) > 1:
            raise MultipleInstancesError('Instance name "%s" is not unique' % instance_name)

        # We have an instance! return its ID
        return instances[0].id

    def _get_ami_id_by_name(self, image_name):
        matches = self.api.get_all_images(filters={'name': image_name})
        if not matches:
            raise ImageNotFoundError(image_name)
        elif len(matches) > 1:
            raise MultipleImagesError('Template name %s returned more than one image_name. '
                'Use the ami-ID or remove duplicates from EC2' % image_name)

        return matches[0].id

    def does_vm_exist(self, name):
        try:
            self._get_instance_id_by_name(name)
            return True
        except MultipleInstancesError:
            return True
        except VMInstanceNotFound:
            return False

    def _get_instances_from_reservations(self, reservations):
        """Takes a sequence of reservations and returns their instances"""
        instances = list()
        for reservation in reservations:
            for instance in reservation.instances:
                instances.append(instance)
        return instances

    def _get_all_instances(self):
        """Gets all instances that EC2 can see"""
        reservations = self.api.get_all_instances()
        instances = self._get_instances_from_reservations(reservations)
        return instances

    def _block_until(self, instance_id, expected, timeout=90):
        """Blocks until the given instance is in one of the expected states

        Takes an optional timeout value.
        """
        wait_for(lambda: self.vm_status(instance_id) in expected, num_sec=timeout)

    def remove_host_from_cluster(self, hostname):
        raise NotImplementedError('remove_host_from_cluster not implemented')

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

    def copy_image(self, source_region, source_image, image_id):
        self.logger.info(" Copying image %s from region %s to region %s with image id %s",
            source_image, source_region, self.kwargs.get('region'), image_id)
        try:
            self.ec2_connection.copy_image(SourceRegion=source_region, SourceImageId=source_image,
                                       Name=image_id)
            return True

        except Exception:
            self.logger.exception("Copy of {} image failed.".format(source_image))
            return False

    def deregister_image(self, image_id, delete_snapshot=True):
        """Deregister the given AMI ID, only valid for self owned AMI's"""
        images = self.api.get_all_images(owners=['self'], filters={'image-type': 'machine'})
        matching_images = [image for image in images if image.id == image_id]

        try:
            for image in matching_images:
                image.deregister(delete_snapshot=delete_snapshot)
            return True
        except Exception:
            self.logger.exception('Deregister of image_id {} failed'.format(image_id))
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
