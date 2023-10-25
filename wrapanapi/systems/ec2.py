import base64
import os
import re

import boto3
from boto3 import client as boto3client
from boto3 import resource as boto3resource
from botocore.config import Config
from botocore.exceptions import ClientError

from wrapanapi.entities import Instance
from wrapanapi.entities import Network
from wrapanapi.entities import NetworkMixin
from wrapanapi.entities import Stack
from wrapanapi.entities import StackMixin
from wrapanapi.entities import Template
from wrapanapi.entities import TemplateMixin
from wrapanapi.entities import VmMixin
from wrapanapi.entities import VmState
from wrapanapi.entities import Volume
from wrapanapi.exceptions import ActionTimedOutError
from wrapanapi.exceptions import MultipleItemsError
from wrapanapi.exceptions import NotFoundError
from wrapanapi.systems.base import System


def _regions(regionmodule, regionname):
    for region in regionmodule.regions():
        if region.name == regionname:
            return region
    return None


class _SharedMethodsMixin:
    """
    Mixin class that holds properties/methods EC2Entities share.
    This should be listed first in the child class inheritance to satisfy
    the methods required by the Entity abstract base class
    """

    @property
    def _identifying_attrs(self):
        return {"uuid": self._uuid}

    @property
    def uuid(self):
        return self._uuid

    def refresh(self):
        try:
            self.raw.reload()
            return True
        except Exception:
            return False

    def get_details(self):
        return self.raw

    def rename(self, new_name):
        self.logger.info(
            "setting name of %s %s to %s", self.__class__.__name__, self.uuid, new_name
        )
        self.raw.create_tags(Tags=[{"Key": "Name", "Value": new_name}])
        self.refresh()  # update raw
        return new_name


class _TagMixin:
    def set_tag(self, key, value):
        self.system.ec2_connection.create_tags(
            Resources=[self.uuid], Tags=[{"Key": key, "Value": value}]
        )

    def get_tag_value(self, key):
        self.refresh()
        if self.raw.tags:
            for tag in self.raw.tags:
                if tag.get("Key") == key:
                    return tag.get("Value")
        return None

    def unset_tag(self, key, value):
        self.system.ec2_connection.delete_tags(
            Resources=[self.uuid], Tags=[{"Key": key, "Value": value}]
        )


class EC2Instance(_TagMixin, _SharedMethodsMixin, Instance):
    state_map = {
        "pending": VmState.STARTING,
        "stopping": VmState.STOPPING,
        "shutting-down": VmState.STOPPING,
        "running": VmState.RUNNING,
        "stopped": VmState.STOPPED,
        "terminated": VmState.DELETED,
    }

    def __init__(self, system, raw=None, **kwargs):
        """
        Constructor for an EC2Instance tied to a specific system.

        Args:
            system: an EC2System object
            raw: the boto.ec2.instance.Instance object if already obtained, or None
            uuid: unique ID of instance
        """

        self._uuid = raw.id if raw else kwargs.get("uuid")
        if not self._uuid:
            raise ValueError("missing required kwarg: 'uuid'")

        super().__init__(system, raw, **kwargs)

        self._api = self.system.ec2_connection

    @property
    def name(self):
        tag_value = self.get_tag_value("Name")
        return getattr(self.raw, "name", None) or tag_value if tag_value else self.raw.id

    def _get_state(self):
        self.refresh()
        return self._api_state_to_vmstate(self.raw.state.get("Name"))

    @property
    def ip(self):
        self.refresh()
        return self.raw.public_ip_address

    @property
    def all_ips(self):
        """Wrapping self.ip to meet abstractproperty requirement

        Returns: (list) the addresses assigned to the machine
        """
        return [self.ip]

    @property
    def type(self):
        return self.raw.instance_type

    @property
    def creation_time(self):
        self.refresh()
        # Example instance.launch_time: datetime.datetime(2019, 3, 13, 14, 45, 33, tzinfo=tzutc())
        return self.raw.launch_time

    @property
    def az(self):
        return self.raw.placement["AvailabilityZone"]

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

    def change_type(self, instance_type):
        try:
            self.raw.modify_attribute(InstanceType={"Value": instance_type})
            return True
        except Exception:
            return False


class StackStates:
    ACTIVE = ["CREATE_COMPLETE", "ROLLBACK_COMPLETE", "CREATE_FAILED", "UPDATE_ROLLBACK_COMPLETE"]
    COMPLETE = ["CREATE_COMPLETE", "UPDATE_ROLLBACK_COMPLETE"]
    FAILED = [
        "ROLLBACK_COMPLETE",
        "CREATE_FAILED",
        "ROLLBACK_FAILED",
        "DELETE_FAILED",
        "UPDATE_ROLLBACK_FAILED",
    ]
    DELETED = ["DELETE_COMPLETE"]
    IN_PROGRESS = [
        "CREATE_IN_PROGRESS",
        "ROLLBACK_IN_PROGRESS",
        "DELETE_IN_PROGRESS",
        "UPDATE_IN_PROGRESS",
        "UPDATE_ROLLBACK_COMPLETE_CLEANUP_IN_PROGRESS",
        "UPDATE_ROLLBACK_IN_PROGRESS",
        "UPDATE_COMPLETE_CLEANUP_IN_PROGRESS",
        "REVIEW_IN_PROGRESS",
    ]
    ALL = [
        "CREATE_IN_PROGRESS",
        "CREATE_FAILED",
        "CREATE_COMPLETE",
        "ROLLBACK_IN_PROGRESS",
        "ROLLBACK_FAILED",
        "ROLLBACK_COMPLETE",
        "DELETE_IN_PROGRESS",
        "DELETE_FAILED",
        "DELETE_COMPLETE",
        "UPDATE_IN_PROGRESS",
        "UPDATE_COMPLETE_CLEANUP_IN_PROGRESS",
        "UPDATE_COMPLETE",
        "UPDATE_ROLLBACK_IN_PROGRESS",
        "UPDATE_ROLLBACK_FAILED",
        "UPDATE_ROLLBACK_COMPLETE_CLEANUP_IN_PROGRESS",
        "UPDATE_ROLLBACK_COMPLETE",
        "REVIEW_IN_PROGRESS",
    ]


class CloudFormationStack(_TagMixin, _SharedMethodsMixin, Stack):
    def __init__(self, system, raw=None, **kwargs):
        """
        Represents a CloudFormation stack

        Args:
            system: instance of EC2System
            raw: raw dict for this stack returned by boto CloudFormation.Client.describe_stacks()
            uuid: the stack ID
        """
        self._uuid = raw.stack_id if raw else kwargs.get("uuid")
        if not self._uuid:
            raise ValueError("missing required kwarg: 'uuid'")

        super().__init__(system, raw, **kwargs)
        self._api = self.system.cloudformation_connection

    @property
    def name(self):
        return self.raw.name

    @property
    def creation_time(self):
        self.refresh()
        return self.raw.creation_time

    @property
    def status_active(self):
        self.refresh()
        return self.raw.stack_status in StackStates.ACTIVE

    def delete(self):
        """
        Removes the stack on the provider

        Returns:
            True if delete was successful
            False otherwise
        """
        self.logger.info("terminating EC2 stack '%s', id: '%s'", self.name, self.uuid)
        try:
            self.raw.delete()
            return True
        except ActionTimedOutError:
            return False

    def cleanup(self):
        """
        Removes the stack on the provider and any of its associated resources
        """
        return self.delete()

    def rename(self, new_name):
        raise NotImplementedError


class EC2Image(_TagMixin, _SharedMethodsMixin, Template):
    def __init__(self, system, raw=None, **kwargs):
        """
        Constructor for an EC2Image tied to a specific system.

        Args:
            system: an EC2System object
            raw: the boto.ec2.image.Image object if already obtained, or None
            uuid: unique ID of the image
        """
        self._uuid = raw.id if raw else kwargs.get("uuid")
        if not self._uuid:
            raise ValueError("missing required kwarg: 'uuid'")

        super().__init__(system, raw, **kwargs)

        self._api = self.system.ec2_connection

    @property
    def name(self):
        tag_value = self.get_tag_value("Name")
        return tag_value if tag_value else self.raw.name

    def delete(self):
        """
        Deregister the EC2 image
        """
        return self.raw.deregister()

    def cleanup(self):
        """
        Deregister the EC2 image and delete the snapshot
        """
        return self.delete()

    def deploy(self, *args, **kwargs):
        """
        Deploy ec2 instance(s) using this template

        Args/kwargs are passed to EC2System.create_vm(), the image_id arg
        will be this image's ID
        """
        return self.system.create_vm(image_id=self.uuid, *args, **kwargs)


class EC2Vpc(_TagMixin, _SharedMethodsMixin, Network):
    def __init__(self, system, raw=None, **kwargs):
        """
        Constructor for an EC2Network tied to a specific system.

        Args:
            system: an EC2System object
            raw: the boto.ec2.network.Network object if already obtained, or None
            uuid: unique ID of the network
        """
        self._uuid = raw.id if raw else kwargs.get("uuid")
        if not self._uuid:
            raise ValueError("missing required kwarg: 'uuid'")

        super().__init__(system, raw, **kwargs)

        self._api = self.system.ec2_connection

    @property
    def name(self):
        tag_value = self.get_tag_value("Name")
        return tag_value if tag_value else self.raw.id

    def delete(self):
        """
        Delete Network
        """
        self.logger.info("Deleting EC2Vpc '%s', id: '%s'", self.name, self.uuid)
        try:
            self.raw.delete()
            return True
        except ActionTimedOutError:
            return False

    def cleanup(self):
        """
        Cleanup Network
        """
        return self.delete()


class EBSVolume(_TagMixin, _SharedMethodsMixin, Volume):
    def __init__(self, system, raw=None, **kwargs):
        """
        Constructor for an EBSVolume tied to a specific system.

        Args:
            system: an EC2System object
            raw: the boto.ec2.volume.Volume object if already obtained, or None
            uuid: unique ID of the volume
        """
        self._uuid = raw.id if raw else kwargs.get("uuid")
        if not self._uuid:
            raise ValueError("missing required kwarg: 'uuid'")

        super().__init__(system, raw, **kwargs)

        self._api = self.system.ec2_connection

    @property
    def name(self):
        tag_value = self.get_tag_value("Name")
        return tag_value if tag_value else self.raw.id

    def resize(self, new_size):
        try:
            self._api.modify_volume(VolumeId=self.uuid, Size=new_size)
            self.refresh()
            return new_size
        except Exception:
            return False

    def attach(self, instance_id, device="/dev/sdh"):
        try:
            self.raw.attach_to_instance(Device=device, InstanceId=instance_id)
            self.refresh()
            return True
        except Exception:
            return False

    def detach(self, instance_id, device="/dev/sdh", force=False):
        try:
            self.raw.detach_from_instance(Device=device, InstanceId=instance_id, Force=force)
            self.refresh()
            return True
        except Exception:
            return False

    def delete(self):
        """
        Delete Volume
        """
        self.logger.info("Deleting EBSVolume '%s', id: '%s'", self.name, self.uuid)
        try:
            self.raw.delete()
            return True
        except ActionTimedOutError:
            return False

    def cleanup(self):
        """
        Cleanup Volume
        """
        return self.delete()


class ResourceExplorerResource:
    """
    This class represents a resource returned by Resource Explorer.
    """

    def __init__(self, arn, region, resource_type, service, properties=[]):
        self.arn = arn
        self.region = region
        self.resource_type = resource_type
        self.service = service
        self.properties = properties

    def get_tag_value(self, key) -> str:
        """
        Returns a tag value for a given tag key.
        Tags are taken from the resource properties.

        Args:
            key: a tag key
        """
        tags = self.get_tags(regex=f"^{key}$")
        if len(tags) > 0:
            return tags[0].get("Value")
        return None

    def get_tags(self, regex="") -> list[dict]:
        """
        Returns a list of tags (a dict with keys 'Key' and 'Value').
        Tags are taken from the resource properties.

        Args:
            regex: a regular expressions for keys, default is ""
        """
        list = []
        for property in self.properties:
            data = property.get("Data")
            for tag in data:
                key = tag.get("Key")
                if re.match(regex, key):
                    list.append(tag)
        return list

    @property
    def id(self) -> str:
        """
        Returns the last part of the arn.
        This part is used as id in aws cli.
        """
        if self.arn:
            return self.arn.split(":")[-1]
        return None

    @property
    def name(self) -> str:
        """
        Returns a name for the resource derived from the associated tag with key 'Name'.
        If there is no such tag then the name is the id from arn.
        """
        name = self.get_tag_value("Name")
        if not name:
            name = self.id
        return name


class EC2System(System, VmMixin, TemplateMixin, StackMixin, NetworkMixin):
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
        "num_vm": lambda self: len(self.list_vms()),
        "num_template": lambda self: len(self.list_templates()),
    }

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._username = kwargs.get("username")
        self._password = kwargs.get("password")
        self._region_name = kwargs.get("region")

        connection_config = Config(signature_version="s3v4", retries=dict(max_attempts=10))
        connection_kwargs = {
            "aws_access_key_id": self._username,
            "aws_secret_access_key": self._password,
            "region_name": self._region_name,
            "config": connection_config,
        }

        self.sqs_connection = boto3client("sqs", **connection_kwargs)
        self.elb_connection = boto3client("elb", **connection_kwargs)
        self.s3_connection = boto3resource("s3", **connection_kwargs)
        self.s3_client = boto3client("s3", **connection_kwargs)
        self.ec2_connection = boto3client("ec2", **connection_kwargs)
        self.ec2_resource = boto3resource("ec2", **connection_kwargs)
        self.ecr_connection = boto3client("ecr", **connection_kwargs)
        self.cloudformation_connection = boto3client("cloudformation", **connection_kwargs)
        self.cloudformation_resource = boto3resource("cloudformation", **connection_kwargs)
        self.ssm_connection = boto3client("ssm", **connection_kwargs)
        self.sns_connection = boto3client("sns", **connection_kwargs)
        self.cw_events_connection = boto3client("events", **connection_kwargs)
        self.resource_explorer_connection = boto3client("resource-explorer-2", **connection_kwargs)

        self.kwargs = kwargs

    @property
    def _identifying_attrs(self):
        return {"username": self._username, "password": self._password, "region": self._region_name}

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
        """Returns the current versions of boto3"""
        return boto3.__version__

    def _get_resource(self, resource, find_method, name=None, id=None, **kwargs):
        """
        Get a single resource with name equal to 'name' or id equal to 'id'

        Must be a unique name or id

        Args:
            resource: Class of entity to get
            find_method: Find method of entity get will use
            name: name of resource
            id: id of resource
        Returns:
            resource object
        Raises:
            NotFoundError if no resource exists with this name/id
            MultipleItemsError if name/id is not unique
        """
        resource_name = resource.__name__
        if not name and not id or name and id:
            raise ValueError("Either name or id must be set and not both!")
        resources = find_method(name=name, id=id, **kwargs)
        name_or_id = name if name else id
        if not resources:
            raise NotFoundError(
                "{} with {} {} not found".format(
                    resource_name, "name" if name else "id", name_or_id
                )
            )
        elif len(resources) > 1:
            raise MultipleItemsError(
                "Multiple {}s with {} {} found".format(
                    resource_name, "name" if name else "id", name_or_id
                )
            )
        return resources[0]

    def _get_instances(self, **kwargs):
        """
        Gets instance reservations and parses instance objects
        """
        reservations = self.ec2_connection.describe_instances(**kwargs).get("Reservations")
        instances = list()
        for reservation in reservations:
            for instance in reservation.get("Instances"):
                instances.append(
                    EC2Instance(
                        system=self, raw=self.ec2_resource.Instance(instance.get("InstanceId"))
                    )
                )
        return instances

    @staticmethod
    def _add_filter_for_terminated(kwargs_dict):
        new_filter = {
            "Name": "instance-state-name",
            "Values": [
                api_state
                for api_state, vm_state in EC2Instance.state_map.items()
                if vm_state is not VmState.DELETED
            ],
        }
        if "Filters" not in kwargs_dict:
            kwargs_dict["Filters"] = [new_filter]
        else:
            kwargs_dict["Filters"].append(new_filter)
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
        filled_args = [
            arg
            for arg in (
                name,
                id,
                filters,
            )
            if arg
        ]
        if not filled_args or len(filled_args) > 1:
            raise ValueError("You must select one of these search methods: name, id, or filters")

        if id:
            kwargs = {"InstanceIds": [id]}
        elif filters:
            kwargs = {"Filters": filters}
        elif name:
            # Quick validation that the instance name isn't actually an ID
            pattern = re.compile(r"^i-\w{8,17}$")
            if pattern.match(name):
                # Switch to using the id search method
                kwargs = {"InstanceIds": [name]}
            else:
                kwargs = {"Filters": [{"Name": "tag:Name", "Values": [name]}]}

        if hide_deleted:
            self._add_filter_for_terminated(kwargs)

        instances = self._get_instances(**kwargs)

        return instances

    def get_vm(self, name=None, id=None):
        """
        Get a single EC2Instance with name or id equal to 'name'

        Must be a unique name

        Args:
            name: name or id of instance
        Returns:
            EC2Instance object
        Raises:
            NotFoundError if no instance exists with this name/id
            MultipleItemsError if name is not unique
        """
        return self._get_resource(name=name, id=id, resource=EC2Instance, find_method=self.find_vms)

    def list_vms(self, hide_deleted=True):
        """
        Returns a list of instances currently active on EC2 (not terminated)
        """
        kwargs = {}
        if hide_deleted:
            self._add_filter_for_terminated(kwargs)
        return [inst for inst in self._get_instances(**kwargs)]

    def create_vm(
        self, image_id, min_count=1, max_count=1, instance_type="t1.micro", vm_name="", **kwargs
    ):
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
        self.logger.info(
            "Creating instances[%d] with name %s,type %s and image ID: %s ",
            max_count,
            vm_name,
            instance_type,
            image_id,
        )
        try:
            result = self.ec2_connection.run_instances(
                ImageId=image_id,
                MinCount=min_count,
                MaxCount=max_count,
                InstanceType=instance_type,
                TagSpecifications=[
                    {
                        "ResourceType": "instance",
                        "Tags": [
                            {
                                "Key": "Name",
                                "Value": vm_name,
                            },
                        ],
                    },
                ],
            )
        except Exception:
            self.logger.exception("Create of instance '%s' failed.", vm_name)
            raise

        try:
            instances_json = result["Instances"]
            instance_ids = [entry["InstanceId"] for entry in instances_json]
        except KeyError:
            self.logger.exception("Unable to parse all InstanceId's from response json")
            raise

        instances = [
            EC2Instance(system=self, raw=self.ec2_resource.Instance(uuid), uuid=uuid)
            for uuid in instance_ids
        ]
        for instance in instances:
            self.logger.info("Waiting for instance '%s' to reach steady state", instance.uuid)
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
            CloudFormationStack(
                system=self,
                uuid=stack_summary["StackId"],
                raw=self.cloudformation_resource.Stack(stack_summary["StackName"]),
            )
            for stack_summary in self.cloudformation_connection.list_stacks()["StackSummaries"]
            if stack_summary["StackStatus"] in stack_status_filter
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
            raise ValueError("missing one of required kwargs: name, id")

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
                CloudFormationStack(
                    system=self,
                    uuid=stack["StackId"],
                    raw=self.cloudformation_resource.Stack(stack["StackName"]),
                )
                for stack in self.cloudformation_connection.describe_stacks(StackName=name_or_id)[
                    "Stacks"
                ]
            ]
        except ClientError as error:
            # Stack not found, if searching by name, look through deleted stacks...
            if searching_by_name and f"Stack with id {name} does not exist" in str(error):
                stack_list = [
                    CloudFormationStack(
                        system=self,
                        uuid=stack_summary["StackId"],
                        raw=self.cloudformation_resource.Stack(stack_summary["StackName"]),
                    )
                    for stack_summary in self.cloudformation_connection.list_stacks()[
                        "StackSummaries"
                    ]
                    if stack_summary["StackName"] == name
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
        return self._get_resource(
            name=name, resource=CloudFormationStack, find_method=self.find_stacks
        )

    def list_templates(self, executable_by_me=True, owned_by_me=True, public=False):
        """
        List images on ec2 of image-type 'machine'

        Args:
            executable_by_me: search images executable by me (default True)
            owned_by_me: search images owned only by me (default True)
            public: search public images (default False)
        """
        img_filter = [{"Name": "image-type", "Values": ["machine"]}]

        if not any([public, executable_by_me, owned_by_me]):
            raise ValueError(
                "One of the following must be 'True': owned_by_me, executable_by_me, public"
            )

        images = []
        if public:
            img_filter.append({"Name": "is-public", "Values": ["true"]})
            images.extend(self.ec2_connection.describe_images(Filters=img_filter).get("Images"))
        if executable_by_me:
            images.extend(
                self.ec2_connection.describe_images(
                    ExecutableUsers=["self"], Filters=img_filter
                ).get("Images")
            )
        if owned_by_me:
            images.extend(
                self.ec2_connection.describe_images(Owners=["self"], Filters=img_filter).get(
                    "Images"
                )
            )

        return [
            EC2Image(system=self, raw=self.ec2_resource.Image(image["ImageId"])) for image in images
        ]

    def list_free_images(self, image_list=None):
        """
        Returns images which don't have a VM associated to it

        Args:
            image_list (list): List of  ids of all images in resource group
        """
        free_images = []
        vm_list = self.list_vms()

        if not vm_list:
            # No VMs using the images, images are free
            return image_list

        for vm in vm_list:
            if vm.raw.image_id not in image_list:
                free_images.append(vm.raw.image_id)
        return free_images

    def delete_images(self, image_list=None):
        """
        Deletes images by ID

        Args:
            image_list (list): ["imageID_1", "imageID_2"]
        """
        for image in image_list:
            EC2Image(system=self, raw=self.ec2_resource.Image(image)).delete()

    def find_templates(
        self,
        name=None,
        id=None,
        executable_by_me=True,
        owned_by_me=True,
        public=False,
        filters=None,
    ):
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
        filled_args = [
            arg
            for arg in (
                name,
                id,
                filters,
            )
            if arg
        ]
        if not filled_args or len(filled_args) > 1:
            raise ValueError("You must select one of these search methods: name, id, or filters")

        if id:
            kwargs = {"ImageIds": [id]}
        elif filters:
            kwargs = {"Filters": filters}
        elif name:
            # Quick validation that the image name isn't actually an ID
            if name.startswith("ami-"):
                # Switch to using the id search method
                kwargs = {"ImageIds": [name]}
            else:
                kwargs = {"Filters": [{"Name": "name", "Values": [name]}]}

        if not any([public, executable_by_me, owned_by_me]):
            raise ValueError(
                "One of the following must be 'True': owned_by_me, executable_by_me, public"
            )

        images = []
        if public:
            public_kwargs = {"Filters": [{"Name": "is-public", "Values": ["true"]}]}
            if "Filters" in kwargs:
                public_kwargs["Filters"] = kwargs["Filters"] + public_kwargs["Filters"]
            else:
                public_kwargs.update(kwargs)
            images.extend(self.ec2_connection.describe_images(**public_kwargs).get("Images"))
        if executable_by_me:
            images.extend(
                self.ec2_connection.describe_images(ExecutableUsers=["self"], **kwargs).get(
                    "Images"
                )
            )
        if owned_by_me:
            images.extend(
                self.ec2_connection.describe_images(Owners=["self"], **kwargs).get("Images")
            )

        return [
            EC2Image(system=self, raw=self.ec2_resource.Image(image["ImageId"])) for image in images
        ]

    def get_template(self, name_or_id):
        try:
            template = self._get_resource(
                name=name_or_id, resource=EC2Image, find_method=self.find_templates
            )
        except Exception:
            template = self._get_resource(
                name=name_or_id, resource=EC2Image, find_method=self.find_templates, public=True
            )
        return template

    def create_template(self, *args, **kwargs):
        raise NotImplementedError

    # TODO: Move everything below here into the entity/class-based structure

    def create_s3_bucket(self, bucket_name):
        self.logger.info("Creating bucket: '%s'", bucket_name)
        try:
            self.s3_connection.create_bucket(
                Bucket=bucket_name,
                CreateBucketConfiguration={"LocationConstraint": self.kwargs.get("region")},
            )
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
        """Deletes specified bucket(s) with keys"""
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
                self.logger.exception(
                    "Bucket '%s' deletion failed due to %s", bucket.name, e.message
                )
        return deleted_list

    def delete_objects_from_s3_bucket(self, bucket_name, object_keys):
        """Delete each of the given object_keys from the given bucket"""
        if not isinstance(object_keys, list):
            raise ValueError("object_keys argument must be a list of key strings")
        bucket = self.s3_connection.Bucket(name=bucket_name)
        try:
            bucket.delete_objects(
                Delete={"Objects": [{"Key": object_key} for object_key in object_keys]}
            )
            return True
        except Exception:
            self.logger.exception(
                "Deleting object keys %s from Bucket '%s' failed", object_keys, bucket_name
            )
            return False

    def get_all_disassociated_addresses(self):
        return [
            addr
            for addr in self.ec2_connection.describe_addresses().get("Addresses")
            if not addr.get("InstanceId") and not addr.get("NetworkInterfaceId")
        ]

    def release_vpc_address(self, alloc_id):
        self.logger.info(" Releasing EC2 VPC EIP '%s'", str(alloc_id))
        try:
            self.ec2_connection.release_address(AllocationId=alloc_id)
            return True

        except ActionTimedOutError:
            return False

    def release_address(self, address):
        self.logger.info(" Releasing EC2-CLASSIC EIP '%s'", address)
        try:
            self.ec2_connection.release_address(PublicIp=address)
            return True

        except ActionTimedOutError:
            return False

    def get_all_unattached_volumes(self):
        return self.ec2_connection.describe_volumes(
            Filters=[{"Name": "status", "Values": ["available"]}]
        ).get("Volumes")

    def delete_sqs_queue(self, queue_url):
        self.logger.info(" Deleting SQS queue '%s'", queue_url)
        try:
            self.sqs_connection.delete_queue(QueueUrl=queue_url)
            return True
        except ActionTimedOutError:
            return False

    def get_all_unused_loadbalancers(self):
        return [
            loadbalancer
            for loadbalancer in self.elb_connection.describe_load_balancers().get(
                "LoadBalancerDescriptions"
            )
            if not loadbalancer.get("Instances")
        ]

    def delete_loadbalancer(self, loadbalancer):
        self.logger.info(
            " Deleting Elastic Load Balancer '%s'", loadbalancer.get("LoadBalancerName")
        )
        try:
            self.elb_connection.delete_load_balancer(
                LoadBalancerName=loadbalancer.get("LoadBalancerName")
            )
            return True

        except ActionTimedOutError:
            return False

    def get_all_unused_network_interfaces(self):
        return self.ec2_connection.describe_network_interfaces(
            Filters=[{"Name": "status", "Values": ["available"]}]
        ).get("NetworkInterfaces")

    def import_image(self, s3bucket, s3key, format="vhd", description=None):
        self.logger.info(
            " Importing image %s from %s bucket with description %s in %s started successfully.",
            s3key,
            s3bucket,
            description,
            format,
        )
        try:
            result = self.ec2_connection.import_image(
                DiskContainers=[
                    {
                        "Description": description if description is not None else s3key,
                        "Format": format,
                        "UserBucket": {"S3Bucket": s3bucket, "S3Key": s3key},
                    }
                ]
            )
            task_id = result.get("ImportTaskId")
            return task_id

        except Exception:
            self.logger.exception("Import of image '%s' failed.", s3key)
            return False

    def copy_image(self, source_region, source_image, image_id):
        self.logger.info(
            " Copying image %s from region %s to region %s with image id %s",
            source_image,
            source_region,
            self.kwargs.get("region"),
            image_id,
        )
        try:
            copy_image = self.ec2_connection.copy_image(
                SourceRegion=source_region, SourceImageId=source_image, Name=image_id
            )
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
        if result_status == "completed":
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
            t.get("TopicArn")
            for t in topics.get("Topics")
            if t.get("TopicArn").split(":")[-1] == topic_name
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
                    VolumeIds=[volume_id], Filters=[{"Name": "status", "Values": ["available"]}]
                )
                if response.get("Volumes"):
                    return True
                else:
                    return False
            except Exception:
                return False
        elif volume_name:
            response = self.ec2_connection.describe_volumes(
                Filters=[
                    {"Name": "status", "Values": ["available"]},
                    {"Name": "tag:Name", "Values": [volume_name]},
                ]
            )
            if response.get("Volumes"):
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
                if response.get("Snapshots"):
                    return True
                else:
                    return False
            except Exception:
                return False
        elif snapshot_name:
            response = self.ec2_connection.describe_snapshots(
                Filters=[{"Name": "tag:Name", "Values": [snapshot_name]}]
            )
            if response.get("Snapshots"):
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
            source_region = self.kwargs.get("region")
        try:
            self.ec2_connection.copy_snapshot(
                SourceRegion=source_region,
                SourceSnapshotId=source_snapshot_id,
                DestinationRegion=source_region,
            )
            return True
        except Exception:
            self.logger.exception("Copy snapshot with id '%s' failed.", source_snapshot_id)
            return False

    def list_load_balancer(self):
        self.logger.info("Attempting to List EC2 Load Balancers")
        return [
            loadbalancer.get("LoadBalancerName")
            for loadbalancer in self.elb_connection.describe_load_balancers().get(
                "LoadBalancerDescriptions"
            )
        ]

    def list_network(self):
        self.logger.info("Attempting to List EC2 Virtual Private Networks")
        networks = self.ec2_connection.describe_network_acls()["NetworkAcls"]
        # EC2 api does not return the tags of the networks.... so returns only the IDs.
        return [vpc_id["VpcId"] for vpc_id in networks]

    def list_subnet(self):
        self.logger.info("Attempting to List EC2 Subnets")
        subnets = self.ec2_connection.describe_subnets()["Subnets"]
        subnets_names = []

        # Subnets are not having mandatory tags names. They can have multiple tags, but only the tag
        # 'Name' will be taken as the subnet name. If not tag is given, CFME displays the SubnetId
        for subnet in subnets:
            subnet_name = None
            if "Tags" in subnet and subnet["Tags"]:
                for tag in subnet["Tags"]:
                    if "Name" in list(tag.values()):
                        subnet_name = tag["Value"]
                        break
            if not subnet_name:
                subnet_name = subnet["SubnetId"]
            subnets_names.append(subnet_name)
        return subnets_names

    def list_security_group(self):
        self.logger.info("Attempting to List EC2 security groups")
        return [
            sec_gp.get("GroupName")
            for sec_gp in self.ec2_connection.describe_security_groups().get("SecurityGroups")
        ]

    def list_router(self):
        route_tables = self.ec2_connection.describe_route_tables()["RouteTables"]
        routers_names = []

        # Routers names are tags which are not mandatory, and tag with key called Name will be
        # used to name the router. If no tag name is provided, the routerTableId will be
        # displayed as name in CFME.
        for route in route_tables:
            router_name = None
            if route["Tags"]:
                for tag in route["Tags"]:
                    if "Name" in list(tag.values()):
                        router_name = tag["Value"]
                        break
            if not router_name:
                router_name = route["RouteTableId"]
            routers_names.append(router_name)
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
                        QueueUrl=queue_url, AttributeNames=["CreatedTimestamp"]
                    )
                    queue_dict[queue_url] = response.get("Attributes").get("CreatedTimestamp")
                except Exception:
                    pass
        return queue_dict

    def get_registry_data(self):
        # Returns dict with docker registry url and token
        data = self.ecr_connection.get_authorization_token()
        if data["ResponseMetadata"]["HTTPStatusCode"] >= 400:
            raise NotFoundError("couldn't get registry details. please check environment setup")

        try:
            first_registry = data["authorizationData"][0]
            encoded_data = base64.b64decode(first_registry["authorizationToken"].encode("utf-8"))
            username, password = encoded_data.decode("utf-8").split(":")
            return {
                "username": username,
                "password": password,
                "registry": first_registry["proxyEndpoint"],
            }
        except (IndexError, KeyError):
            raise NotFoundError("couldn't get registry details. please check environment setup")

    def create_network(self, cidr_block="10.0.0.0/16"):
        try:
            response = self.ec2_connection.create_vpc(CidrBlock=cidr_block)
            network_id = response.get("Vpc").get("VpcId")
            return EC2Vpc(system=self, uuid=network_id, raw=self.ec2_resource.Vpc(network_id))
        except Exception:
            return False

    def get_network(self, name=None, id=None):
        return self._get_resource(EC2Vpc, self.find_networks, name=name, id=id)

    def list_networks(self):
        """
        Returns a list of Network objects
        """
        network_list = [
            EC2Vpc(system=self, uuid=vpc["VpcId"], raw=self.ec2_resource.Vpc(vpc["VpcId"]))
            for vpc in self.ec2_connection.describe_vpcs().get("Vpcs")
        ]
        return network_list

    def find_networks(self, name=None, id=None):
        """
        Return list of all networks with given name or id
        Args:
            name: name to search
            id: id to search
        Returns:
            List of EC2Vpc objects
        """
        if not name and not id or name and id:
            raise ValueError("Either name or id must be set and not both!")
        if id:
            vpcs = self.ec2_connection.describe_vpcs(VpcIds=[id])
        else:
            vpcs = self.ec2_connection.describe_vpcs(
                Filters=[{"Name": "tag:Name", "Values": [name]}]
            )
        return [
            EC2Vpc(system=self, raw=self.ec2_resource.Vpc(vpc["VpcId"])) for vpc in vpcs.get("Vpcs")
        ]

    def create_volume(self, az, iops=None, encrypted=False, size=10, type="gp2", name=None):
        """
        Creates volume
        Args:
            az: rquired field availability zone where to create volume in
            iops: iops value for io1 volume type
            encrypted: whether volume should be encrypted - default is False
            size: size of volume - default is 10GB
            type: type of volume 'standard'|'io1'|'gp2'|'sc1'|'st1' - default is gp2
            name: name of volume, default is None
        Returns:
            Created Volume object
        """
        attributes = {
            "AvailabilityZone": az,
            "Size": size,
            "VolumeType": type,
            "Encrypted": encrypted,
        }
        if type not in ("standard", "io1", "gp2", "sc1", "st1"):
            raise ValueError("One of 'standard'|'io1'|'gp2'|'sc1'|'st1' volume types must be set!")
        if type == "io1":
            if not iops:
                raise ValueError("iops parameter must be set when creating io1 volume type!")
            else:
                attributes["Iops"] = iops
        if name:
            attributes["TagSpecifications"] = [
                {"Tags": [{"Key": "Name", "Value": name}], "ResourceType": "volume"}
            ]
        try:
            response = self.ec2_connection.create_volume(**attributes)
            volume_id = response.get("VolumeId")
            return EBSVolume(system=self, uuid=volume_id, raw=self.ec2_resource.Volume(volume_id))
        except Exception:
            return False

    def get_volume(self, name=None, id=None):
        return self._get_resource(EBSVolume, self.find_volumes, name=name, id=id)

    def list_volumes(self):
        """
        Returns a list of Volumes objects
        """
        volume_list = [
            EBSVolume(
                system=self,
                uuid=volume["VolumeId"],
                raw=self.ec2_resource.Volume(volume["VolumeId"]),
            )
            for volume in self.ec2_connection.describe_volumes().get("Volumes")
        ]
        return volume_list

    def find_volumes(self, name=None, id=None):
        """
        Return list of all volumes with given name or id
        Args:
            name: name to search
            id: id to search
        Returns:
            List of EBSVolume objects
        """
        if not name and not id or name and id:
            raise ValueError("Either name or id must be set and not both!")
        if id:
            volumes = self.ec2_connection.describe_volumes(VolumeIds=[id])
        else:
            volumes = self.ec2_connection.describe_volumes(
                Filters=[{"Name": "tag:Name", "Values": [name]}]
            )
        return [
            EBSVolume(system=self, raw=self.ec2_resource.Volume(volume["VolumeId"]))
            for volume in volumes.get("Volumes")
        ]

    def list_regions(self, verbose=False):
        regions = self.ec2_connection.describe_regions().get("Regions")
        region_names = [r.get("RegionName") for r in regions]
        if not verbose:
            return region_names

        verbose_region_names = []
        for region in region_names:
            tmp = f"/aws/service/global-infrastructure/regions/{region}/longName"
            ssm_response = self.ssm_connection.get_parameter(Name=tmp)
            verbose_region_names.append(ssm_response["Parameter"]["Value"])
        return verbose_region_names

    def create_stack(
        self, name, template_url=None, template_body=None, parameters=None, capabilities=None
    ):
        if (not template_body and not template_url) or (template_body and template_url):
            raise ValueError("Either template_body or template_url must be set and not both!")
        stack_kwargs = {
            "StackName": name,
        }
        if template_body:
            stack_kwargs["TemplateBody"] = template_body
        else:
            stack_kwargs["TemplateURL"] = template_url
        if parameters:
            stack_kwargs["Parameters"] = parameters
        if capabilities:
            stack_kwargs["Capabilities"] = capabilities

        response = self.cloudformation_connection.create_stack(**stack_kwargs)
        stack_id = response.get("StackId")
        return CloudFormationStack(
            system=self, uuid=stack_id, raw=self.cloudformation_resource.Stack(stack_id)
        )

    def set_sns_topic_target_for_all_cw_rules(self, topic_arn):
        # After recreating sns topic cloudwatch rule targets are not set so we need to set them back
        try:
            # Get all enabled rules
            rules = self.cw_events_connection.list_rules().get("Rules")
            enabled_rules = []
            for rule in rules:
                if rule.get("State") == "ENABLED":
                    enabled_rules.append(rule.get("Name"))
            # Set targets to rules again
            for enabled_rule in enabled_rules:
                target = self.cw_events_connection.list_targets_by_rule(Rule=enabled_rule).get(
                    "Targets"
                )[0]
                target["Arn"] = topic_arn
                self.cw_events_connection.put_targets(Rule=enabled_rule, Targets=[target])
            return True
        except Exception:
            return False

    def import_snapshot(self, s3bucket, s3key, format="vhd", description=None):
        self.logger.info(
            " Importing snapshot %s from %s bucket with description %s in %s started successfully.",
            s3key,
            s3bucket,
            description,
            format,
        )
        try:
            result = self.ec2_connection.import_snapshot(
                DiskContainer={
                    "Description": description if description is not None else s3key,
                    "Format": format,
                    "UserBucket": {"S3Bucket": s3bucket, "S3Key": s3key},
                }
            )
            task_id = result.get("ImportTaskId")
            return task_id

        except Exception:
            self.logger.exception("Import of snapshot '%s' failed.", s3key)
            return False

    def get_import_snapshot_task(self, task_id):
        result = self.ec2_connection.describe_import_snapshot_tasks(ImportTaskIds=[task_id])
        result_task = result.get("ImportSnapshotTasks")
        return result_task[0]

    def get_snapshot_id_if_import_completed(self, task_id):
        result = self.get_import_snapshot_task(task_id).get("SnapshotTaskDetail")
        result_status = result.get("Status")
        if result_status == "completed":
            return result.get("SnapshotId")
        else:
            return False

    def create_image_from_snapshot(
        self,
        name,
        snapshot_id,
        architecture="x86_64",
        ena_support=True,
        virtualization_type="hvm",
        device_name="/dev/sda1",
    ):
        try:
            ami_id = self.ec2_connection.register_image(
                Name=name,
                Architecture=architecture,
                VirtualizationType=virtualization_type,
                RootDeviceName=device_name,
                EnaSupport=ena_support,
                BlockDeviceMappings=[
                    {
                        "DeviceName": device_name,
                        "Ebs": {"SnapshotId": snapshot_id, "DeleteOnTermination": True},
                    }
                ],
            )
            return ami_id
        except Exception:
            self.logger.exception("Creation of image from snapshot '%s' failed.", snapshot_id)
            return False

    def remove_network_interface_by_id(self, nic_id):
        try:
            self.ec2_connection.delete_network_interface(NetworkInterfaceId=nic_id)
            return True
        except Exception:
            self.logger.exception(f"Removal of Network interface id {nic_id} failed.")
            return False

    def remove_volume_by_id(self, volume_id):
        try:
            self.ec2_connection.delete_volume(VolumeId=volume_id)
            return True
        except Exception:
            self.logger.exception(f"Removal of Volume by id {volume_id} failed.")
            return False

    def remove_all_unused_nics(self):
        """
        Remove all unused Network interfaces in given region

        Returns: None
        """
        all_unused_nics = self.get_all_unused_network_interfaces()
        for nic in all_unused_nics:
            self.remove_network_interface_by_id(nic_id=nic["NetworkInterfaceId"])

    def remove_all_unused_volumes(self):
        """
        Remove all unused Volumes in given region

        Returns: None
        """
        all_unused_volumes = self.get_all_unattached_volumes()
        for volume in all_unused_volumes:
            self.remove_volume_by_id(volume_id=volume["VolumeId"])

    def remove_all_unused_ips(self):
        """
        Remove all disassociated addresses in given region

        Returns: None
        """
        all_unused_ips = self.get_all_disassociated_addresses()
        for ip in all_unused_ips:
            self.release_vpc_address(alloc_id=ip["AllocationId"])

    def cleanup_resources(self):
        """
        Removes all unused NICs, Volumes and IP addresses
        """
        self.logger.info("cleanup: Removing all unused NICs/Volumes/IPs in resource group")
        self.remove_all_unused_nics()
        self.remove_all_unused_volumes()
        self.remove_all_unused_ips()

    def list_resources(self, query="", view="") -> list[ResourceExplorerResource]:
        """
        Lists resources using AWS Resource Explorer (resource-explorer-2).

        Args:
            query: keywords and filters for resources; default is "" (all)
            view: arn of the view to use for the query; default is "" (default view)

        Return:
            a list of resources satisfying the query

        Examples:
            Use query "tag.key:kubernetes.io/cluster/*" to list OCP resources
        """
        args = {"QueryString": query}
        if view:
            args["ViewArn"] = view
        list = []
        paginator = self.resource_explorer_connection.get_paginator("search")
        page_iterator = paginator.paginate(**args)
        for page in page_iterator:
            resources = page.get("Resources")
            for r in resources:
                resource = ResourceExplorerResource(
                    arn=r.get("Arn"),
                    region=r.get("Region"),
                    service=r.get("Service"),
                    properties=r.get("Properties"),
                    resource_type=r.get("ResourceType"),
                )
                list.append(resource)
        return list
