# coding: utf-8
"""Backend management system classes

Used to communicate with providers without using CFME facilities
"""
import winrm
import json
import urlparse
import os
from cStringIO import StringIO
from contextlib import contextmanager
from textwrap import dedent

from lxml import etree
from wait_for import wait_for

from base import MgmtSystemAPIBase


class AzureSystem(MgmtSystemAPIBase):
    """This class is used to connect to Microsoft Azure Portal via PowerShell AzureRM Module
    """
    STATE_RUNNING = "VM running"
    STATE_STOPPED = "VM deallocated"
    STATE_STARTING = "VM starting"
    STATE_PAUSED = "Paused"
    STATES_STEADY = {STATE_RUNNING, STATE_PAUSED}
    STATES_STEADY.update(STATE_STOPPED)

    _stats_available = {
        'num_vm': lambda self: len(self.list_vm()),
        'num_template': lambda self: len(self.list_template()),
    }

    def __init__(self, **kwargs):
        super(AzureSystem, self).__init__(kwargs)
        self.host = kwargs["hostname"]
        self.user = kwargs["username"]
        self.password = kwargs["password"]
        self.user_azure = kwargs["username_azure"]
        self.password_azure = kwargs["password_azure"]
        self.storage_key = kwargs["storage_key"]
        self.subscription_id = kwargs["subscription_id"]
        self.tenant_id = kwargs["tenant_id"]
        self.api = winrm.Session(self.host, auth=(self.user, self.password))

    @property
    def pre_script(self):
        """Script that ensures we can access the Azure Portal Resource Manager.

        SCVMM2 will be used as the PowerShell Host for accessing Azure API
        but any windows machine or VM with AzureRm Powershell module can be used.
        """
        return dedent("""
        $myazurename = "{}"
        $myazurepwd = ConvertTo-SecureString "{}" -AsPlainText -Force
        $azcreds = New-Object System.Management.Automation.PSCredential ($myazurename, $myazurepwd)
        Login-AzureRMAccount -Credential $azcreds
        Get-AzureRmSubscription -SubscriptionId \"{}\" -TenantId \"{}\" | Select-AzureRmSubscription
        """.format(self.user_azure, self.password_azure, self.subscription_id, self.tenant_id))

    def run_script(self, script):
        """Wrapper for running powershell scripts. Ensures the ``pre_script`` is loaded."""
        script = dedent(script)
        self.logger.info(" Running PowerShell script:\n{}\n".format(script))
        result = self.api.run_ps("{}\n\n{}".format(self.pre_script, script))
        if result.status_code != 0:
            raise self.PowerShellScriptError("Script returned {}!: {}"
                .format(result.status_code, result.std_err))
        self.logger.info("PowerShell Returned:\n{}\n".format(result.std_out.strip()))
        return result.std_out.strip()

    def start_vm(self, vm_name, resource_group):
        if self.is_vm_stopped(vm_name, resource_group):
            self.logger.info("Attempting to Start Azure VM {}".format(vm_name))
            self.run_script(
                "Start-AzureRmVm -ResourceGroup \"{}\" -Name \"{}\""
                .format(resource_group, vm_name).strip())
        else:
            self.logger.info("Azure VM {} is already running".format(vm_name))

    def restart_vm(self, vm_name, resource_group):
        self.logger.info("Attempting to Restart Azure VM {}".format(vm_name))
        self.run_script(
            "Restart-AzureRmVm -ResourceGroup \"{}\" -Name \"{}\""
            .format(resource_group, vm_name).strip())

    def stop_vm(self, vm_name, resource_group, shutdown=False):
        if self.is_vm_running(vm_name, resource_group):
            self.logger.info("Attempting to Stop Azure VM {}".format(vm_name))
            self.run_script(
                "Stop-AzureRmVm -ResourceGroup \"{}\" -Name \"{}\" -Force"
                .format(resource_group, vm_name).strip())
        else:
            self.logger.info("Azure VM {} is already stopped".format(vm_name))

    def wait_vm_running(self, vm_name, resource_group, num_sec=300):
        wait_for(
            lambda: self.is_vm_running(vm_name),
            message="Waiting for Azure VM {} to be running.".format(vm_name),
            num_sec=num_sec)

    def wait_vm_stopped(self, vm_name, resource_group, num_sec=300):
        wait_for(
            lambda: self.is_vm_stopped(vm_name),
            message="Waiting for Azure VM {} to be stopped.".format(vm_name),
            num_sec=num_sec)

    def create_vm(self, vm_name, resource_group):
        raise NotImplementedError('create_vm not implemented.')

    def delete_vm(self, vm_name, resource_group):
        raise NotImplementedError('delte_vm not implemented.')

    def list_vm(self):
        self.logger.info("Attempting to List Azure VMs")
        azure_data = self.run_script("Get-AzureRmVm | convertto-xml -as String")
        data = self.clean_azure_xml(azure_data)
        nameList = etree.parse(StringIO(data)).getroot().xpath(
            "./Object/Property[@Name='Name']/text()")
        return nameList

    def list_template(self):
        self.logger.info("Attempting to List Azure VHDs in templates directory")
        script = "$myStorage = New-AzureStorageContext -StorageAccountName cfmeqe "
        script += "-StorageAccountKey '" + self.storage_key + "' ;"
        script += "Get-AzureStorageBlob -Container templates -Context $myStorage | Select Name;"
        azure_data = self.run_script("Invoke-Command -scriptblock {" + script + "}")
        lines = iter(azure_data.splitlines())
        templates = []
        for line in lines:
            if ".vhd" in line:
                vhd = line.split(" ")
                templates.append(str(vhd[2])[:-4])
        return templates

    def list_flavor(self):
        raise NotImplementedError('list_flavor not implemented.')

    def list_network(self):
        raise NotImplementedError('list_network not implemented.')

    def vm_creation_time(self, vm_name):
        raise NotImplementedError('vm_creation_time not implemented.')

    def info(self, vm_name):
        pass

    def disconnect(self):
        pass

    def clean_azure_xml(self, azure_xml_data):
        # Azure prepends a non-XML header to returned xml.  This strips that header.
        sep = '<?xml'
        clean_xml = sep + azure_xml_data.split(sep, 1)[1]
        return clean_xml

    def vm_status(self, vm_name, resource_group):
        self.logger.info("Attempting to Retrieve Azure VM Status {}".format(vm_name))
        azure_data = self.run_script(
            "Get-AzureRmVm -ResourceGroup \"{}\" -Name \"{}\" -Status | convertto-xml -as String"
            .format(resource_group, vm_name))
        data = self.clean_azure_xml(azure_data)
        statusValue = json.loads(etree.parse(StringIO(data)).getroot().xpath(
            "./Object/Property[@Name='StatusesText']/text()")[0])
        powerStatus = statusValue[1]
        powerDisplayStatus = powerStatus['DisplayStatus']
        self.logger.info("Returned Status was {}".format(powerDisplayStatus))
        return powerDisplayStatus

    def is_vm_running(self, vm_name, resource_group):
        return self.vm_status(vm_name, resource_group) == self.STATE_RUNNING

    def is_vm_stopped(self, vm_name, resource_group):
        return self.vm_status(vm_name, resource_group) == self.STATE_STOPPED

    def is_vm_starting(self, vm_name, resource_group):
        return self.vm_status(vm_name, resource_group) == self.STATE_STARTING

    def is_vm_suspended(self, vm_name, resource_group):
        return self.vm_status(vm_name, resource_group) == self.STATE_PAUSED

    def in_steady_state(self, vm_name, resource_group):
        return self.vm_status(vm_name, resource_group) in self.STATES_STEADY

    def suspend_vm(self, vm_name, resource_group):
        self._do_vm(vm_name, "Suspend")

    def wait_vm_suspended(self, vm_name, resource_group, num_sec=300):
        raise NotImplementedError('wait_vm_suspended not implemented.')

    def clone_vm(self, source_name, vm_name):
        """It wants exact host and placement (c:/asdf/ghjk) :("""
        raise NotImplementedError('clone_vm not implemented.')

    def does_vm_exist(self, vm_name):
        result = self.list_vm()
        if vm_name in result:
            return True

    def deploy_template(self, template, vm_name=None, host_group=None, **bogus):
        raise NotImplementedError('deploy_template not implemented.')

    @contextmanager
    def with_vm(self, *args, **kwargs):
        """Context manager for better cleanup"""
        name = self.deploy_template(*args, **kwargs)
        yield name
        self.delete_vm(name)

    def current_ip_address(self, vm_name, resource_group):
        azure_data = self.run_script(
            "Get-AzureRmPublicIpAddress -ResourceGroup \"{}\" -Name \"{}\" |"
            "convertto-xml -as String".format(resource_group, vm_name))
        data = self.clean_azure_xml(azure_data)
        return etree.parse(StringIO(data)).getroot().xpath(
            "./Object/Property[@Name='IpAddress']/text()")
        # TODO: Scavenge informations how these are formatted, I see no if-s in SCVMM

    def get_ip_address(self, vm_name, resource_group, **kwargs):
        current_ip_address = self.current_ip_address(vm_name, resource_group)
        return current_ip_address

    def get_vm_vhd(self, vm_name, resource_group):
        self.logger.info("Attempting to Retrieve Azure VM VHD {}".format(vm_name))
        azure_data = self.run_script(
            "Get-AzureRmVm -ResourceGroup \"{}\" -Name \"{}\" | convertto-xml -as String"
            .format(resource_group, vm_name))
        data = self.clean_azure_xml(azure_data)
        status_value = json.loads(etree.parse(StringIO(data)).getroot().xpath(
            "./Object/Property[@Name='StorageProfileText']/text()")[0])
        vhd_disk_uri = status_value['OSDisk']['VirtualHardDisk']['Uri']
        self.logger.info("Returned Status was {}".format(vhd_disk_uri))
        return os.path.split(urlparse.urlparse(vhd_disk_uri).path)[1]

    def get_network_interface(self, vm_name, resource_group):
        self.logger.info("Attempting to Retrieve Azure VM Network Interface {}".format(vm_name))
        azure_data = self.run_script(
            "Get-AzureRmVm -ResourceGroup \"{}\" -Name \"{}\" | convertto-xml -as String"
            .format(resource_group, vm_name))
        data = self.clean_azure_xml(azure_data)
        status_value = json.loads(etree.parse(StringIO(data)).getroot().xpath(
            "./Object/Property[@Name='NetworkProfileText']/text()")[0])
        nic_uri = status_value['NetworkInterfaces'][0]['ReferenceUri']
        self.logger.info("Returned URI was {}".format(nic_uri))
        return os.path.split(urlparse.urlparse(nic_uri).path)[1]

    def remove_host_from_cluster(self, hostname):
        """I did not notice any scriptlet that lets you do this."""

    def disconnect_dvd_drives(self, vm_name):
        raise NotImplementedError('disconnect_dvd_drives not implemented.')

    def data(self, vm_name, resource_group):
        raise NotImplementedError('data not implemented.')

    ##
    # Classes and functions used to access detailed SCVMM Data
    @staticmethod
    def parse_data(t, data):
        if data is None:
            return None
        elif t == "System.Boolean":
            return data.lower().strip() == "true"
        elif t.startswith("System.Int"):
            return int(data)
        elif t == "System.String" and data.lower().strip() == "none":
            return None

    class AzureDataHolderDict(object):
        def __init__(self, data):
            for prop in data.xpath("./Property"):
                name = prop.attrib["Name"]
                t = prop.attrib["Type"]
                children = prop.getchildren()
                if children:
                    if prop.xpath("./Property[@Name]"):
                        self.__dict__[name] = AzureSystem.AzureDataHolderDict(prop)
                    else:
                        self.__dict__[name] = AzureSystem.AzureDataHolderList(prop)
                else:
                    data = prop.text
                    result = AzureSystem.parse_data(t, prop.text)
                    self.__dict__[name] = result

        def __repr__(self):
            return repr(self.__dict__)

    class AzureDataHolderList(list):
        def __init__(self, data):
            super(AzureSystem.AzureDataHolderList, self).__init__()
            for prop in data.xpath("./Property"):
                t = prop.attrib["Type"]
                data = prop.text
                result = AzureSystem.parse_data(t, prop.text)
                self.append(result)

    class PowerShellScriptError(Exception):
        pass
