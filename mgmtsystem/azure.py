# coding: utf-8
"""Backend management system classes

Used to communicate with providers without using CFME facilities
"""
from datetime import datetime
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
    STATE_SUSPEND = "VM stopped"
    STATE_PAUSED = "Paused"
    STATES_STEADY = {STATE_RUNNING, STATE_PAUSED}
    STATES_STEADY.update(STATE_STOPPED)

    _stats_available = {
        'num_vm': lambda self: len(self.list_vm()),
        'num_template': lambda self: len(self.list_template()),
    }

    def __init__(self, **kwargs):
        super(AzureSystem, self).__init__(kwargs)
        self.host = kwargs["powershell_host"]
        self.provisioning = kwargs['provisioning']
        self.resource_group = kwargs['provisioning']['resource_group']
        self.storage_blob = kwargs['provisioning']['storage_blob']
        self.username = kwargs["username"]
        self.password = kwargs["password"]
        self.ui_username = kwargs["ui_username"]
        self.ui_password = kwargs["ui_password"]
        self.ps_username = kwargs["powershell_username"]
        self.ps_password = kwargs["powershell_password"]
        self.storage_account = kwargs["storage_account"]
        self.storage_key = kwargs["storage_key"]
        self.subscription_id = kwargs["subscription_id"]
        self.tenant_id = kwargs["tenant_id"]
        self.api = winrm.Session(self.host, auth=(self.ps_username, self.ps_password))

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
        """.format(self.ui_username, self.ui_password, self.subscription_id, self.tenant_id))

    def run_script(self, script, ignore_error=False):
        """Wrapper for running powershell scripts. Ensures the ``pre_script`` is loaded."""
        script = dedent(script)
        self.logger.info(" Running PowerShell script:\n{}\n{}".format(self.pre_script, script))
        result = self.api.run_ps("{}\n\n{}".format(self.pre_script, script))
        if result.status_code != 0 and not ignore_error:
            raise self.PowerShellScriptError("Script returned {}!: {}"
                .format(result.status_code, result.std_err))
        self.logger.info("run_script Script Complete")
        return result.std_out.strip()

    def start_vm(self, vm_name, resource_group=None):
        if self.is_vm_stopped(vm_name, resource_group or self.resource_group):
            self.logger.info("Attempting to Start Azure VM {}".format(vm_name))
            self.run_script(
                "Start-AzureRmVm -ResourceGroup \"{}\" -Name \"{}\""
                .format(resource_group or self.resource_group, vm_name).strip())
            self.wait_vm_running(vm_name, resource_group)
        else:
            self.logger.info("Azure VM {} is already running".format(vm_name))

    def restart_vm(self, vm_name, resource_group=None):
        self.logger.info("Attempting to Restart Azure VM {}".format(vm_name))
        self.run_script(
            "Restart-AzureRmVm -ResourceGroup \"{}\" -Name \"{}\""
            .format(resource_group or self.resource_group, vm_name).strip())

    def stop_vm(self, vm_name, resource_group=None, shutdown=False):
        if self.is_vm_running(vm_name, resource_group or self.resource_group):
            self.logger.info("Attempting to Stop Azure VM {}".format(vm_name))
            self.run_script(
                "Stop-AzureRmVm -ResourceGroup \"{}\" -Name \"{}\" -Force"
                .format(resource_group or self.resource_group, vm_name).strip())
            self.wait_vm_stopped(vm_name, resource_group)
        else:
            self.logger.info("Azure VM {} is already stopped".format(vm_name))

    def suspend_vm(self, vm_name, resource_group=None):
        if self.is_vm_running(vm_name, resource_group or self.resource_group):
            self.logger.info("Attempting to Suspend Azure VM {}".format(vm_name))
            self.run_script(
                "Stop-AzureRmVm -ResourceGroup \"{}\" -Name \"{}\" -Force -StayProvisioned"
                .format(resource_group or self.resource_group, vm_name).strip())
            self.wait_vm_suspended(vm_name, resource_group)
        else:
            self.logger.info("Azure VM {} is already suspended or stopped".format(vm_name))

    def wait_vm_running(self, vm_name, resource_group=None, num_sec=300):
        wait_for(
            lambda: self.is_vm_running(vm_name, resource_group or self.resource_group),
            message="Waiting for Azure VM {} to be running.".format(vm_name),
            num_sec=num_sec)

    def wait_vm_steady(self, vm_name, resource_group=None, num_sec=300):
        self.logger.info("All states are steady in Azure. {}".format(vm_name))
        return True

    def wait_vm_stopped(self, vm_name, resource_group=None, num_sec=300):
        wait_for(
            lambda: self.is_vm_stopped(vm_name, resource_group or self.resource_group),
            message="Waiting for Azure VM {} to be stopped.".format(vm_name),
            num_sec=num_sec)

    def wait_vm_suspended(self, vm_name, resource_group=None, num_sec=300):
        wait_for(
            lambda: self.is_vm_suspended(vm_name, resource_group or self.resource_group),
            message="Waiting for Azure VM {} to be suspended.".format(vm_name),
            num_sec=num_sec)

    def create_vm(self, vm_name, resource_group=None):
        raise NotImplementedError('NIE - create_vm not implemented.')

    def delete_vm(self, vm_name, resource_group=None):
        vhd_endpoint = self.get_vm_vhd(vm_name, resource_group or self.resource_group)
        self.logger.info("Begin delete_vm {}".format(vm_name))
        self.run_script(
            """
            Invoke-Command -scriptblock {{
            Remove-AzureRmVM -ResourceGroupName \"{rg}\" -Name \"{vm}\" -Force
            Remove-AzureRmNetworkInterface -Name \"{vm}\" -ResourceGroupName \"{rg}\" -Force
            Remove-AzureRmPublicIpAddress -Name \"{vm}\" -ResourceGroupName \"{rg}\" -Force
            }}
            """.format(rg=resource_group or self.resource_group, vm=vm_name), True)
        self.remove_blob_image(vhd_endpoint)

    def list_vm(self):
        self.logger.info("Attempting to List Azure VMs")
        azure_data = self.run_script("Get-AzureRmVm | convertto-xml -as String")
        data = self.clean_azure_xml(azure_data)
        nameList = etree.parse(StringIO(data)).getroot().xpath(
            "./Object/Property[@Name='Name']/text()")
        return nameList

    def list_stack(self, resource_group=None):
        self.logger.info("Attempting to List Azure Orchestration Deployment Stacks")
        azure_data = self.run_script(
            """
            Invoke-Command -scriptblock {{
            Get-AzureRmResourceGroupDeployment -ResourceGroupName \"{rg}\" |
            convertto-xml -as String;
            }}
            """.format(rg=resource_group or self.resource_group))
        data = self.clean_azure_xml(azure_data)
        name_list = etree.parse(StringIO(data)).getroot().xpath(
            "./Object/Property[@Name='DeploymentName']/text()")
        return name_list

    def list_template(self):
        self.logger.info("Attempting to List Azure VHDs in templates directory")
        azure_data = self.run_script(
            """
            Invoke-Command -scriptblock {{
            $myStorage = New-AzureStorageContext -StorageAccountName \"{}\" |
            -StorageAccountKey '\"{}\"';
            Get-AzureStorageBlob -Container templates -Context $myStorage | Select Name;
            }}
            """.format(self.storage_account, self.storage_key))
        lines = iter(azure_data.splitlines())
        templates = []
        for line in lines:
            if ".vhd" in line:
                vhd = line.split(" ")
                templates.append(str(vhd[2])[:-4])
        templates.append(str('Microsoft.Compute/Images/templates/tmpl-osDisk'))
        return templates

    def list_flavor(self):
        raise NotImplementedError('list_flavor not implemented.')

    def list_network(self):
        raise NotImplementedError('list_network not implemented.')

    def vm_creation_time(self, vm_name, resource_group=None):
        # There is no such parameter as vm creation time.  Using VHD date instead.
        self.logger.info("Attempting to Retrieve Azure VM Modification Time {}".format(vm_name))
        vm_vhd = self.get_vm_vhd(vm_name, resource_group or self.resource_group)
        vhd_name = os.path.split(urlparse.urlparse(vm_vhd).path)[1]
        data = self.run_script(
            """
            Invoke-Command -scriptblock {{
            $storageContext = New-AzureStorageContext -StorageAccountName \"{storage_account}\" `
                            -StorageAccountKey \"{storage_key}\"
            Get-AzureStorageBlob -Name \"{storage_blob}\" `
                            -Context $storageContext -Blob \"{vhd_blob}\" | convertto-xml -as String
            }}
            """.format(storage_account=self.storage_account,
                       storage_blob=self.storage_blob,
                       storage_key=self.storage_key,
                       vhd_blob=vhd_name), True)
        vhd_last_modified = etree.parse(StringIO(self.clean_azure_xml(data))).getroot().xpath(
            "./Object/Property[@Name='LastModified']/text()")
        create_time = datetime.strptime(str(vhd_last_modified), '[\'%m/%d/%Y %H:%M:%S %p +00:00\']')
        self.logger.info("VM last edit time based on vhd =  {}".format(str(create_time)))
        return create_time

    def info(self, vm_name):
        pass

    def disconnect(self):
        pass

    def clean_azure_xml(self, azure_xml_data):
        # Azure prepends a non-XML header to returned xml.  This strips that header.
        sep = '<?xml'
        clean_xml = sep + azure_xml_data.split(sep, 1)[1]
        return clean_xml

    def vm_status(self, vm_name, resource_group=None):
        self.logger.info("Attempting to Retrieve Azure VM Status {}".format(vm_name))
        azure_data = self.run_script(
            "Get-AzureRmVm -ResourceGroup \"{}\" -Name \"{}\" -Status | convertto-xml -as String"
            .format(resource_group or self.resource_group, vm_name))
        data = self.clean_azure_xml(azure_data)
        statusValue = json.loads(etree.parse(StringIO(data)).getroot().xpath(
            "./Object/Property[@Name='StatusesText']/text()")[0])
        powerStatus = statusValue[1]
        powerDisplayStatus = powerStatus['DisplayStatus']
        self.logger.info("Returned Status was {}".format(powerDisplayStatus))
        return powerDisplayStatus

    def is_vm_running(self, vm_name, resource_group=None):
        if self.vm_status(vm_name, resource_group or self.resource_group) == self.STATE_RUNNING:
            self.logger.info("According to Azure, the VM \"{}\" is running".format(vm_name))
            return True
        else:
            return False

    def is_vm_stopped(self, vm_name, resource_group=None):
        if self.vm_status(vm_name, resource_group or self.resource_group) == self.STATE_STOPPED:
            self.logger.info("According to Azure, the VM \"{}\" is stopped".format(vm_name))
            return True
        else:
            return False

    def is_vm_starting(self, vm_name, resource_group=None):
        if self.vm_status(vm_name, resource_group or self.resource_group) == self.STATE_STARTING:
            self.logger.info("According to Azure, the VM \"{}\" is starting".format(vm_name))
            return True
        else:
            return False

    def is_vm_suspended(self, vm_name, resource_group=None):
        if self.vm_status(vm_name, resource_group or self.resource_group) == self.STATE_SUSPEND:
            self.logger.info("According to Azure, the VM \"{}\" is suspended".format(vm_name))
            return True
        else:
            return False

    def in_steady_state(self, vm_name, resource_group=None):
        return self.vm_status(vm_name, resource_group or self.resource_group) in self.STATES_STEADY

    def clone_vm(self, source_name, vm_name):
        """It wants exact host and placement (c:/asdf/ghjk) :("""
        raise NotImplementedError('NIE - clone_vm not implemented.')

    def does_vm_exist(self, vm_name):
        return vm_name in self.list_vm()

    def stack_exist(self, stack_name):
        return stack_name in self.list_stack()

    def delete_stack(self, stack_name, resource_group=None):
        self.logger.info("Removes a Deployment Stack resource created with Orchestration")
        self.run_script(
            """
            Invoke-Command -scriptblock {{
            Remove-AzureRmResourceGroupDeployment -ResourceGroupName \"{rg}\" `
            -DeploymentName \"{stack}\" -Force
            }}
            """.format(rg=resource_group or self.resource_group, stack=stack_name))
        return True

    def deploy_template(self, template, vm_name=None, **vm_settings):
        self.copy_blob_image(template, vm_name, vm_settings['storage_account'],
            vm_settings['template_blob'], vm_settings['storage_blob'])
        self.run_script(
            """
            Invoke-Command -scriptblock {{
            $StorageAccount = Get-AzureRmStorageAccount -ResourceGroupName \"{resource_group}\" `
                 -Name \"{storage_account}\"
            $NetworkSecurityGroupID = Get-AzureRmNetworkSecurityGroup -Name \"{network_nsg}\" `
                -ResourceGroupName \"{resource_group}\"
            $PIp = New-AzureRmPublicIpAddress -Name \"{vm_name}\" -ResourceGroupName `
                \"{resource_group}\" -Location \"{region}\" -AllocationMethod Dynamic -Force
            $SubnetConfig = New-AzureRmVirtualNetworkSubnetConfig -Name "default" `
                -AddressPrefix \"{subnet_range}\"
            $VNet = New-AzureRmVirtualNetwork -Name \"{virtual_net}\" -ResourceGroupName `
                \"{resource_group}\" -Location \"{region}\" -AddressPrefix \"{address_space}\" `
                -Subnet $SubnetConfig -Force
            $Interface = New-AzureRmNetworkInterface -Name \"{vm_name}\" -ResourceGroupName `
                \"{resource_group}\" -Location \"{region}\" -SubnetId $VNet.Subnets[0].Id `
                -PublicIpAddressId $PIp.Id -Force
            $VirtualMachine = New-AzureRmVMConfig -VMName \"{vm_name}\" -VMSize \"{vm_size}\"
            $VirtualMachine = Add-AzureRmVMNetworkInterface -VM $VirtualMachine -Id $Interface.Id
            $OSDiskUri = $StorageAccount.PrimaryEndpoints.Blob.ToString() + \"{storage_blob}\" `
                + "/" + \"{vm_name}.vhd\"
            $VirtualMachine = Set-AzureRmVMOSDisk -VM $VirtualMachine -Name \"{vm_name}\" `
                -VhdUri $OSDiskUri -CreateOption attach -Linux
            New-AzureRmVM -ResourceGroupName \"{resource_group}\" -Location \"{region}\" `
                -VM $VirtualMachine
            }}
            """.format(source_name=template.split("/")[-1],
                       vm_name=vm_name,
                       resource_group=vm_settings['resource_group'],
                       virtual_net=vm_settings['virtual_net'],
                       address_space=vm_settings['address_space'],
                       subnet_range=vm_settings['subnet_range'],
                       network_nsg=vm_settings['network_nsg'],
                       region=vm_settings['region_api'],
                       vm_size=vm_settings['vm_size'],
                       av_set=vm_settings['av_set'],
                       storage_account=vm_settings['storage_account'],
                       storage_blob=vm_settings['storage_blob']))
        self.wait_vm_running(vm_name, vm_settings['resource_group'])

    def copy_blob_image(self, template, vm_name, storage_account, template_blob, storage_blob):
        self.run_script(
            """
            Invoke-Command -scriptblock {{
            $sourceContext = New-AzureStorageContext -StorageAccountName \"{storage_account}\" `
                            -StorageAccountKey \"{storage_key}\"
            $destContext = New-AzureStorageContext -StorageAccountName \"{storage_account}\" `
                            -StorageAccountKey \"{storage_key}\"
            $blobCopy = Start-AzureStorageBlobCopy -DestContainer \"{storage_blob}\" `
                        -DestContext $destContext -DestBlob \"{vm_name}.vhd\" `
                        -SrcBlob \"{source_name}.vhd\" `
                        -Context $sourceContext -SrcContainer \"{template_blob}\"
            }}
            """.format(source_name=template.split("/")[-1],
                       vm_name=vm_name,
                       storage_account=storage_account,
                       storage_key=self.storage_key,
                       template_blob=template_blob,
                       storage_blob=storage_blob))

    def remove_blob_image(self, vm_vhd):
        vhd_name = os.path.split(urlparse.urlparse(vm_vhd).path)[1]
        self.run_script(
            """
            Invoke-Command -scriptblock {{
            $sourceContext = New-AzureStorageContext -StorageAccountName \"{storage_account}\" `
                            -StorageAccountKey \"{storage_key}\"
            $blobRemove = Remove-AzureStorageBlob -Blob \"{vhd_name}\" `
                        -Context $sourceContext -Container \"{storage_blob}\"
            }}
            """.format(vhd_name=vhd_name,
                       storage_account=self.storage_account,
                       storage_key=self.storage_key,
                       storage_blob=self.storage_blob), True)

    def remove_diags_container(self):
        self.run_script(
            """
            Invoke-Command -scriptblock {{
            $storeContext = New-AzureStorageContext -StorageAccountName \"{storage_account}\" `
                            -StorageAccountKey \"{storage_key}\"
            $storageContainer = Get-AzureStorageContainer -Context $storeContext
            foreach ($container in $storageContainer) {{
                if ($container.name -like 'bootdiagnostics-test*'){{
                    Remove-AzureStorageContainer -Name $container.name -Context $storeContext -Force
                }}
            }}
            }}
            """.format(storage_account=self.storage_account,
                       storage_key=self.storage_key), True)

    @contextmanager
    def with_vm(self, *args, **kwargs):
        """Context manager for better cleanup"""
        name = self.deploy_template(*args, **kwargs)
        yield name
        self.delete_vm(name)

    def current_ip_address(self, vm_name, resource_group=None):
        # Returns first active IPv4 IpAddress only
        azure_data = self.run_script(
            "Get-AzureRmPublicIpAddress -ResourceGroup \"{}\" -Name \"{}\" |"
            "convertto-xml -as String".format(resource_group or self.resource_group, vm_name))
        data = self.clean_azure_xml(azure_data)
        return etree.parse(StringIO(data)).getroot().xpath(
            "./Object/Property[@Name='IpAddress']/text()")

    def get_ip_address(self, vm_name, resource_group=None, **kwargs):
        current_ip_address = self.current_ip_address(vm_name, resource_group or self.resource_group)
        return current_ip_address

    def get_vm_vhd(self, vm_name, resource_group=None):
        self.logger.info("get_vm_vhd - Attempting to Retrieve Azure VM VHD {}".format(vm_name))
        azure_data = self.run_script(
            "Get-AzureRmVm -ResourceGroup \"{}\" -Name \"{}\" | convertto-xml -as String"
            .format(resource_group or self.resource_group, vm_name))
        data = self.clean_azure_xml(azure_data)
        vhd_value = json.loads(etree.parse(StringIO(data)).getroot().xpath(
            "./Object/Property[@Name='StorageProfileText']/text()")[0])
        vhd_endpoint = vhd_value['OSDisk']['VirtualHardDisk']['Uri']
        self.logger.info("Returned Disk Endpoint was {}".format(vhd_endpoint))
        return vhd_endpoint

    def get_network_interface(self, vm_name, resource_group=None):
        self.logger.info("Attempting to Retrieve Azure VM Network Interface {}".format(vm_name))
        azure_data = self.run_script(
            "Get-AzureRmVm -ResourceGroup \"{}\" -Name \"{}\" | convertto-xml -as String"
            .format(resource_group or self.resource_group, vm_name))
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

    def data(self, vm_name, resource_group=None):
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
