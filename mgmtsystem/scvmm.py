# coding: utf-8
"""Backend management system classes

Used to communicate with providers without using CFME facilities
"""
import winrm
import re
from cStringIO import StringIO
from contextlib import contextmanager
from datetime import datetime
from textwrap import dedent

from lxml import etree
from wait_for import wait_for

from base import MgmtSystemAPIBase, VMInfo


class SCVMMSystem(MgmtSystemAPIBase):
    """This class is used to connect to M$ SCVMM

    It still has some drawback, the main one is that pywinrm does not support domains with simple
    auth mode so I have to do the connection manually in the script which seems to be VERY slow.
    """
    STATE_RUNNING = "Running"
    STATES_STOPPED = {"PowerOff", "Stopped"}  # TODO:  "Stopped" when using shutdown. Differ it?
    STATE_PAUSED = "Paused"
    STATES_STEADY = {STATE_RUNNING, STATE_PAUSED}
    STATES_STEADY.update(STATES_STOPPED)
    STATES_FAILED = {'Creation Failed'}

    _stats_available = {
        'num_vm': lambda self: len(self.list_vm()),
        'num_template': lambda self: len(self.list_template()),
    }

    def __init__(self, **kwargs):
        super(SCVMMSystem, self).__init__(kwargs)
        self.host = kwargs["hostname"]
        self.user = kwargs["username"]
        self.password = kwargs["password"]
        self.domain = kwargs["domain"]
        self.api = winrm.Session(self.host, auth=(self.user, self.password))

    @property
    def pre_script(self):
        """Script that ensures we can access the SCVMM.

        Without domain used in login, it is not possible to access the SCVMM environment. Therefore
        we need to create our own authentication object (PSCredential) which will provide the
        domain. Then it works. Big drawback is speed of this solution.
        """
        return dedent("""
        $secpasswd = ConvertTo-SecureString "{}" -AsPlainText -Force
        $mycreds = New-Object System.Management.Automation.PSCredential ("{}\\{}", $secpasswd)
        $scvmm_server = Get-SCVMMServer -Computername localhost -Credential $mycreds
        """.format(self.password, self.domain, self.user))

    def run_script(self, script):
        """Wrapper for running powershell scripts. Ensures the ``pre_script`` is loaded."""
        script = dedent(script)
        self.logger.debug(" Running PowerShell script:\n{}\n".format(script))
        result = self.api.run_ps("{}\n\n{}".format(self.pre_script, script))
        if result.status_code != 0:
            raise self.PowerShellScriptError("Script returned {}!: {}"
                .format(result.status_code, result.std_err))
        return result.std_out.strip()

    def _do_vm(self, vm_name, action, params=""):
        self.logger.info(" {} {} SCVMM VM `{}`".format(action, params, vm_name))
        self.run_script(
            "Get-SCVirtualMachine -Name \"{}\" -VMMServer $scvmm_server | {}-SCVirtualMachine {}"
            .format(vm_name, action, params).strip())

    def start_vm(self, vm_name, force_start=False):
        """Start or resume virtual machine.

        Args:
            vm_name: Name of the virtual machine
            force_start: If we want to use the Start specifically and not Resume
        """
        if not force_start and self.is_vm_suspended(vm_name):
            # Resume
            self._do_vm(vm_name, "Resume")
        else:
            # Ordinary start
            self._do_vm(vm_name, "Start")

    def wait_vm_running(self, vm_name, num_sec=300):
        wait_for(
            lambda: self.is_vm_running(vm_name),
            message="SCVMM VM {} be running.".format(vm_name),
            num_sec=num_sec)

    def stop_vm(self, vm_name, shutdown=False):
        self._do_vm(vm_name, "Stop", "-Force" if not shutdown else "")

    def wait_vm_stopped(self, vm_name, num_sec=300):
        wait_for(
            lambda: self.is_vm_stopped(vm_name),
            message="SCVMM VM {} be stopped.".format(vm_name),
            num_sec=num_sec)

    def create_vm(self, vm_name):
        raise NotImplementedError('create_vm not implemented.')

    def rename_vm(self, vm_name, vm_new_name):
        if not self.is_vm_stopped(vm_name) and self.vm_status(vm_name) not in self.STATES_FAILED:
            self.stop_vm(vm_name)
            self.wait_vm_stopped(vm_name)
        script = """
        Get-SCVirtualMachine -Name "{vm_name}" | Set-SCVirtualMachine -Name "{vm_new_name}"
        """.format(vm_name=vm_name, vm_new_name=vm_new_name)
        self.logger.info(" Renaming SCVMM VM `{}` to  `{}`"
            .format(vm_name, vm_new_name))
        self.run_script(script)

    def delete_vm(self, vm_name):
        try:
            if not self.is_vm_stopped(vm_name) and self.vm_status(
                    vm_name) not in self.STATES_FAILED:
                self.stop_vm(vm_name)
                self.wait_vm_stopped(vm_name)
        except Exception as e:
            self.logger.exception(e)
        finally:
            script = """
            $VM = Get-SCVirtualMachine -Name \"{vm_name}\" -VMMServer $scvmm_server
            Remove-SCVirtualMachine -VM $VM -Force
            """.format(vm_name=vm_name)
            self.logger.info(" Deleting SCVMM VM `{}`".format(vm_name))
            self.run_script(script)

    def delete_template(self, template):
        script = """
        $Template = Get-SCVMTemplate -Name \"{}\" -VMMServer $scvmm_server
        Remove-SCVMTemplate -VMTemplate $Template -Force
        """.format(template)
        self.logger.info(" Removing SCVMM VM `{}`".format(template))
        self.run_script(script)

    def restart_vm(self, vm_name):
        self._do_vm(vm_name, "Reset")

    def list_vm(self, **kwargs):
        data = self.run_script(
            "Get-SCVirtualMachine -All -VMMServer $scvmm_server |"
            "Select name | ConvertTo-Xml -as String")
        return etree.parse(StringIO(data)).getroot().xpath("./Object/Property[@Name='Name']/text()")

    def all_vms(self, **kwargs):
        vm_list = []
        data = self.run_script("""
            $outputCollection = @()
            $VMs = Get-SCVirtualMachine -All -VMMServer $scvmm_server |
            Select VMId, Name, StatusString
            $NetAdapter = Get-SCVirtualNetworkAdapter -VMMServer $scvmm_server -All |
            Select ID, Name, IPv4Addresses
            #Associate objects
            $VMs | ForEach-Object{
                $vm_object = $_
                $ip_object = $NetAdapter | Where-Object {$_.Name -eq $vm_object.Name}

                #Make a combined object
                $outObj = "" | Select VMId, Name, Status, IPv4
                $outObj.VMId = if($vm_object.VMId){$vm_object.VMId} else {"None"}
                $outObj.Name = $vm_object.Name
                $outObj.Status = $vm_object.StatusString
                $outObj.IPv4 = if($ip_object.IPv4Addresses){$ip_object.IPv4Addresses} else {"None"}
                #Add the object to the collection

                $outputCollection += $outObj
            }
            $outputCollection | ConvertTo-Xml -as String
            """)
        vms = etree.parse(StringIO(data)).getroot()
        for vm in vms:
            VMId = vm.xpath("./Property[@Name='VMId']/text()")[0],
            Name = vm.xpath("./Property[@Name='Name']/text()")[0],
            Status = vm.xpath("./Property[@Name='Status']/text()")[0],
            IPv4 = vm.xpath("./Property[@Name='IPv4']/text()")[0]
            vm_data = (
                None if VMId == 'None' else VMId[0],
                Name[0],
                Status[0],
                None if IPv4 == 'None' else IPv4)
            vm_list.append(VMInfo(*vm_data))
        return vm_list

    def list_template(self):
        data = self.run_script(
            "Get-SCVMTemplate -VMMServer $scvmm_server | Select name | ConvertTo-Xml -as String")
        return etree.parse(StringIO(data)).getroot().xpath("./Object/Property[@Name='Name']/text()")

    def list_flavor(self):
        raise NotImplementedError('list_flavor not implemented.')

    def list_network(self):
        data = self.run_script(
            "Get-SCLogicalNetwork -VMMServer $scvmm_server | ConvertTo-Xml -as String")
        return etree.parse(StringIO(data)).getroot().xpath(
            "./Object/Property[@Name='Name']/text()")

    def vm_creation_time(self, vm_name):
        xml = self.run_script(
            "Get-SCVirtualMachine -Name \"{}\""
            " -VMMServer $scvmm_server | ConvertTo-Xml -as String".format(vm_name))
        date_time = etree.parse(StringIO(xml)).getroot().xpath(
            "./Object/Property[@Name='CreationTime']/text()")[0]
        return datetime.strptime(date_time, "%m/%d/%Y %I:%M:%S %p")

    def info(self, vm_name):
        pass

    def disconnect(self):
        pass

    def vm_status(self, vm_name):
        data = self.run_script(
            "Get-SCVirtualMachine -Name \"{}\" -VMMServer $scvmm_server | ConvertTo-Xml -as String"
            .format(vm_name))
        return etree.parse(StringIO(data)).getroot().xpath(
            "./Object/Property[@Name='StatusString']/text()")[0]

    def is_vm_running(self, vm_name):
        return self.vm_status(vm_name) == self.STATE_RUNNING

    def is_vm_stopped(self, vm_name):
        return self.vm_status(vm_name) in self.STATES_STOPPED

    def is_vm_suspended(self, vm_name):
        return self.vm_status(vm_name) == self.STATE_PAUSED

    def in_steady_state(self, vm_name):
        return self.vm_status(vm_name) in self.STATES_STEADY

    def suspend_vm(self, vm_name):
        self._do_vm(vm_name, "Suspend")

    def wait_vm_suspended(self, vm_name, num_sec=300):
        wait_for(
            lambda: self.is_vm_suspended(vm_name),
            message="SCVMM VM {} suspended.".format(vm_name),

            num_sec=num_sec)

    def clone_vm(self, vm_source, vm_host, path, vm_name):
        script = """
        $vm_new = Get-SCVirtualMachine -Name "{vm_source}" -VMMServer $scvmm_server
        $vm_host = Get-SCVMHost -VMMServer $scvmm_server -ComputerName "{vm_host}"
        New-SCVirtualMachine -Name "{vm_name}" -VM $vm_new -VMHost $vm_host -Path "{path}" -StartVM
        """.format(vm_name=vm_name, vm_source=vm_source, vm_host=vm_host, path=path)
        self.logger.info(" Deploying SCVMM VM `{}` from Clone of `{}`"
            .format(vm_name, vm_source))
        self.run_script(script)

    def does_vm_exist(self, vm_name):
        result = self.run_script("Get-SCVirtualMachine -Name \"{}\" -VMMServer $scvmm_server"
            .format(vm_name)).strip()
        return len(result) > 0

    def does_template_exist(self, template):
        result = self.run_script("Get-SCVMTemplate -Name \"{}\" -VMMServer $scvmm_server"
            .format(template)).strip()
        return len(result) > 0

    def deploy_template(self, template, host_group, vm_name=None, **kwargs):
        timeout = kwargs.pop('timeout', 900)
        script = """
        $tpl = Get-SCVMTemplate -Name "{template}" -VMMServer $scvmm_server
        $vm_host_group = Get-SCVMHostGroup -Name "{host_group}" -VMMServer $scvmm_server
        $vmc = New-SCVMConfiguration -VMTemplate $tpl -Name "{vm_name}" -VMHostGroup $vm_host_group
        Update-SCVMConfiguration -VMConfiguration $vmc
        New-SCVirtualMachine -Name "{vm_name}" -VMConfiguration $vmc
        """.format(template=template, vm_name=vm_name, host_group=host_group)
        self.logger.info(" Deploying SCVMM VM `{}` from template `{}` on host group `{}`"
            .format(vm_name, template, host_group))
        self.run_script(script)
        self.enable_virtual_services(vm_name)
        self.start_vm(vm_name)
        self.wait_vm_running(vm_name, num_sec=timeout)
        self.update_scvmm_virtualmachine(vm_name)
        return vm_name

    def enable_virtual_services(self, vm_name):
        # Make sure you double bracket any Invoke_Command calls.
        script = """
        $vm = Get-SCVirtualMachine -Name \"{vm_name}\"
        $secpswd = ConvertTo-SecureString "{password}" -AsPlainText -Force
        $mycreds = New-Object System.Management.Automation.PSCredential("LOCAL\\{user}", $secpswd)
        Invoke-Command -ComputerName $vm.HostName -Credential $mycreds -ScriptBlock {{
             Enable-VMIntegrationService -Name 'Guest Service Interface' -VMName \"{vm_name}\" }}
        Read-SCVirtualMachine -VM $vm
         """.format(user=self.user, password=self.password, vm_name=vm_name)
        self.run_script(script)

    def update_scvmm_virtualmachine(self, vm_name):
        # This forces SCVMM to update a VM that was changed directly in Hyper-V using Invoke-Command
        script = """
        $vm = Get-SCVirtualMachine -Name \"{vm_name}\"
        Read-SCVirtualMachine -VM $vm
         """.format(vm_name=vm_name)
        self.logger.info("Updating SCVMM VM \"{vm_name}\" using Read-SCVirtualMachine"
            .format(vm_name=vm_name))
        self.run_script(script)

    def mark_as_template(self, vm_name, library, library_share):
        # Converts an existing VM into a template.  VM no longer exists afterwards.
        script = """
        $VM = Get-SCVirtualMachine -Name \"{vm_name}\" -VMMServer $scvmm_server
        New-SCVMTemplate -Name \"{vm_name}\" -VM $VM -LibraryServer \"{ls}\" -SharePath \"{lp}\"
         """.format(vm_name=vm_name, ls=library, lp=library_share)
        self.logger.info("Creating SCVMM Template `{vm_name}` from VM `{vm_name}`-tpl"
            .format(vm_name=vm_name))
        self.run_script(script)

    @contextmanager
    def with_vm(self, *args, **kwargs):
        """Context manager for better cleanup"""
        name = self.deploy_template(*args, **kwargs)
        yield name
        self.delete_vm(name)

    def current_ip_address(self, vm_name):
        data = self.run_script(
            "Get-SCVirtualMachine -Name \"{}\" -VMMServer $scvmm_server |"
            "Get-SCVirtualNetworkAdapter | Select IPv4Addresses |"
            "ft -HideTableHeaders".format(vm_name))
        return data.translate(None, '{}')

    def get_ip_address(self, vm_name, **kwargs):
        # Forcing an update to account for any delayed status changes
        self.update_scvmm_virtualmachine(vm_name)
        if not re.findall(r'[0-9]+(?:.[0-9]+){3}', self.current_ip_address(vm_name)):
            return None
        return self.current_ip_address(vm_name)

    def remove_host_from_cluster(self, hostname):
        """I did not notice any scriptlet that lets you do this."""

    def disconnect_dvd_drives(self, vm_name):
        number_dvds_disconnected = 0
        script = """\
        $VM = Get-SCVirtualMachine -Name "{}"
        $DVDDrive = Get-SCVirtualDVDDrive -VM $VM
        $DVDDrive[0] | Remove-SCVirtualDVDDrive
        """.format(vm_name)
        while self.data(vm_name).VirtualDVDDrives is not None:
            self.run_script(script)
            number_dvds_disconnected += 1
        return number_dvds_disconnected

    def data(self, vm_name):
        """Returns detailed informations about SCVMM VM"""
        data = self.run_script(
            "Get-SCVirtualMachine -Name \"{}\" -VMMServer $scvmm_server | ConvertTo-Xml -as String"
            .format(vm_name))
        return self.SCVMMDataHolderDict(etree.parse(StringIO(data)).getroot().xpath("./Object")[0])

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

    class SCVMMDataHolderDict(object):
        def __init__(self, data):
            for prop in data.xpath("./Property"):
                name = prop.attrib["Name"]
                t = prop.attrib["Type"]
                children = prop.getchildren()
                if children:
                    if prop.xpath("./Property[@Name]"):
                        self.__dict__[name] = SCVMMSystem.SCVMMDataHolderDict(prop)
                    else:
                        self.__dict__[name] = SCVMMSystem.SCVMMDataHolderList(prop)
                else:
                    data = prop.text
                    result = SCVMMSystem.parse_data(t, prop.text)
                    self.__dict__[name] = result

        def __repr__(self):
            return repr(self.__dict__)

    class SCVMMDataHolderList(list):
        def __init__(self, data):
            super(SCVMMSystem.SCVMMDataHolderList, self).__init__()
            for prop in data.xpath("./Property"):
                t = prop.attrib["Type"]
                data = prop.text
                result = SCVMMSystem.parse_data(t, prop.text)
                self.append(result)

    class PowerShellScriptError(Exception):
        pass
