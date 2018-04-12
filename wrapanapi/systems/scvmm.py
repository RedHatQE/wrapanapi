# coding: utf-8
"""Backend management system classes

Used to communicate with providers without using CFME facilities
"""
from __future__ import absolute_import

import json
import re
from datetime import datetime
from textwrap import dedent

import pytz
import tzlocal
import winrm
from wrapanapi.entities import Template, TemplateMixin, Vm, VmMixin, VmState
from wrapanapi.exceptions import (ImageNotFoundError, NotFoundError,
                                  VMInstanceNotFound)
from wrapanapi.systems import System


def convert_powershell_date(date_obj_string):
    """
    Converts a string representation of a Date object into datetime

    PowerShell prints this as an msec timestamp

    So this converts to:
    "/Date(1449273876697)/" == datetime.datetime.fromtimestamp(1449273876697/1000.)
    """
    match = re.search(r'^/Date\((\d+)\)/$', date_obj_string)
    if not match:
        raise ValueError('Invalid date object string: {}'.format(date_obj_string))
    return datetime.fromtimestamp(match.groups(1)/1000.)


class SCVirtualMachine(Vm):
    def __init__(self, system, name, raw=None):
        """
        Construct an SCVirtualMachine instance tied to a specific system

        Args:
            system: instance of SCVMMSystem
            name: name of VM
            raw: raw json (as dict) for the VM returned by the API
        """
        super(SCVirtualMachine, self).__init__(system)
        # TODO: switch to using ID to track VM's instead of name?
        self._name = name
        self._raw = raw

        self._run_script = self.system.run_script
        self._get_json = self.system.get_json

    def _get_myself(self, sync=True):
        """
        Get VM from SCVMM

        Args:
            sync (bool) -- force reload from host in cases where VM was updated directly on host

        Returns:
            raw VM json
        """
        script = 'Get-SCVirtualMachine -Name \"{}\" -VMMServer $scvmm_server'
        if sync:
            script = '{} | Read-SCVirtualMachine'.format(script)
        data = self._get_json(script.format(self.name))
        if not data:
            raise VMInstanceNotFound(self.name)
        return data

    def refresh(self):
        data = self._get_myself()
        self._raw = data

    @property
    def raw(self):
        if not self._raw:
            self.refresh()
        return self._raw

    @property
    def name(self):
        return self._name

    @property
    def exists(self):
        try:
            if self._get_myself():
                return True
            return False
        except NotFoundError:
            return False

    @staticmethod
    def state_map():
        return {
            'Running': VmState.RUNNING,
            'PowerOff': VmState.STOPPED,
            'Paused': VmState.PAUSED,
            'Missing': VmState.ERROR,
            'Creation Failed': VmState.ERROR,
        }

    @property
    def state(self):
        self.refresh()
        return self.raw['StatusString']

    @property
    def id(self):
        return self.raw['ID']

    @property
    def ip(self):
        self.refresh()
        data = self._run_script(
            "Get-SCVirtualMachine -Name \"{}\" -VMMServer $scvmm_server |"
            "Get-SCVirtualNetworkAdapter | Select IPv4Addresses |"
            "ft -HideTableHeaders".format(self.name))
        ip = data.translate(None, '{}')
        return ip if ip else None

    @property
    def creation_time(self):
        self.refresh()
        creation_time = convert_powershell_date(self.raw['CreationTime'])
        return creation_time.replace(tzinfo=tzlocal.get_localzone()).astimezone(pytz.UTC)

    def _do_vm(self, action, params=""):
        self.logger.info(" %s %s SCVMM VM '%s'", action, params, self.name)
        self._run_script(
            "Get-SCVirtualMachine -Name \"{}\" -VMMServer $scvmm_server | {}-SCVirtualMachine {}"
            .format(self.name, action, params).strip())
        return True

    def start(self):
        if self.is_suspended:
            return self._do_vm("Resume")
        else:
            return self._do_vm("Start")

    def stop(self, graceful=False):
        return self._do_vm("Stop", "-Shutdown" if graceful else "-Force")

    def restart(self):
        return self._do_vm("Reset")

    def suspend(self):
        return self._do_vm("Suspend")
    
    def delete(self):
        self.logger.info("Deleting SCVMM VM %s", self.name)
        self.ensure_state(VmState.STOPPED)
        return self._do_vm("Remove")

    def cleanup(self):
        return self.delete()
    
    def rename(self, name):
        self.logger.info(" Renaming SCVMM VM '%s' to '%s'",
            self.name, name)
        self.ensure_state(VmState.STOPPED)
        self._do_vm("Set", "-Name {}".format(name))
        self._name = name
        self.refresh()

    def clone(self, vm_name, vm_host, path, start_vm=True):
        self.logger.info("Deploying SCVMM VM '%s' from clone of '%s'",
            vm_name, self.name)
        script = """
            $vm_new = Get-SCVirtualMachine -Name "{src_vm}" -VMMServer $scvmm_server
            $vm_host = Get-SCVMHost -VMMServer $scvmm_server -ComputerName "{vm_host}"
            New-SCVirtualMachine -Name "{vm_name}" -VM $vm_new -VMHost $vm_host -Path "{path}"
        """.format(vm_name=vm_name, src_vm=self.name, vm_host=vm_host, path=path)
        if start_vm:
            script = "{} -StartVM".format(script)
        self._run_script(script)
        return SCVirtualMachine(system=self.system, name=vm_name)
    
    def enable_virtual_services(self):
        script = """
            $vm = Get-SCVirtualMachine -Name "{vm}"
            $pwd = ConvertTo-SecureString "{password}" -AsPlainText -Force
            $creds = New-Object System.Management.Automation.PSCredential("LOCAL\\{user}", $pwd)
            Invoke-Command -ComputerName $vm.HostName -Credential $creds -ScriptBlock {{
                Enable-VMIntegrationService -Name 'Guest Service Interface' -VMName "{vm}" }}
            Read-SCVirtualMachine -VM $vm
        """.format(user=self.system.user, password=self.system.password, vm=self.name)
        self._run_script(script)

    def get_hardware_configuration(self):
        self.refresh()
        return {'mem': self.raw['CPUCount'], 'cpu': self.raw['Memory']}

    def disconnect_dvd_drives(self):
        number_dvds_disconnected = 0
        script = """\
            $VM = Get-SCVirtualMachine -Name "{}"
            $DVDDrives = Get-SCVirtualDVDDrive -VM $VM
            foreach ($drive in $DVDDrives) {{$drive | Remove-SCVirtualDVDDrivce}}
            Write-Host "number_dvds_disconnected: " + $DVDDrives.length
        """.format(self.name)
        output = self._run_script(script)
        output = output.splitlines()
        num_removed_line = [line for line in output if "number_dvds_disconnected:" in line]
        if num_removed_line:
            number_dvds_disconnected = int(
                num_removed_line.split('number_dvds_disconnected:')[1].strip()
            )
        return number_dvds_disconnected

    def mark_as_template(self, library_server, library_share):
        # Converts an existing VM into a template.  VM no longer exists afterwards.
        script = """
            $VM = Get-SCVirtualMachine -Name \"{name}\" -VMMServer $scvmm_server
            New-SCVMTemplate -Name \"{name}\" -VM $VM -LibraryServer \"{ls}\" -SharePath \"{lp}\"
        """.format(name=self.name, ls=library_server, lp=library_share)
        self.logger.info("Creating SCVMM Template '%s' from VM '%s'", self.name, self.name)
        self._run_script(script)
        self.system.update_scvmm_library()
        return SCVMTemplate(system=self.system, name=self.name)


class SCVMTemplate(Template):
    def __init__(self, system, name, raw=None):
        """
        Construct an SCVMTemplate instance tied to a specific system

        Args:
            system: instance of SCVMMSystem
            name: name of template
            raw: raw json (as dict) for the template returned by the API
        """
        super(SCVMTemplate, self).__init__(system)
        self._name = name
        self._raw = raw

        self._run_script = self.system.run_script
        self._get_json = self.system.get_json

    def _get_myself(self):
        """
        Get Template from SCVMM

        Returns:
            dict of raw template json
        """
        script = 'Get-SCVMTemplate -Name \"{}\" -VMMServer $scvmm_server'
        data = self._get_json(script.format(self.name))
        if not data:
            raise ImageNotFoundError(self.name)
        return data

    @property
    def name(self):
        return self._name

    def refresh(self):
        data = self._get_myself()
        self._raw = data

    @property
    def raw(self):
        if not self._raw:
            self.refresh()
        return self._raw

    @property
    def exists(self):
        try:
            if self._get_myself():
                return True
            return False
        except NotFoundError:
            return False

    def deploy(self, vm_name, host_group, timeout=900, vm_cpu=None, vm_ram=None):
        script = """
            $tpl = Get-SCVMTemplate -Name "{template}" -VMMServer $scvmm_server
            $vm_hg = Get-SCVMHostGroup -Name "{host_group}" -VMMServer $scvmm_server
            $vmc = New-SCVMConfiguration -VMTemplate $tpl -Name "{vm_name}" -VMHostGroup $vm_hg
            Update-SCVMConfiguration -VMConfiguration $vmc
            New-SCVirtualMachine -Name "{vm_name}" -VMConfiguration $vmc
        """.format(template=self.name, vm_name=vm_name, host_group=host_group)

        if vm_cpu:
            script += " -CPUCount '{vm_cpu}'".format(vm_cpu=vm_cpu)
        if vm_ram:
            script += " -MemoryMB '{vm_ram}'".format(vm_ram=vm_ram)
        self.logger.info(" Deploying SCVMM VM '%s' from template '%s' on host group '%s'",
            vm_name, self.name, host_group)
        self._run_script(script)

        vm = self.system.get_vm(vm_name)
        vm.enable_virtual_services()
        vm.ensure_state(VmState.RUNNING, num_sec=timeout)
        vm.refresh()
        return vm

    def delete(self):
        script = """
            $Template = Get-SCVMTemplate -Name \"{template}\" -VMMServer $scvmm_server
            Remove-SCVMTemplate -VMTemplate $Template -Force
        """.format(template=self.name)
        self.logger.info("Removing SCVMM VM Template '%s'", self.name)
        self._run_script(script)
        self.system.update_scvmm_library()

    def cleanup(self):
        return self.delete()


class SCVMMSystem(System, VmMixin, TemplateMixin):
    """
    This class is used to connect to M$ SCVMM

    It still has some drawback, the main one is that pywinrm does not support domains with simple
    auth mode so I have to do the connection manually in the script which seems to be VERY slow.
    """
    _stats_available = {
        'num_vm': lambda self: len(self.list_vms()),
        'num_template': lambda self: len(self.list_templates()),
    }

    def __init__(self, **kwargs):
        super(SCVMMSystem, self).__init__(kwargs)
        self.host = kwargs["hostname"]
        self.user = kwargs["username"]
        self.password = kwargs["password"]
        self.domain = kwargs["domain"]
        self.provisioning = kwargs["provisioning"]
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
        self.logger.debug(' Running PowerShell script:\n%s\n', script)
        result = self.api.run_ps("{}\n\n{}".format(self.pre_script, script))
        if result.status_code != 0:
            raise self.PowerShellScriptError(
                "Script returned {}!: {}"
                .format(result.status_code, result.std_err)
            )
        return result.std_out.strip()

    def get_json(self, script, depth=2):
        """
        Run script and parse output as json
        """
        result = self.run_script(
            "{} | ConvertTo-Json -Compress -Depth {}".format(script, depth))
        if not result:
            return None
        try:
            return json.loads(result)
        except ValueError:
            self.logger.error("Returned data was not json.  Data:\n\n%s", result)
            raise ValueError("Returned data was not json")

    def create_vm(self, vm_name):
        raise NotImplementedError

    def list_vms(self):
        vm_list = self.get_json('Get-SCVirtualMachine -All -VMMServer $scvmm_server')
        return [SCVirtualMachine(system=self, name=vm['Name'], raw=vm) for vm in vm_list]

    def find_vms(self, **kwargs):
        """
        TODO -- in future there may be things worth filtering here using PowerShell '-Where'
        """
        raise NotImplementedError

    def get_vm(self, vm_name):
        vm = SCVirtualMachine(system=self, name=vm_name)
        vm.refresh()
        return vm

    def list_templates(self):
        templates = self.get_json("Get-SCVMTemplate -VMMServer $scvmm_server")
        return [SCVMTemplate(system=self, name=t.name, raw=t) for t in templates]

    def get_template(self, name):
        template = SCVMTemplate(system=self, name=name)
        template.refresh()
        return template

    def find_templates(self, **kwargs):
        raise NotImplementedError

    def _get_names(self, item_type):
        """
        Return names for an arbitrary item type
        """
        return [
            item['Name'] for item in
            self.get_json('Get-{} -VMMServer $scvmm_server').format(item_type)
        ]

    def list_clusters(self, **kwargs):
        """List all clusters' names."""
        return self._get_names('SCVMHostCluster')

    def list_networks(self):
        """List all networks' names."""
        return self._get_names('SCLogicalNetwork')

    def list_hosts(self, **kwargs):
        return self._get_names('SCVMHost')

    def info(self):
        return "SCVMMSystem host={}".format(self.host)

    def disconnect(self):
        pass

    def update_scvmm_library(self):
        # This forces SCVMM to update Library after a template change instead of waiting on timeout
        self.logger.info("Updating SCVMM Library")
        script = """
            $lib = Get-SCLibraryShare | where {$_.name -eq \'VMMLibrary\'}
            Read-SCLibraryShare -LibraryShare $lib[0] -Path VHDs -RunAsynchronously
            """
        self.run_script(script)

    class PowerShellScriptError(Exception):
        pass
