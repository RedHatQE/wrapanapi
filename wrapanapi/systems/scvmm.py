# coding: utf-8
"""Backend management system classes

Used to communicate with providers without using CFME facilities
"""
from __future__ import absolute_import

import json
import re
from datetime import datetime
import six
from textwrap import dedent
import time

import pytz
import tzlocal
import winrm
from wait_for import wait_for

from wrapanapi.entities import Template, TemplateMixin, Vm, VmMixin, VmState
from wrapanapi.exceptions import (
    ImageNotFoundError, VMInstanceNotFound, MultipleItemsError
)
from wrapanapi.systems.base import System


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
    return datetime.fromtimestamp(int(match.group(1)) / 1000.)


class _LogStrMixin(object):
    @property
    def _log_str(self):
        """
        Returns name or ID, but doesn't refresh raw to get name if we don't
        have raw data yet.. This is used only for logging purposes.
        """
        return (
            "[name: {}, id: {}]"
            .format(self._raw['Name'] if self._raw else "<not retrieved>", self._id)
        )


class SCVirtualMachine(Vm, _LogStrMixin):

    state_map = {
        'Running': VmState.RUNNING,
        'PowerOff': VmState.STOPPED,
        'Stopped': VmState.STOPPED,
        'Paused': VmState.SUSPENDED,  # 'Paused' is scvmm's version of 'suspended'
        'Missing': VmState.ERROR,
        'Creation Failed': VmState.ERROR,
    }
    ALLOWED_CHECK_TYPES = ["Standard", "Production", "ProductionOnly"]

    def __init__(self, system, raw=None, **kwargs):
        """
        Construct an SCVirtualMachine instance tied to a specific system

        Args:
            system: instance of SCVMMSystem
            raw: raw json (as dict) for the VM returned by the API
            id: uuid of the VM (the SCVMM 'ID' property on the VM)
        """
        super(SCVirtualMachine, self).__init__(system, raw, **kwargs)
        self._id = raw['ID'] if raw else kwargs.get('id')
        if not self._id:
            raise ValueError("missing required kwarg: 'id'")
        self._run_script = self.system.run_script
        self._get_json = self.system.get_json

    @property
    def _identifying_attrs(self):
        return {'id': self._id}

    def refresh(self, read_from_hyperv=True):
        """
        Get VM from SCVMM

        Args:
            read_from_hyperv (boolean) -- force reload vm data from host

        Returns:
            raw VM json
        """
        script = 'Get-SCVirtualMachine -ID \"{}\" -VMMServer $scvmm_server'
        if read_from_hyperv:
            script = '{} | Read-SCVirtualMachine'.format(script)
        try:
            data = self._get_json(script.format(self._id))
        except SCVMMSystem.PowerShellScriptError as error:
            if "Error ID: 801" in str(error):
                # Error ID 801 is a "not found" error
                data = None
            elif 'Error ID: 1730' in str(error):
                self.logger.warning('Refresh called on a VM in a state not valid for refresh')
                return None
            else:
                raise
        if not data:
            raise VMInstanceNotFound(self._id)
        self.raw = data
        return self.raw

    @property
    def name(self):
        return self.raw['Name']

    @property
    def host(self):
        return self.raw["HostName"]

    def _get_state(self):
        self.refresh(read_from_hyperv=False)
        return self._api_state_to_vmstate(self.raw['StatusString'])

    @property
    def uuid(self):
        return self._id

    @property
    def vmid(self):
        """ VMId is the ID of the VM according to Hyper-V"""
        return self.raw["VMId"]

    @property
    def ip(self):
        self.refresh(read_from_hyperv=True)
        data = self._run_script(
            "Get-SCVirtualMachine -ID \"{}\" -VMMServer $scvmm_server |"
            "Get-SCVirtualNetworkAdapter | Select IPv4Addresses |"
            "ft -HideTableHeaders".format(self._id))
        ip = data.translate(None, '{}')
        return ip if ip else None

    @property
    def creation_time(self):
        self.refresh()
        creation_time = convert_powershell_date(self.raw['CreationTime'])
        return creation_time.replace(tzinfo=tzlocal.get_localzone()).astimezone(pytz.UTC)

    def _do_vm(self, action, params=""):
        cmd = (
            "Get-SCVirtualMachine -ID \"{}\" -VMMServer $scvmm_server | {}-SCVirtualMachine {}"
            .format(self._id, action, params).strip()
        )
        self.logger.info(cmd)
        self._run_script(cmd)
        return True

    def start(self):
        if self.is_suspended:
            self._do_vm("Resume")
        else:
            self._do_vm("Start")
        self.wait_for_state(VmState.RUNNING)
        return True

    def stop(self, graceful=False):
        self._do_vm("Stop", "-Shutdown" if graceful else "-Force")
        self.wait_for_state(VmState.STOPPED)
        return True

    def restart(self):
        return self.stop() and self.start()

    def suspend(self):
        self._do_vm("Suspend")
        self.wait_for_state(VmState.SUSPENDED)
        return True

    def delete(self):
        self.logger.info("Deleting SCVMM VM %s", self._log_str)
        self.ensure_state(VmState.STOPPED)
        self._do_vm("Remove")
        wait_for(
            lambda: not self.exists, delay=5, timeout="3m",
            message="vm {} to not exist".format(self._log_str)
        )
        return True

    def cleanup(self):
        return self.delete()

    def rename(self, name):
        self.logger.info(" Renaming SCVMM VM '%s' to '%s'", self._log_str, name)
        self.ensure_state(VmState.STOPPED)
        self._do_vm("Set", "-Name {}".format(name))
        old_name = self.raw['Name']
        wait_for(
            lambda: self.refresh(read_from_hyperv=True) and self.name != old_name, delay=5,
            timeout="3m", message="vm {} to change names".format(self._log_str)
        )
        return True

    def clone(self, vm_name, vm_host, path, start_vm=True):
        self.logger.info("Deploying SCVMM VM '%s' from clone of '%s'", vm_name, self.log_str)
        script = """
            $vm_new = Get-SCVirtualMachine -ID "{src_vm}" -VMMServer $scvmm_server
            $vm_host = Get-SCVMHost -VMMServer $scvmm_server -ComputerName "{vm_host}"
            New-SCVirtualMachine -Name "{vm_name}" -VM $vm_new -VMHost $vm_host -Path "{path}"
        """.format(vm_name=vm_name, src_vm=self._id, vm_host=vm_host, path=path)
        if start_vm:
            script = "{} -StartVM".format(script)
        self._run_script(script)
        return SCVirtualMachine(system=self.system, name=vm_name)

    def enable_virtual_services(self):
        script = """
            $vm = Get-SCVirtualMachine -ID "{scvmm_vm_id}"
            $pwd = ConvertTo-SecureString "{password}" -AsPlainText -Force
            $creds = New-Object System.Management.Automation.PSCredential("{dom}\\{user}", $pwd)
            Invoke-Command -ComputerName $vm.HostName -Credential $creds -ScriptBlock {{
                Get-VM -Id {h_id} | Enable-VMIntegrationService -Name 'Guest Service Interface' }}
            Read-SCVirtualMachine -VM $vm
        """.format(
            dom=self.system.domain, user=self.system.user,
            password=self.system.password, scvmm_vm_id=self._id, h_id=self.vmid
        )
        self.system.run_script(script)

    def create_snapshot(self, check_type="Standard"):
        """ Create a snapshot of a VM, set checkpoint type to standard by default. """
        self.set_checkpoint_type(check_type=check_type)
        self.logger.info("Creating a checkpoint/snapshot of VM '%s'", self.name)
        script = """
            $vm = Get-SCVirtualMachine -ID "{scvmm_vm_id}"
            New-SCVMCheckpoint -VM $vm
        """.format(scvmm_vm_id=self._id)
        self.system.run_script(script)

    def set_checkpoint_type(self, check_type="Standard"):
        """ Set the checkpoint type of a VM, check_type must be one of ALLOW_CHECK_TYPES """
        self.logger.info("Setting checkpoint type to %s for VM '%s'", check_type, self.name)

        if check_type not in self.ALLOWED_CHECK_TYPES:
            raise NameError("checkpoint type '{}' not understood".format(check_type))

        script = """
            $vm = Get-SCVirtualMachine -ID "{scvmm_vm_id}"
            $pwd = ConvertTo-SecureString "{password}" -AsPlainText -Force
            $creds = New-Object System.Management.Automation.PSCredential("{dom}\\{user}", $pwd)
            Invoke-Command -ComputerName $vm.HostName -Credential $creds -ScriptBlock {{
                Get-VM -Id {h_id} | Set-VM -CheckpointType {check_type}
            }}
        """.format(
            dom=self.system.domain,
            user=self.system.user,
            password=self.system.password,
            scvmm_vm_id=self._id,
            h_id=self.vmid,
            check_type=check_type
        )
        self.system.run_script(script)

    def get_hardware_configuration(self):
        self.refresh(read_from_hyperv=True)
        data = {'mem': self.raw['Memory'], 'cpu': self.raw['CPUCount']}
        return {
            key: str(val) if isinstance(val, six.string_types) else val
            for key, val in data.items()
        }

    def disconnect_dvd_drives(self):
        number_dvds_disconnected = 0
        script = """\
            $VM = Get-SCVirtualMachine -ID "{}"
            $DVDDrives = Get-SCVirtualDVDDrive -VM $VM
            foreach ($drive in $DVDDrives) {{$drive | Remove-SCVirtualDVDDrivce}}
            Write-Host "number_dvds_disconnected: " + $DVDDrives.length
        """.format(self._id)
        output = self._run_script(script)
        output = output.splitlines()
        num_removed_line = [line for line in output if "number_dvds_disconnected:" in line]
        if num_removed_line:
            number_dvds_disconnected = int(
                num_removed_line[0].split('number_dvds_disconnected:')[1].strip()
            )
        return number_dvds_disconnected

    def mark_as_template(self, library_server, library_share, template_name=None, **kwargs):
        # Converts an existing VM into a template.  VM no longer exists afterwards.
        name = template_name or self.raw['Name']
        script = """
            $VM = Get-SCVirtualMachine -ID \"{id}\" -VMMServer $scvmm_server
            New-SCVMTemplate -Name \"{name}\" -VM $VM -LibraryServer \"{ls}\" -SharePath \"{lp}\"
        """.format(id=self._id, name=name, ls=library_server, lp=library_share)
        self.logger.info(
            "Creating SCVMM Template '%s' from VM '%s'", name, self._log_str)
        self._run_script(script)
        self.system.update_scvmm_library()
        return self.system.get_template(name=name)


class SCVMTemplate(Template, _LogStrMixin):
    def __init__(self, system, raw=None, **kwargs):
        """
        Construct an SCVMTemplate instance tied to a specific system

        Args:
            system: instance of SCVMMSystem
            raw: raw json (as dict) for the template returned by the API
            id: uuid of template (the 'ID' property on the template)
        """
        super(SCVMTemplate, self).__init__(system, raw, **kwargs)
        self._id = raw['ID'] if raw else kwargs.get('id')
        if not self._id:
            raise ValueError("missing required kwarg: 'id'")
        self._run_script = self.system.run_script
        self._get_json = self.system.get_json

    @property
    def _identifying_attrs(self):
        return {'id': self._id}

    @property
    def name(self):
        return self.raw['Name']

    @property
    def uuid(self):
        return self._id

    def refresh(self):
        """
        Get Template from SCVMM

        Returns:
            dict of raw template json
        """
        script = 'Get-SCVMTemplate -ID \"{}\" -VMMServer $scvmm_server'
        try:
            data = self._get_json(script.format(self._id))
        except SCVMMSystem.PowerShellScriptError as error:
            if "Error ID: 801" in str(error):
                # Error ID 801 is a "not found" error
                data = None
            else:
                raise
        if not data:
            raise ImageNotFoundError(self._id)
        self.raw = data
        return self.raw

    def deploy(self, vm_name, host_group, timeout=900, vm_cpu=None, vm_ram=None, **kwargs):
        script = """
            $tpl = Get-SCVMTemplate -ID "{id}" -VMMServer $scvmm_server
            $vm_hg = Get-SCVMHostGroup -Name "{host_group}" -VMMServer $scvmm_server
            $vmc = New-SCVMConfiguration -VMTemplate $tpl -Name "{vm_name}" -VMHostGroup $vm_hg
            Update-SCVMConfiguration -VMConfiguration $vmc
            New-SCVirtualMachine -Name "{vm_name}" -VMConfiguration $vmc
        """.format(id=self._id, vm_name=vm_name, host_group=host_group)
        if kwargs:
            self.logger.warn("deploy() ignored kwargs: %s", kwargs)
        if vm_cpu:
            script += " -CPUCount '{vm_cpu}'".format(vm_cpu=vm_cpu)
        if vm_ram:
            script += " -MemoryMB '{vm_ram}'".format(vm_ram=vm_ram)
        self.logger.info(
            " Deploying SCVMM VM '%s' from template '%s' on host group '%s'",
            vm_name, self._log_str, host_group
        )
        self._run_script(script)

        vm = self.system.get_vm(vm_name)
        vm.enable_virtual_services()
        vm.ensure_state(VmState.RUNNING, timeout=timeout)

        return vm

    def delete(self):
        script = """
            $Template = Get-SCVMTemplate -ID \"{id}\" -VMMServer $scvmm_server
            Remove-SCVMTemplate -VMTemplate $Template -Force
        """.format(id=self._id)
        self.logger.info("Removing SCVMM VM Template '%s'", self._log_str)
        self._run_script(script)
        self.system.update_scvmm_library()
        return True

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

    can_suspend = True
    can_pause = False

    def __init__(self, **kwargs):
        super(SCVMMSystem, self).__init__(**kwargs)
        self.host = kwargs["hostname"]
        self.port = kwargs.get("winrm_port", 5985)
        self.scheme = kwargs.get("winrm_scheme", "http")
        self.winrm_validate_ssl_cert = kwargs.get("winrm_validate_ssl_cert", False)
        self.user = kwargs["username"]
        self.password = kwargs["password"]
        self.domain = kwargs["domain"]
        self.provisioning = kwargs["provisioning"]
        self.api = winrm.Session(
            '{scheme}://{host}:{port}'.format(scheme=self.scheme, host=self.host, port=self.port),
            auth=(self.user, self.password),
            server_cert_validation='validate' if self.winrm_validate_ssl_cert else 'ignore',
        )

    @property
    def _identifying_attrs(self):
        return {'hostname': self.host}

    @property
    def can_suspend(self):
        return True

    @property
    def can_pause(self):
        return False

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

        def _raise_for_result(result):
            raise self.PowerShellScriptError(
                "Script returned {}!: {}"
                .format(result.status_code, result.std_err)
            )

        # Add retries for error id 1600
        num_tries = 6
        sleep_time = 10
        for attempt in range(1, num_tries + 1):
            self.logger.debug(' Running PowerShell script:\n%s\n', script)
            result = self.api.run_ps("{}\n\n{}".format(self.pre_script, script))
            if result.status_code == 0:
                break
            elif hasattr(result, 'std_err') and 'Error ID: 1600' in result.std_err:
                if attempt == num_tries:
                    self.logger.error("Retried %d times, giving up", num_tries)
                    _raise_for_result(result)

                self.logger.warning(
                    "Hit scvmm error 1600 running script, waiting %d sec... (%d/%d)",
                    sleep_time, attempt, num_tries
                )
                time.sleep(sleep_time)
            else:
                _raise_for_result(result)

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
        return [SCVirtualMachine(system=self, raw=vm) for vm in vm_list]

    def find_vms(self, name):
        """
        Find VMs based on name.

        Returns a list of SCVirtualMachine objects matching this name.
        """
        script = (
            'Get-SCVirtualMachine -Name \"{}\" -VMMServer $scvmm_server')
        data = self.get_json(script.format(name))
        # Check if the data returned to us was a list or 1 dict. Always return a list
        if not data:
            return []
        elif isinstance(data, list):
            return [SCVirtualMachine(system=self, raw=vm_data) for vm_data in data]
        return [SCVirtualMachine(system=self, raw=data)]

    def get_vm(self, vm_name):
        """
        Find VM with name 'name'.

        Raises ImageNotFoundError if no matches found
        Raises MultipleItemsError if multiple matches found
        """
        matches = self.find_vms(name=vm_name)
        if not matches:
            raise VMInstanceNotFound('vm with name {}'.format(vm_name))
        if len(matches) > 1:
            raise MultipleItemsError('multiple VMs with name {}'.format(vm_name))
        return matches[0]

    def list_templates(self):
        templates = self.get_json("Get-SCVMTemplate -VMMServer $scvmm_server")
        return [SCVMTemplate(system=self, raw=t) for t in templates]

    def find_templates(self, name):
        """
        Find templates based on name.

        Returns a list of SCVMTemplate objects matching this name.
        """
        script = (
            'Get-SCVMTemplate -Name \"{}\" -VMMServer $scvmm_server')
        data = self.get_json(script.format(name))
        # Check if the data returned to us was a list or 1 dict. Always return a list
        if not data:
            return []
        elif isinstance(data, list):
            return [SCVMTemplate(system=self, raw=tmpl_data) for tmpl_data in data]
        return [SCVMTemplate(system=self, raw=data)]

    def get_template(self, name):
        """
        Find template with name 'name'.

        Raises ImageNotFoundError if no matches found
        Raises MultipleItemsError if multiple matches found
        """
        matches = self.find_templates(name=name)
        if not matches:
            raise ImageNotFoundError('template with name {}'.format(name))
        if len(matches) > 1:
            raise MultipleItemsError('multiple templates with name {}'.format(name))
        return matches[0]

    def create_template(self, **kwargs):
        raise NotImplementedError

    def _get_names(self, item_type):
        """
        Return names for an arbitrary item type
        """
        data = self.get_json('Get-{} -VMMServer $scvmm_server'.format(item_type))
        if data:
            return [item['Name'] for item in data] if isinstance(data, list) else [data["Name"]]
        else:
            return None

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

    def update_scvmm_library(self, path="VHDs"):
        # This forces SCVMM to update Library after a template change instead of waiting on timeout
        self.logger.info("Updating SCVMM Library")
        script = """
            $lib = Get-SCLibraryShare
            Read-SCLibraryShare -LibraryShare $lib[0] -Path {path} -RunAsynchronously
        """.format(path=path)
        self.run_script(script)

    def download_file(self, url, name, dest="L:\\Library\\VHDs\\"):
        """ Downloads a file given a URL into the SCVMM library (or any dest) """
        self.logger.info("Downloading file {} from url into: {}".format(name, dest))
        script = """
            $url = "{url}"
            $output = "{dest}{name}"
            $wc = New-Object System.Net.WebClient
            $wc.DownloadFile($url, $output)
        """.format(url=url, name=name, dest=dest)
        self.run_script(script)
        # refresh the library so it's available for SCVMM to use
        self.refresh_library()

    def delete_file(self, name, dest="L:\\Library\\VHDs\\"):
        """ Deletes a file from the SCVMM library """
        self.logger.info("Deleting file {} from: {}".format(name, dest))
        script = """
            $fname = "{dest}{name}"
            Remove-Item -Path $fname
        """.format(name=name, dest=dest)
        self.run_script(script)
        self.refresh_library()

    def refresh_library(self):
        """ Perform a generic refresh of the SCVMM library """
        self.logger.info("Refreshing VMM library...")
        script = """
            Refresh-LibraryShare
        """
        self.run_script(script)

    class PowerShellScriptError(Exception):
        pass
