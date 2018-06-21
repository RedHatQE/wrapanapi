# coding: utf-8
from __future__ import absolute_import
from pyvcloud.vcd.client import Client, BasicLoginCredentials
from pyvcloud.vcd.org import Org
from pyvcloud.vcd.vdc import VDC
from pyvcloud.vcd.vapp import VApp

from wrapanapi.systems.base import System


class VmwareCloudSystem(System):
    """Client to VMware vCloud API"""

    def __init__(self, hostname, username, organization, password, api_port,
                 api_version, **kwargs):
        super(VmwareCloudSystem, self).__init__(**kwargs)
        self.endpoint = 'https://{}:{}'.format(hostname, api_port)
        self.username = username
        self.organization = organization
        self.password = password
        self.api_version = api_version
        self._client = None

    def info(self):
        return 'VmwareCloudSystem endpoint={}, api_version={}'.format(
            self.endpoint, self.api_version)

    @property
    def client(self):
        if self._client is None:
            self._client = Client(
                self.endpoint,
                api_version=self.api_version,
                verify_ssl_certs=False,
            )
            self.client.set_credentials(
                BasicLoginCredentials(self.username, self.organization, self.password)
            )
        return self._client

    def stats(self, *requested_stats):
        stats = self.count_vcloud(self.client)
        return {stat: stats[stat] for stat in requested_stats}

    def count_vcloud(self, client):
        """
        Obtain counts via vCloud API. Multiple dependent requests are needed therefore
        we collect them all in one pass to avoid repeating previous requests e.g. to
        fetch VMs, one must first fetch vApps and vdcs.
        :param client:
        :return:
        """
        org_resource = client.get_org()
        org = Org(client, resource=org_resource)

        stats = {
            'num_availability_zone': 0,
            'num_orchestration_stack': 0,
            'num_vm': 0
        }

        for vdc_info in org.list_vdcs():
            stats['num_availability_zone'] += 1
            vdc = VDC(client, resource=org.get_vdc(vdc_info['name']))
            for vapp_info in vdc.list_resources():
                try:
                    vapp_resource = vdc.get_vapp(vapp_info.get('name'))
                except Exception:
                    continue  # not a vapp (probably vapp template or something)

                vapp = VApp(client, resource=vapp_resource)
                stats['num_orchestration_stack'] += 1
                stats['num_vm'] += len(vapp.get_all_vms())

        return stats

    def disconnect(self):
        if self._client is not None:
            self._client.logout()
        self._client = None
