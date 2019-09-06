.. image:: https://landscape.io/github/ManageIQ/wrapanapi/master/landscape.svg?style=fla
   :scale: 50 %
   :alt: Health Status
   :align: left
   :target: (https://landscape.io/github/ManageIQ/wrapanapi/master
.. image:: https://coveralls.io/repos/ManageIQ/wrapanapi/badge.svg?branch=master&service=github
   :scale: 50 %
   :alt: Coverage Status
   :align: left
   :target: https://coveralls.io/github/ManageIQ/wrapanapi?branch=master
.. image:: https://travis-ci.org/ManageIQ/wrapanapi.svg
   :scale: 50 %
   :alt: Build Status
   :align: left
   :target: https://travis-ci.org/ManageIQ/wrapanapi
.. image:: https://img.shields.io/pypi/pyversions/wrapanapi.svg
   :scale: 50 %
   :alt: Python Supported Versions
   :align: left
   :target: https://pypi.org/project/wrapanapi/


wrapanapi
==========

Introduction
------------
wrapanapi is a simple virtualization client with support (in varying degrees) for the following

* Red Hat Enterprize Virtualization (RHEV)
* Red Hat Openstack (RHOS)
* Red Hat Openshift
* Openshift
* VMware vCenter
* Microsoft SCVMM
* Microsoft Azure
* Google Compute Engine
* Hawkular
* Amazon EC2

It can be used to control virtualization clients and offers operations such as

* list_vm (returns a list of vm_names)
* list_template (returns a list of templates/images)
* start_vm (starts a vm)
* stop_vm (stops a vm)

Though conceptually names differ between cloud and infrastructure virtualization technologies (e.g. instance/vm)
it was decided to stick to one representation in the codebase and interface to give a singlar API across
all systems.

Installation
------------

Wrapanapi can be installed via `pip` as `pip install wrapanapi`
It is always a good idea to use virtualenv to install pip packages.

For Linux Users, Depending on the distribution you are using, you may need to install following packages
(or similar for your distribution of linux):

* libcurl-devel
* openssl-devel
* libxml2-devel
* libxml2-static
* gcc

If you are in doubt if you really need these packages, you will hit errors during installation that will make it
apparent for you to figure out that you need it.
Pycurl is a one such package that requires you to install packages like ones listed above, you can read more about it at
http://pycurl.io/docs/latest/install.html

Usage
-----
Each management system is invoked usually with a hostname and some credentials

.. code-block:: python

  from wrapanapi.virtualcenter import VMWareSystem
  
  system = VMWareSystem(hostname='10.0.0.0', username="root", password="password")
  system.list_vm()

Adding a new Management System
------------------------------
A management system should extend the Base class, and supply "Not Implemented" raises for items which
it doesn't support. This behaviour may change in the future as more and more diverse management systems.

.. code-block:: python

  from base import WrapanapiAPIBase

  class RHEVMSystem(WrapanapiAPIBase):
  
    _stats_available = {
      'num_vm': lambda self: self.api.get_summary().get_vms().total,
      'num_host': lambda self: len(self.list_host()),
      'num_cluster': lambda self: len(self.list_cluster()),
      'num_template': lambda self: len(self.list_template()),
      'num_datastore': lambda self: len(self.list_datastore()),
    }
  
    def __init__(self, hostname, username, password, **kwargs):
      super(RHEVMSystem, self).__init__(kwargs)

The call to ``super`` is necessary to set up the logger if noe has not been passed in with the ``logger``
keyword.

The developer can then add their own methods to interact with their own management system. Commonly accessible
statistics are generally all named the same across management systems. In this way we can treat multiple management
systems the same and use an identical method to check the number of vms on a RHEV system, to a VMware system.

Exceptions currently sit in a single module, this will probably change later with each management system having it's own
package and exceptions stored there.

This module was originally developed for assisting in the ManageIQ testing team.

Contributing
------------
The guidelines to follow for this project can be found in the 
cfme `dev_guide <http://cfme-tests.readthedocs.org/guides/dev_guide.html>`_.
