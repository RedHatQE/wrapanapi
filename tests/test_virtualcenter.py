import pytest
from pyVmomi import vim, vmodl

import wrapanapi
from wrapanapi.virtualcenter import VMWareSystem


@pytest.fixture(scope='function')
def provider(mocker):
    mocker.patch('wrapanapi.virtualcenter.SmartConnect')
    mocker.patch('wrapanapi.virtualcenter.Disconnect')
    system = VMWareSystem(hostname='vhost', username='vuser', password='vpass')
    return system


def test_constructor():
    """Test creating a :class:`VMWareSystem` object"""
    hostname = 'vsphere'
    username = 'vuser'
    password = 'vpass'
    system = VMWareSystem(hostname=hostname, username=username, password=password)

    assert system is not None
    assert system.hostname == hostname
    assert system.username == username
    assert system.password == password
    assert system._service_instance is None
    assert system._content is None
    assert system._vm_cache == {}
    assert system.kwargs == {}


def test_service_instance(provider):
    """Test that a service instance connects to vsphere"""
    si = provider.service_instance

    assert si is not None
    wrapanapi.virtualcenter.SmartConnect.assert_called_with(host='vhost', user='vuser',
                                                            pwd='vpass')


def test_content(provider):
    """Test that a content node is retrieved"""
    content = provider.content

    assert content is not None
    provider._service_instance.RetrieveContent.assert_called_with()


def test_version(provider):
    """Test that the version is returned correctly"""
    provider.content.about.version = '6.5'
    ver = provider.version

    assert ver == '6.5'


def test_get_obj_list(provider, mocker):
    """Test that we get the right objects"""
    mocked_folder = mocker.MagicMock()
    mocked_container = mocker.MagicMock(view=[1, 2, 3])
    provider.content.viewManager.CreateContainerView.return_value = mocked_container
    object_list = provider._get_obj_list(vim.VirtualMachine, mocked_folder)

    provider.content.viewManager.CreateContainerView.assert_called_with(
        mocked_folder, [vim.VirtualMachine], True)
    assert object_list == [1, 2, 3]


def test_get_obj(provider, mocker):
    """Test that _get_obj() does the right calls"""
    mock_obj = mocker.MagicMock()
    mock_obj.name = 'mock'
    mocked_get_list = mocker.MagicMock(return_value=[mocker.MagicMock(name='first'), mock_obj])
    mocker.patch.object(provider, '_get_obj_list', mocked_get_list)
    obj = provider._get_obj(vim.VirtualMachine, 'mock')

    assert obj is mock_obj
    mocked_get_list.assert_called_with(vim.VirtualMachine, None)


def test_get_obj_doesnt_exist(provider, mocker):
    """Test that _get_obj() returns None when it can't find an object"""
    mocked_get_list = mocker.MagicMock(return_value=[mocker.MagicMock(name='first')])
    mocker.patch.object(provider, '_get_obj_list', mocked_get_list)
    obj = provider._get_obj(vim.VirtualMachine, 'mock')

    assert obj is None
    mocked_get_list.assert_called_with(vim.VirtualMachine, None)


def test_build_filter_spec(provider):
    """Test that the _build_filter_spec() method correctly builds a filter spec"""
    start_entity = vim.VirtualMachine('start_entity')
    property_spec = vmodl.query.PropertyCollector.PropertySpec()
    property_spec.all = True
    filter_spec = provider._build_filter_spec(start_entity, property_spec)

    assert isinstance(filter_spec, vmodl.query.PropertyCollector.FilterSpec)
    assert filter_spec.propSet == [property_spec]
    assert len(filter_spec.objectSet) == 1
    assert isinstance(filter_spec.objectSet[0], vmodl.query.PropertyCollector.ObjectSpec)
    assert filter_spec.objectSet[0].obj is start_entity
    assert len(filter_spec.objectSet[0].selectSet) == 9
    assert filter_spec.objectSet[0].selectSet[0].name == 'resource_pool_traversal_spec'
    assert filter_spec.objectSet[0].selectSet[0].type == vim.ResourcePool
    assert filter_spec.objectSet[0].selectSet[0].path == 'resourcePool'
    assert len(filter_spec.objectSet[0].selectSet[0].selectSet) == 2
    assert filter_spec.objectSet[0].selectSet[0].selectSet[0].name == 'resource_pool_traversal_spec'
    assert filter_spec.objectSet[0].selectSet[0].selectSet[1].name == \
        'resource_pool_vm_traversal_spec'
    assert filter_spec.objectSet[0].selectSet[8].selectSet[0].name == 'folder_traversal_spec'
    assert filter_spec.objectSet[0].selectSet[8].selectSet[1].name == \
        'datacenter_host_traversal_spec'
    assert filter_spec.objectSet[0].selectSet[8].selectSet[2].name == 'datacenter_vm_traversal_spec'
    assert filter_spec.objectSet[0].selectSet[8].selectSet[3].name == \
        'compute_resource_rp_traversal_spec'
    assert filter_spec.objectSet[0].selectSet[8].selectSet[4].name == \
        'compute_resource_host_traversal_spec'
    assert filter_spec.objectSet[0].selectSet[8].selectSet[5].name == \
        'host_vm_traversal_spec'
    assert filter_spec.objectSet[0].selectSet[8].selectSet[6].name == \
        'resource_pool_vm_traversal_spec'
    assert filter_spec.objectSet[0].selectSet[8].selectSet[7].name == \
        'datacenter_datastore_traversal_spec'


def test_get_updated_obj(provider, mocker):
    """Test that the _get_updated_obj() method works correctly"""
    # Mocked properties and objects
    mocker.patch('wrapanapi.virtualcenter.vmodl')
    mocked_object_set = mocker.MagicMock()
    mocked_object_set.obj.runtime.paused = False
    mocked_filter_set = mocker.MagicMock()
    mocked_filter_set.objectSet = [mocked_object_set]
    mocked_update = mocker.MagicMock()
    mocked_update.filterSet = [mocked_filter_set]
    mocked_content = mocker.MagicMock()
    mocked_content.propertyCollector.WaitForUpdates.return_value = mocked_update
    provider._content = mocked_content
    # Create an object and update it
    obj = mocker.MagicMock()
    obj.runtime.paused = True
    new_obj = provider._get_updated_obj(obj)

    assert not new_obj.runtime.paused
