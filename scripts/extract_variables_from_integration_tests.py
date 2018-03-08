"""
extracts manageiq integration test provider data/credentials
into variables files for the wrapanapi testsuite

this script uses manageiq integration test domain knowledge
and should be executed using its virtualenv
"""

import click
import six
import pathlib2
import json


WANTED_TAGS = frozenset(['rhevm', 'ms_scvmm'])


def extract_rhevm_system(mgmt, credentials):
    """extract all data needed for rhevm tests"""
    system_credentials = credentials[mgmt.credentials]
    system = dict(
        hostname=mgmt.hostname,
        version=mgmt.version,
        username=system_credentials.username,
        password=system_credentials.password,
    )
    return dict(
        system=system,
        templates=mgmt.templates
    )


def extract_scvmm_system(mgmt, credentials):
    """extract all data needed for scvmm tests
    """
    system_credentials = credentials[mgmt.credentials]
    system = dict(
        hostname=mgmt.hostname,
        username=system_credentials.username,
        password=system_credentials.password,
        domain=system_credentials.domain,
        provisioning=mgmt.provisioning

    )
    return dict(
        system=system,
        templates=mgmt.templates,
    )


system_type_map = {
    'scvmm': extract_scvmm_system,
    'rhevm': extract_rhevm_system,
}


def extract_system(mgt, credentials):
    """
    pick and run the real extraction based on the "type" key of the mgmt data
    and returns the data strucutred for later variables file merging
    """
    return {mgt.type: system_type_map[mgt.type](mgt, credentials)}


def write_variables(store_variables_folder, provider, data):
    """writes the extracted provider data to a json
    file based on the provider name"""
    filename = "{provider}.vars.json".format(provider=provider)
    with store_variables_folder.joinpath(filename).open('wb') as fp:
        json.dump(data, fp, indent=2, sort_keys=True)


def filter_mgmt_systems(management_systems):
    """return the items of management_systems filtered based on the WANTED_TAGS
    """

    for k, v in six.iteritems(management_systems):
        if WANTED_TAGS.intersection(v.get('tags', [])):
            yield k, v


@click.command()
@click.argument('store_variables_folder',
                type=click.Path(file_okay=False, exists=False),
                default="../wrapanapi_test_variables")
def main(store_variables_folder):
    """
    extracts each management system into a data file
    """
    store_variables_folder = pathlib2.Path(store_variables_folder)
    if not store_variables_folder.is_dir():
        store_variables_folder.mkdir()
    try:
        from cfme.utils.conf import cfme_data, credentials
    except ImportError:
        click.secho("could not import cfme data. please use the cfme virtualenv for this script")
        return 1

    to_process = list(filter_mgmt_systems(cfme_data.management_systems))
    for k, v in to_process:
        v = extract_system(v, credentials)
        click.echo(k)
        write_variables(store_variables_folder, k, v)


if __name__ == '__main__':
    main()
