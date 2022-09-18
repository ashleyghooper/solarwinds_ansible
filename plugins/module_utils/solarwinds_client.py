# -*- coding: utf-8 -*-
#
# Copyright: (c) 2022, Ashley Hooper <ashleyghooper@gmail.com>
#
# GNU General Public License v3.0+
# (see COPYING or https://www.gnu.org/licenses/gpl-3.0.txt)

from __future__ import (absolute_import, division, print_function)
__metaclass__ = type


import traceback

from ansible.module_utils.basic import missing_required_lib

try:
    from orionsdk import SwisClient
except ImportError:
    HAS_ORIONSDK = False
    ORIONSDK_IMPORT_ERROR = traceback.format_exc()
else:
    HAS_ORIONSDK = True


def solarwindsclient_argument_spec():
    options = dict(
        hostname=dict(type='str', required=True),
        username=dict(type='str', required=True, no_log=True),
        password=dict(type='str', required=True, no_log=True),
    )

    return dict(
        solarwinds_connection=dict(
            type='dict',
            apply_defaults=True,
            options=options
        )
    )


def check_client(module):
    if not HAS_ORIONSDK:
        module.fail_json(msg=missing_required_lib('orionsdk'), exception=ORIONSDK_IMPORT_ERROR)


def validate_connection_params(module):
    params = module.params['solarwinds_connection']
    error_str = "missing required argument: solarwinds_connection[{}]"
    hostname = params['hostname']
    username = params['username']
    password = params['password']

    if (hostname and username and password):
        return params
    for arg in ['hostname', 'username', 'password']:
        if params[arg] in (None, ''):
            module.fail_json(msg=error_str.format(arg))


class SolarwindsClient(object):
    """
        Class encapsulating SolarWinds Information Service API client.
    """

    def __init__(self, module):
        # handle import errors
        check_client(module)

        params = validate_connection_params(module)

        hostname = params['hostname']
        username = params['username']
        password = params['password']

        self._module = module
        self._hostname = hostname
        self._auth = dict(username=username, password=password)
        try:
            self._client = SwisClient(self._hostname, **self._auth)
        except Exception as e:
            self.module.fail_json(msg="failed to open connection (%s): %s" % (hostname, str(e)))

        try:
            self._client.query("SELECT uri FROM Orion.Environment")
        except Exception as e:
            module.fail_json(msg="Failed to query Orion. "
                             "Check Orion hostname, username, and/or password: {0}".format(str(e))
                             )

    @property
    def module(self):
        """ Ansible module module

        Returns:
            the ansible module
        """
        return self._module

    @property
    def hostname(self):
        """ SolarWinds Information Service server hostname

        Returns:
            the SolarWinds Information Service server hostname.
        """
        return self._hostname

    @property
    def client(self):
        """ SolarWinds Information Service client.
        Returns:
            the SolarWinds Information Service client.
        """
        return self._client

    def entity(self, entity_uri):
        """ Search for entity by uri.
        Returns:
            the entity, or None if entity was not found.
        """
        if entity_uri is not None:
            return self._client.read(entity_uri)

    def credential(self, module):
        """ Search for credential by name.
        Returns:
            the credential, or None if credential was not found.
        """
        credential_name = module.params['credential_name']
        try:
            credentials_res = self._client.query(
                "SELECT ID FROM Orion.Credential WHERE Name = @credential_name",
                credential_name=credential_name
            )
            try:
                return next(c for c in credentials_res['results'])['ID']
            except Exception as e:
                module.fail_json(msg="Failed to query credential {0}".format(str(e)))
        except Exception as e:
            module.fail_json(msg="Failed to query credentials {0}".format(str(e)))

    def polling_engine(self, module, polling_engine_name):
        """ Search for polling engine by name.
        Returns:
            the polling engine, or None if polling engine was not found.
        """
        try:
            engines_res = self._client.query(
                "SELECT EngineID, ServerName, PollingCompletion FROM Orion.Engines WHERE ServerName = @engine_name",
                engine_name=polling_engine_name
            )
            return next(e for e in engines_res['results'])
        except Exception as e:
            module.fail_json(msg="Failed to query polling engines {0}".format(str(e)))
