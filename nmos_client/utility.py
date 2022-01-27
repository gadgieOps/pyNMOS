import http
import requests
import logging
import urllib.request
from urllib.parse import urlparse
from typing import Any
from collections.abc import Callable
import os
import json
import ipaddress


class NmosCommon:
    """
    Common methods shared between Registry and Node subclasses.
    Not to be initiated
    """

    log: logging.Logger
    ip: str
    transport: str
    port: int
    api: str
    supported_protocols: list[str]
    supported_ver: list[str]
    url: str
    ver: str

    ###
    # Initialisation
    #

    def get_supported_versions(self) -> list[str]:
        """
        Queries /x-nmos/self.api and returns the list of implemented versions. Uses this rather than advertised versions
        in txt records in case the records are incorrect/out of date etc.
        """
        supported_ver = requests.get(f'{self.transport}://{self.ip}:{self.port}/x-nmos/{self.api}/')
        self.log.info(f'{self.api} supported versions are: {supported_ver.json()}')
        return list(supported_ver.json())

    def set_version(self, v: float) -> None:
        """
        Sets the api version to be used by the class instance. If the version is not manually supplied, use the highest
        implemented version on the server
        params: v (float) version number to be set
        """
        self.supported_ver = self.get_supported_versions()
        self.ver = f'v{v}/'

        if self.ver != 'v0/':
            if self.ver in self.supported_ver:
                self.log.info(f'Using statically assigned: {self.ver}')
            else:
                self.log.exception('Registry does not support supplied API version')
                raise ValueError('Registry does not support supplied API version')
        else:
            self.ver = max(self.supported_ver)
            self.log.info(f'Using latest supported versions: {self.ver}')

    def set_url(self) -> None:
        self.log.info(f'Setting base URL as: {self.transport}://{self.ip}:{self.port}/x-nmos/{self.api}/{self.ver}')
        self.url = f'{self.transport}://{self.ip}:{self.port}/x-nmos/{self.api}/{self.ver}'

    ###
    # HTTP Methods
    #

    def get(self, path: str) -> list[dict | str]:
        """
        Send http GET request to registry
        :param path: (str) path to resource
        :return:
            (list of dicts)
        """

        def g(p):
            self.log.info(f'GET: {p}')
            r = requests.get(p)
            if r.ok:
                return r
            else:
                r.raise_for_status()

        resp = g(f'{self.url}{path}')

        # Return data from api could be list or dict, format accordingly
        if isinstance(resp.json(), list):
            results = [i for i in resp.json()]
        else:
            results = [resp.json()]

        """
        If links header is in response, page through to get full data set. Starts with latest data and pages through
        prev link to older records as per IS-04 when no paging.since/until is provided (as per the initial request).

                https://specs.amwa.tv/is-04/releases/v1.3.1/docs/2.5._APIs_-_Query_Parameters.html
        The last cursor URL returns the most recently updated (or created) resources, as when no paging.since
        or paging.until parameters are specified.
        """

        if resp.links.get('next'):
            while 'paging.until=0:0' not in resp.links['prev']['url']:
                resp = g(resp.links['prev']['url'])
                for i in resp.json():
                    results.append(i)
            return results
        else:
            return results

    def post(self, path: str, body: dict | list[dict]) -> dict:
        self.log.info(f'POST: {self.url}{path}')
        r = requests.post(f'{self.url}{path}', json.dumps(body))
        if r.ok:
            return r.json()
        else:
            r.raise_for_status()

    def delete(self, path: str) -> dict:
        self.log.info(f'DELETE: {self.url}{path}')
        r = requests.delete(f'{self.url}{path}')
        if r.ok:
            return r.json()
        else:
            r.raise_for_status()

    def patch(self, path: str, data) -> dict:
        self.log.info(f'PATCH: {self.url}{path}')
        r = requests.patch(f'{self.url}{path}', json=data)
        if r.ok:
            return r.json()
        else:
            r.raise_for_status()

    ###
    # Utility
    #

    def test_connection(self, protocol: str, ip: str, port: int) -> bool:
        """
        Sends a GET to transport://socket/x-nmos. If it receives a 200 response, reachability is confirmed.
        Returns True if reachability is 200 is received
        Returns False if request fails to respond or responds with a non 200 code
        """

        url = f'{protocol}://{ip}:{port}/x-nmos/'
        self.log.info(f'Testing connection to {url}')

        try:
            r = requests.get(url, timeout=3)
        except (OSError, requests.exceptions.ConnectionError):
            self.log.exception(f'Unable to reach {protocol}://{ip}:{port}/x-nmos/')
            return False
        else:
            if r.status_code == 200:
                self.log.info(f'Connection to {url} successful')
                return True
            else:
                self.log.error(f'Error, got status code {r.status_code} from {url}')
                r.raise_for_status()

    def verify_protocol(self, protocol: str) -> bool:
        self.log.info(f'Verifying {protocol} is supported')
        if protocol in self.supported_protocols:
            return True
        else:
            self.log.error(f'Invalid protocol. Supported protocols: {self.supported_protocols}, got: {protocol}')
            raise Exception('Invalid protocol')

    def verify_ip(self, ip: str) -> bool:
        """
        Takes an IP address as a string or a list of IP addresses and tests their validity

        ipaddress raises ValueError if not a valid IP
        """
        self.log.info(f'Verifying IP address: {ip}')
        if isinstance(ip, str):
            ip = [ip]
        for address in ip:
            ipaddress.ip_address(address)
        return True

    def verify_port(self, port: int) -> bool:
        self.log.info(f'Verifying port number: {port}')
        if 0 < int(port) <= 65535:
            return True
        else:
            self.log.error(f'Port number invalid. Should be between 1 and 65535, got: {port}')
            raise ValueError('Port number invalid. Should be between 1 and 65535')

    def set_socket(self, protocol: str, ip: str, port: int) -> bool:
        """
        Verifies format of IP and port. Calls test_connection to verify full reachability to the server.

        ip (str): statically assigned address
        port(int): statically assigned tcp port

        Returns True if socket is valid and reachable
        Returns False if socket is invalid or unreachable
        """

        if self.verify_protocol(protocol):
            self.transport = protocol

        if self.verify_ip(ip):
            self.ip = ip

        if self.verify_port(port):
            self.port = int(port)

        self.log.info(f'Setting active {self.api} api as: {protocol}://{ip}:{port}')
        return True

    @staticmethod
    def _filter_data(data: list[dict], *keys: str) -> Any:
        """
        If kwargs are passed, filter them from the data and return.

        data[list] returned from self.get_data()
        *keys: (dict) dict of key/values that are to be filtered from the data

        Return
            list: if there is more than 1 record included in data
            dict: if there is only 1 recorded passed in data
        """

        def __filter_key(d, k):
            ###
            # https://stackoverflow.com/questions/9807634/find-all-occurrences-of-a-key-in-nested-dictionaries-and-lists
            #
            if isinstance(d, list):
                for i in d:
                    for x in __filter_key(i, k):
                        yield x
            elif isinstance(d, dict):
                if k in d:
                    yield d[k]
                for j in d.values():
                    for x in __filter_key(j, k):
                        yield x

        if keys:
            # search for keys in query string filtered data
            return_data = []
            for record in data:
                tmp = {}
                for key in keys:
                    if key in record.keys():
                        filtered_data = (list(__filter_key(record, key)))
                        if len(keys) == 1:
                            return_data.append(filtered_data[0])
                        else:
                            tmp[key] = filtered_data[0]
                if tmp:
                    return_data.append(tmp)

            # remove data from list if there is only one entry
            if type(return_data) is list and len(return_data) == 1:
                return return_data[0]
            else:
                return return_data
        else:
            # remove data from list if there is only one entry
            if type(data) is list and len(data) == 1:
                return data[0]
            else:
                return data


class RegistryNodeShared:
    """
    Methods that are common to the Query and Node APIs
    """

    log: logging.Logger
    base: list
    paging_limit: int

    get: Callable[[str], list[dict | str]]
    local_model: dict[str:list]
    search_local: bool
    _filter_data: Any

    ###
    # GET Methods
    #

    def get_nodes(self, *key: str, **qstr: str) -> Any:
        return self._search_reg('nodes', *key, **qstr)

    def get_devices(self, *key: str, **qstr: str) -> Any:
        return self._search_reg('devices', *key, **qstr)

    def get_senders(self, *key: str, **qstr: str) -> Any:
        return self._search_reg('senders', *key, **qstr)

    def get_receivers(self, *key: str, **qstr: str) -> Any:
        return self._search_reg('receivers', *key, **qstr)

    def get_sources(self, *key: str, **qstr: str) -> Any:
        return self._search_reg('sources', *key, **qstr)

    def get_flows(self, *key: str, **qstr: str) -> Any:
        return self._search_reg('flows', *key, **qstr)

    def get_connection_href(self, id: str) -> str | bool:
        """
        Returns the location of a devices IS-05 Connection API. Returns the latest implemented version href (str) on the
        target device
        """

        # extract controls from device model

        device_controls = self.get_devices('controls', id=id)
        control_connections = [i['href'] for i in device_controls if 'urn:x-nmos:control:sr-ctrl/v' in i['type']]

        if control_connections:
            # find and return the latest supported api version
            path = max([url.path for href in control_connections if (url := urlparse(href))])
            for href in control_connections:
                if path in href:
                    return href
        else:
            return False

    def get_all_sender_ids(self) -> list[str]:
        return self.get_senders('id')

    def get_all_receiver_ids(self) -> list[str]:
        return self.get_receivers('id')

    def download_manifest(self, id: str) -> tuple[str, http.client.HTTPMessage] | bool:
        """
        Downloads SDPs to manifests/
        """

        if not os.path.exists('manifests'):
            os.makedirs('manifests')

        manifest = self.get_senders('manifest_href', 'label', id=id)

        if not manifest['manifest_href']:
            self.log.info(f'Manifest not available for {manifest["label"]}.')
            return False
        else:
            manifest_href = manifest['manifest_href']
            self.log.info(f'Attempting to retrieve manifest for {manifest["label"]}')
            try:
                test = urllib.request.urlretrieve(manifest_href, f'manifests/{manifest["label"].replace("/", "_")}.sdp')
                return test
            except urllib.error.HTTPError:
                self.log.exception(f'Error retrieving {manifest["label"]}.')
                return False

    def get_manifest(self, id: str) -> str | bool:
        """
        returns manifest as a string
        """

        manifest = self.get_senders('manifest_href', 'label', id=id)

        if not manifest:
            self.log.warning(f'Manifest not available for {manifest["label"]}.')
            return False
        else:
            manifest_href = manifest['manifest_href']
            self.log.info(f'Attempting to retrieve manifest for {manifest["label"]}')
            try:
                resp = urllib.request.urlopen(manifest_href)
            except urllib.error.HTTPError:
                self.log.exception(f'Error retrieving {manifest["label"]}.')
                return False

            self.log.info(f'Got manifest for sender: {id}')
            resp = resp.read().decode("utf-8")
            self.log.debug(f'{resp}')
            return resp

    def search(self, *key: str, **qstr: str) -> dict[str: Any]:
        """
        Searches through registry filtering data as per the keys and query strings provided
        Returns dict of results
        """
        return {resource: self._search_reg(resource, *key, **qstr) for resource in self.base}

    ###
    # Utility
    #

    def __build_url(self, path: str, **qstr: str) -> str:
        """
        builds URL before sending GET to query API
        """
        path += '?'
        if qstr:
            # Add query string to path
            for key, value in qstr.items():
                key = key.replace('__', '.')
                path += f'{key}={value}&'
        if self.paging_limit:
            path += f'paging.limit={self.paging_limit}'

        return path

    def _search_reg(self, path: str, *keys: str, **qstr: str) -> Any:
        """
        Gets data from the registry using any supplied key_val filters provided, then filters based on
        any key filters provided before returning

        returns: list of dictionaries
        """

        if len(qstr.keys()) > 1:
            raise ValueError(f'Can only supply one query string, got: {len(qstr.keys())}')

        path = self.__build_url(path, **qstr)
        data = self.get(path)

        if not data:
            self.log.error(f'query returned no results for {path}')
            raise LookupError(f'query returned no results for {path}')
        else:
            return self._filter_data(data, *keys)
