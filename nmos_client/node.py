import logging
from typing import Any
from urllib.parse import urlparse
from nmos_client.utility import NmosCommon, RegistryNodeShared
from nmos_client.service_discovery import ServiceDiscovery


class Node(NmosCommon, RegistryNodeShared, ServiceDiscovery):

    def __init__(self, href: str, ver: int = 0):

        self.log: logging.Logger = logging.getLogger(__name__)
        self.href: str = href
        href_parsed: urlparse = urlparse(self.href)
        self.ip: str = href_parsed.hostname
        self.port: int = href_parsed.port
        self.transport: str = href_parsed.scheme
        self.supported_protocols: list[str] = ['http']
        self.supported_ver: list[str] = []
        self.api: str = 'node'
        self.url: str = ''
        self.paging_limit: int = 10

        self.log.info(f'Validating node href: {self.href}')
        self.test_connection(self.transport, self.ip, self.port)
        self.set_socket(self.transport, self.ip, self.port)

        # Set API version, static assignment used otherwise latest supported API
        # Creates base URL
        self.set_version(ver)
        self.set_url()
        self.base = self.get('')

        # set label and id
        i: dict = self.get_self('label', 'id')
        self.id: str = i['id']
        self.label: str = i['label']

    def get_self(self, *keys: str) -> Any:
        return self._filter_data(self.get('self'), *keys)
