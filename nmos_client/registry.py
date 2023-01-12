import logging
import os
import datetime
import pprint
from typing import Optional, Any
from nmos_client.utility import NmosCommon, RegistryNodeShared
from nmos_client.service_discovery import ServiceDiscovery

class Registry(NmosCommon, RegistryNodeShared, ServiceDiscovery):
    """
    Connect to and query an NMOS registry using HTTP. Intended use case is for adhoc scripting for diagnostics and
    fault-finding

    https://specs.amwa.tv/is-04/branches/
    """

    def __init__(self, ip: str = '', port: int = 0, ver: int = 0, search_domain: str = 'local', dns_sd: bool = True,
                 nameservers: Optional[list[str]] = None, transport: str = 'http', paging_limit: int = 10):
        """
        param ip: (str) ip address of the nmos registry
        :param port: (int) listening port on the nmos registry
        :param ver: (float) query protocol version (the latest supported version is used if not declared)
        :param search_domain: (string) search domain for discovery
        :param nameservers (list) list of name servers to search on. By default, the ServiceDiscovery class will attempt
        to use the hosts DNS settings however this has proven to not be consistently successful, fails if nameserver is
        not listed in /etc/resolv.conf or OS uses systemd-resolved. Explicitly defining the nameserver(s) is encouraged.
        param dns_sd: (bool) turn on or off dns_sd
        :param transport: (str) http currently supported
        :param paging_limit: (int) if left empty, uses server default
        """

        if nameservers is None:
            nameservers: list[str] = []

        self.log: logging.Logger = logging.getLogger(__name__)
        self.transport: str = transport
        self.supported_protocols: list[str] = ['http']
        self.ip: str = ip
        self.port: int = port
        self.name: str = f'{self.ip}:{self.port}'
        self.domain: str = search_domain
        self.nameservers: Optional[list[str]] = nameservers
        self.url: str = ''
        self.api: str = 'query'
        self.supported_ver: list[str] = []
        self.paging_limit: int = paging_limit

        # Discover registry services via DNS-SD or supplied parameters.
        # Exceptions for set_active_static() are not caught as to stop a script running if is unable to reach a registry
        # This behaviour may be reviewed for dynamic cases where the exception is dealt with by the caller
        if dns_sd:
            try:
                self.set_active_dns_sd()
            except RuntimeError:
                self.log.exception('Found no query services via DNS-SD, attempting static discovery ...')
                self.set_active_static()
        else:
            self.log.info('DNS-SD overridden, statically connecting to registry')
            self.set_active_static()

        # Set the socket
        self.set_socket(self.transport, self.ip, self.port)

        # Set API version, parameter used otherwise latest supported API
        self.set_version(ver)

        # Set the base URL for the registry
        self.set_url()

        # get base resources (devices, nodes, senders, receivers etc.) and create local data model
        # websocket messages received from the registry will populate self.local_model
        self.base: list[str] = self.get('')
        self.local_model: dict[str: list] = {resource[:-1]: [] for resource in self.base
                                             if resource != 'subscriptions/'}

    ###
    # Get Methods
    #

    def get_subscriptions(self, *key: str, **qstr: str) -> Any:
        return self._search_reg('subscriptions', *key, **qstr)

    def get_node_href(self, id: str) -> str:
        """

        Parameters
        ----------
        id (str): Node UID

        Returns
        -------

        """
        return self.get_nodes('href', id=id)

    ###
    # Create Methods
    #

    def create_subscription(self, path: str, persist: bool = False, secure: bool = False, **params) -> dict:
        """
        Creates a subscription in the registry
        param: path (str) path to subscribe to
        param: persist (bool): whether subscription remains available after last client disconnects
        param: secure (bool): whether to encrypt the websocket (currently unsupported)
        param: open_ws (bool): Opens WS to subscription when True
        param: params: keyword arguments to filter results from registry.
        """

        body = {
            'max_update_rate_ms': 100,
            'resource_path': f'/{path}',
            'params': params,
            'persist': persist,
            'secure': secure
        }

        return self.post(f'subscriptions', body)

    ###
    # Remove Methods
    #

    def remove_subscription(self, id: str) -> dict:
        """
        Removes subscription from registry model
        param: id (str): id of the subscription to remove
        param: label (str): label of the subscription to remove
        """
        
        return self.delete(f'subscriptions/{id}')

    ###
    # Utility
    #

    def get_id(self, base_resource: str, label: str) -> str:

        """
        Takes a label or description and its associated base resource and returns the id (str)

        :param: base_resource ('senders', 'receivers', 'devices', 'nodes', 'sources', 'flows', 'subscriptions')
        :param: label (str) the label to be translated

        Subscriptions label is found inside params
        """
        for resource in self.base:
            if base_resource == resource[:-2]:
                base_resource += 's'
        if f'{base_resource}/' not in self.base:
            raise LookupError(f"{base_resource} not found in query api's base resources")

        if base_resource == "subscriptions":
            id = eval(f'self.get_{base_resource}("id", params__label="{label}")')
        else:
            id = eval(f'self.get_{base_resource}("id", label="{label}")')
        return id

    def backup(self) -> None:
        """
        Backs up entre registry model into backups/
        """
        if not os.path.exists('backups'):
            os.makedirs('backups')
        now = datetime.datetime.now()
        ts = now.strftime("%d-%m-%Y_%H:%M:%S")

        with open(f'backups/{self.name}_{ts}.txt', 'w+') as backup:
            pprint.pprint(self.search(), backup)
