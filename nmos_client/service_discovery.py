from dns import resolver
from collections.abc import Callable
from typing import Optional
import logging


class ServiceDiscovery:
    domain: str
    nameservers: list
    discovered_registries: list[dict]
    active_registry: dict
    transport: str
    ip: str
    port: int

    log: logging.Logger
    verify_ip: Callable[[str], bool]
    verify_protocol: Callable[[str], bool]
    verify_port: Callable[[int], bool]
    test_connection: Callable[[str, str, int], bool]

    def set_active_dns_sd(self) -> bool:

        # noinspection PyTypeChecker
        self.discovered_registries = self.discover_registries(nameservers=self.nameservers, domain=self.domain)
        self.active_registry = self.get_best_registry(self.discovered_registries)
        self.transport = self.active_registry['transport']
        self.ip = self.active_registry['ip']
        self.port = self.active_registry['port']
        return True

    def set_active_static(self) -> bool:

        self.log.info(f'Building static url using: {self.transport}, {self.ip}, {self.port}')
        self.verify_protocol(self.transport)

        try:
            self.verify_ip(self.ip)
        except ValueError:
            self.log.error(f'Invalid IP address, unable to open connection statically')
            raise RuntimeError(f'Unable to connect to statically declared registry')

        self.verify_port(self.port)
        if self.test_connection(self.transport, self.ip, self.port):
            return True
        else:
            self.log.error('Unable to connect to statically declared registry')
            raise RuntimeError(f'Unable to connect to statically declared registry')

    def discover_registries(self, domain: Optional[str] = 'local',
                            nameservers: Optional[list[str]] = None) -> list[dict]:
        """
        Searches DNS server for NMOS registries

        Tests connections to discovered registries in priority order. Sets active connection to first viable registry.

        Details of all discovered registries are stored as self.discovered_registries.
        Parameters:
            dnspython will pick these up from the host by default however this hasn't tested consistently across
            OS/dns implementations. Statically defining these values where possible is recommended at this time.
            domain (str): search domain.
            nameservers (list): list of name servers

        returns:
        Dict of discovered Registries
        RuntimeError if query services are not found

        Sample bind configuration:

        nmosreg                    IN  A      192.168.10.21
        nmosreg2                   IN  A      192.168.111.29

        qry-api-1._nmos-query._tcp       IN  SRV    0 10 80 nmosreg
        qry-api-2._nmos-query._tcp       IN  SRV    0 20 8010 nmosreg2

        qry-api-1._nmos-query._tcp IN  TXT   ""api_ver=v1.0,v1.1,v1.2,v1.3" "api_proto=http" "pri=101" "api_auth=false""
        qry-api-2._nmos-query._tcp IN  TXT   ""api_ver=v1.0,v1.1,v1.2,v1.3" "api_proto=http" "pri=100" "api_auth=false""

        _nmos-query._tcp        PTR     qry-api-1._nmos-query._tcp
        _nmos-query._tcp        PTR     qry-api-2._nmos-query._tcp
        """

        if nameservers is None:
            nameservers = []

        self.log.info('Attempting DNS-SD discovery ...')

        found = False
        discovered_registries = []

        # Explicitly set nameservers if provided
        resolver.default_resolver = resolver.Resolver(configure=False)
        if nameservers:
            resolver.default_resolver.nameservers = nameservers
        if domain:
            resolver.default_resolver.domain = domain

        # Search for PTR Records
        service = f'_nmos-query._tcp.{resolver.default_resolver.domain}'

        ptr_records = self.query_nameserver(service, 'PTR')
        if not ptr_records:
            self.log.error('Did not find any candidate registries via DNS-SD')
            raise RuntimeError('Did not find any candidate registries via DNS-SD')

        for record in ptr_records:
            self.log.info(f'Found service: {record.target}')

        # search for SRV and TXT records
        records: dict[str:dict] = {}
        for ptr in ptr_records:

            self.log.info(f'Querying nameserver for {ptr.target} SRV and TXT records')

            records[ptr.target] = {'SRV': self.query_nameserver(str(ptr.target), 'SRV'),
                                   'TXT': self.query_nameserver(str(ptr.target), 'TXT')}

            if not records[ptr.target]['SRV'] or not records[ptr.target]['TXT']:
                records.pop(ptr.target)
                continue

            self.log.info(f'Found SRV/TXT records for {service}: {ptr.target}')

        # extract items from TXT records/ resolve A record
        for key, record in records.items():
            self.log.info(f'Extracting data from TXT record for {key}')
            self.log.info(f'Resolve IP address for {record["SRV"][0].target}')

            try:
                txt_items = self.extract_from_txt(record['TXT'], ['pri', 'api_proto', 'api_auth'])
            except LookupError:
                self.log.error(f'Unable to extract data from TXT record for {key}')
                break

            ip = self.resolve_name(str(record['SRV'][0].target).strip(','))
            if not ip:
                self.log.error(f'Unable to resolve IP address for {key}')
                continue

            self.log.debug(f'Extracted {txt_items} from TXT record for {key}')
            self.log.debug(f'Resolved IP: {ip} for {key}')
            self.log.info(f'Adding as candidate registry')

            discovered_registries.append(
                {
                    'name': str(records[key]['SRV'][0].target),
                    'ip': ip,
                    'port': str(records[key]['SRV'][0].port),
                    'pri': txt_items['pri'],
                    'transport': txt_items['api_proto'],
                    'auth': txt_items['api_auth']
                }
            )
            found = True

        if found:
            return discovered_registries
        else:
            self.log.error('Did not find any candidate registries via DNS-SD')
            raise RuntimeError('Did not find any candidate registries via DNS-SD')

    def get_best_registry(self, registries: list[dict]) -> dict[str: str | int]:
        """
        Recursively passes through discovered registries in priority order. Tests reachability and if it fails, it
        tries the next best discovered registry.
        """

        pris = [i['pri'] for i in registries]
        for reg in registries:
            name = reg['name']
            if reg['pri'] == min(pris):
                if self.test_connection(reg['transport'], reg['ip'], reg['port']):
                    return reg
                else:
                    self.log.warning(f"Couldn't form a connection to {name}, trying next discovered server ...")
                    registries.remove(reg)
                    if registries:
                        r = self.get_best_registry(registries)
                        if r:
                            return r
                    else:
                        raise LookupError('No viable registries found, failed connection tests')

    def query_nameserver(self, target: str, record_type: str) -> resolver.Answer | bool:
        """
        Searches nameserver for service PTR record. If not found, raises exception
        Parameters
        ----------
        target (str) service to be queried.
        record_type (str) 'PTR', SRV', 'TXT', 'A'
        Returns
        -------
        DNS Resolver object
        raises exceptions if not
        """

        if record_type not in ['PTR', 'SRV', 'TXT', 'A']:
            raise AttributeError('record type not supported, use PTR, SRV, TXT or A')
        try:
            return resolver.resolve(target, record_type)
        except resolver.NXDOMAIN:
            self.log.exception(f'{target} does not exist on nameserver')
            return False
        except resolver.NoAnswer:
            self.log.exception(f'{target} not found on nameserver, missing or broken records?')
            return False
        except resolver.NoNameservers:
            self.log.exception(f'No answer for {target}')
            return False

    def extract_from_txt(self, record: resolver.Answer, strings: list[str]) -> dict:
        """

        Parameters
        ----------
        record (DNS Resolver Answer Object)
        strings (list) strings to search for in the response. String must be formatted as "<str_to_search>"

        Returns
        -------
        Lookup Error if all strings are not satisfied
        dict of return data using the

        """

        return_data = {}

        for rdata in record:
            for rstring in rdata.strings:
                for string in strings:
                    if f'{string}=' in rstring.decode():
                        return_data[string] = rstring.decode().replace(f'{string}=', '')

        for string in strings:
            if string not in return_data.keys():
                self.log.error(f'Could not find {string} in TXT record')
                raise LookupError(f'Could not find {string} in TXT record')

        return return_data

    def resolve_name(self, name: str) -> str | bool:
        """
        Takes a domain name and returns an IP address
        Parameters
        ----------
        name (str)

        Returns
        -------
        IP address as a string

        """

        resp = self.query_nameserver(name, 'A')

        if not resp:
            return False
        else:
            return str(resp[0])
