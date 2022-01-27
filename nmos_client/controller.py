import sdp_transform
from typing import Optional
import time
from nmos_client.utility import *
from nmos_client.registry import Registry
from nmos_client.node import Node
from nmos_client.connection import Connection
from nmos_client.service_discovery import ServiceDiscovery
from nmos_client.db import Database


class Controller(ServiceDiscovery, NmosCommon):
    """
    NMOS controller
    """

    def __init__(self, search_domain: str = '', nameservers: Optional[list[str]] = None, db_name: str = '',
                 db_user: str = '', db_pass: str = '', db_host: str = '', db_port: int = 5432):
        """
        Discovers registries and adds them to the known registries list (self.registries)
        """

        self.log: logging.Logger = logging.getLogger(__name__)

        self.search_domain: str = search_domain
        if nameservers:
            self.nameservers: list[str] = nameservers

        # Create database instance
        self.db = Database(db_name, db_user, db_pass, db_host, db_port)

        # Registries that are known about whether reachable or not
        self.known_registries: list[dict] = []
        # Registries that are known about and reachable
        self.live_registries: list[dict] = []
        # Registry that the controller is actively using
        self.active_registry: dict = {}
        # Active registry instance
        self.rds: Optional[Registry] = None
        self.nodes: dict[str:Node] = None
        self.connections: dict[str:Connection] = None

        self.receivers_pending_activation: list[str] = []

        # Use DNS-SD to discover Nodes and add them to known registries
        self.add_discovered_registries()

        # Registry that is actively being controlled
        self.set_active_registry(self.get_best_registry(self.live_registries))

    ###
    # Registry management
    #

    def add_registry(self, name: str, protocol: str, ip: str, port: int, pri: int = 255) -> None:
        """
        Tests if the socket already exists and id it does, raises a RuntimeError.
        Adds to known registries
        Tests connection and adds to live registry list if it passes

        Parameters
        ----------
        name (str) name of the registry
        protocol (str) transport protocol to use ('http)
        ip (str) IP address of the registry
        port (str) TCP port the query API is listening on
        priority (str) The priority of the registry (if known)
        """
        # Test to ensure socket is not already in the list of known registries
        for reg in self.known_registries:
            if ip == reg['ip'] and port == reg['port']:
                self.log.error(f'Error; Registration Server {reg["name"]} already exists with {ip}:{port}')
                raise RuntimeError(f'Error; Registration Server {reg["name"]} already exists with {ip}:{port}')

        # update known registry list
        self.log.info(f'Adding {name} to known registries')
        new_reg = {'name': name, 'ip': ip, 'port': port, 'pri': pri, 'transport': protocol}
        self.known_registries.append(new_reg)

        # Add to live registry
        self.add_live_registry(new_reg)

    def remove_registry(self, name: str) -> None:
        """
        Removes a registry from the known and live lists
        Parameters
        ----------
        name (str) name of the registry
        """

        if name == self.active_registry['name']:
            raise RuntimeError("Can't remove registry, it is the current active registry")

        for reg in self.live_registries:
            if name == reg['name']:
                self.log.info(f'Removing {reg["name"]} from controllers live registries')
                self.known_registries.remove(reg)

        for reg in self.known_registries:
            if name == reg['name']:
                self.log.error(f'Removing {reg["name"]} from controllers known registries')
                self.known_registries.remove(reg)

    def add_discovered_registries(self) -> None:
        # Discover known_registries and add to the controller
        for reg in self.discover_registries(domain=self.search_domain, nameservers=self.nameservers):
            self.add_registry(reg['name'], reg['transport'], reg['ip'], reg['port'], pri=reg['pri'])

    def add_live_registry(self, r: dict) -> bool:
        """
        Takes a candidate registry, checks the socket it uses is not already in the list of live registries,
        tests its connections and then adds to the live list.
        If connection test fails, it is not added to the library.
        Parameters
        ----------
        r (dict) A registry model that is a candidate for the live list

        Returns
        -------
        True is reg is added to the live list
        False if the connection test fails
        Runtime Error if the registry is already in the live database
        """

        self.update_live_registries()

        for reg in self.live_registries:
            if reg['ip'] == r['ip'] and reg['port'] == r['port']:
                self.log.error(f'Error; Registration Server {reg["name"]} '
                               f'already exists in live server list using {r["ip"]}:{r["port"]}')
                raise RuntimeError(f'Error; Registration Server {reg["name"]} '
                                   f'already exists in live server list using {r["ip"]}:{r["port"]}')

        if self.test_connection(r['transport'], r['ip'], r['port']):
            self.log.info(f'Connection to {r["name"]} successful. Adding to live registries ... ')
            self.live_registries.append({'name': r['name'], 'ip': r['ip'], 'port': r['port'], 'pri': r['pri'],
                                         'transport': r['transport']})
            return True
        else:
            return False

    def update_live_registries(self) -> None:
        """
        Tests the connection to the list of live registries, removes if they've gone stale
        """
        self.log.info(f'Updating live registries ... ')
        for reg in self.live_registries:
            if not self.test_connection(reg['transport'], reg['ip'], reg['port']):
                self.log.info(f'{reg["name"]} is no longer reachable, removing from live registries')
                self.live_registries.remove(reg)

    def set_active_registry(self, registry: dict) -> bool:
        """
        Takes a registry model, checks if it is in the live list or if it can be added to the live list. If it is in
        the live list or can be put in the live list, sets the active registry model.
        Parameters
        ----------
        registry (dict) registry model
        """
        if registry not in self.live_registries and not self.add_live_registry(registry):
            self.log.error("Can't set active library, registry isn't live")
            raise LookupError("Can't set active library, registry isn't live")
        else:
            self.log.info(f'Active registry updated: {registry["name"]}')
            self.active_registry = registry
            return True

    def open_registry_connection(self) -> None:
        """
        Creates registry instance and assigns to self.rds
        Search registry for nodes and create node instances
        Searches devices and creates connection instances where it finds an IS-05 href. As the hrefs are found in the
        device model, they are references using the device ID found on a node
        """
        # If there is already a rds connection open, close it before continuing:
        if self.rds:
            self.close_registry_connection()

        # Create registry instance using contents of self.active_registry
        self.log.info(f"Creating registry instance for {self.active_registry['name']}")
        self.rds = Registry(transport=self.active_registry['transport'], ip=self.active_registry['ip'],
                            port=self.active_registry['port'], dns_sd=False)

        # open web sockets to each resource on the registry
        for resource in self.rds.base:
            if resource != 'subscriptions/':
                resp = self.rds.create_subscription(resource[:-1])
                self.db.open_ws(resp['id'], resp['ws_href'], resp['resource_path'][1:])

        # Allow time for database to retrive data from registry
        time.sleep(5)

        # Create instances for discovered nodes
        self.log.info('Creating Node instances')
        self.nodes = {node['id']: Node(node['href']) for node in self.db.get_nodes()}

        # Create instances for discovered connection APIs
        self.log.info('Creating Connection instances')
        self.connections = {device['id']: Connection(self.db.get_connection_href(device['id']))
                            for device in self.db.get_devices()
                            if self.db.get_connection_href(device['id'])}

        # Set node id in each connection instance
        for device_id, connection in self.connections.items():
            connection.node_id = self.rds.get_devices('node_id', id=device_id)

    def close_registry_connection(self) -> None:
        """
        Removes the active registry instance as well as any associated node or connection instances
        """
        self.log.info(f'Closing active registry connections')
        self.rds = None
        self.db = None
        self.nodes = {}
        self.connections = {}

    ###
    # Connection Management
    #

    def stage_connection(self, sender_id: str, receiver_id: str) -> bool:
        """
        Connects a sender to a receiver. It tries SDP first, if not available, it attempts to pass the transport params.

        Parameters
        ----------
        sender_id (str) UID of the sender
        receiver_id (str) UID of the receiver

        Returns
        -------
        True

        """
        self.verify_compatibility(sender_id, receiver_id)

        sender_sdp = self.db.get_manifest(sender_id)
        # get the device id that the receiver belongs to
        sdev = self.db.get_senders('device_id', id=sender_id)
        rdev = self.db.get_receivers('device_id', id=receiver_id)

        if sender_sdp:
            self.connections[rdev].connect_receiver(receiver_id, sender_id=sender_id, sdp=sender_sdp)
            self.receivers_pending_activation.append(receiver_id)
            return True
        else:
            sparams = self.connections[sdev].get_active(sender_id, 'transport_params')

            if len(sparams) == 2:
                self.connections[rdev].connect_receiver(receiver_id, sender_id=sender_id,
                                                        red_multicast=sparams[0]['destination_ip'],
                                                        blue_multicast=sparams[1]['destination_ip'],
                                                        red_dest_port=sparams[0]['destination_port'],
                                                        blue_dest_port=sparams[1]['destination_port'],
                                                        red_src_ip=sparams[0]['source_ip'],
                                                        blue_src_ip=sparams[1]['source_ip'])
            elif len(sparams) == 1:
                self.connections[rdev].connect_receiver(receiver_id, sender_id=sender_id,
                                                        red_multicast=sparams[0]['destination_ip'],
                                                        red_dest_port=sparams[0]['destination_port'],
                                                        red_src_ip=sparams[0]['source_ip'])
                
            self.receivers_pending_activation.append(receiver_id)
            return True
        
    def unstage_connection(self, receiver_id: str) -> None:
        """
        Flattens to staged parameters of a receiver
        Parameters
        ----------
        receiver_id (str) ID of the receiver to be unstaged
        """
        rdev = self.db.get_receivers('device_id', id=receiver_id)
        self.connections[rdev].disconnect_receiver(receiver_id)
        self.receivers_pending_activation.remove(receiver_id)

    def activate_pending_receivers(self, mode: str = 'immediate', requested_time: Optional[str] = None) -> None:
        """
        Activates the current list of pending receivers.

        Parameters
        ----------
        mode (str) Activation mode: 'immediate', 'scheduled_absolute', 'scheduled_relative'
        time (str) TAI Timestamp

        Returns
        -------

        """

        # get the device IDs for the receiver IDs found in ids
        devices = []

        for id in self.receivers_pending_activation:
            device_id = self.db.get_receivers('device_id', id=id)
            if device_id not in devices:
                devices.append(device_id)

        # Create a dictionary that assigns a list (value) of receiver ids to the correct device id (key)
        receiver_device_mappings = {}
        for device in devices:
            for id in self.receivers_pending_activation:
                if device not in receiver_device_mappings.keys() \
                        and device == self.db.get_receivers('device_id', id=id):
                    receiver_device_mappings[device] = [id]
                elif device in receiver_device_mappings.keys() \
                        and device == self.db.get_receivers('device_id', id=id):
                    receiver_device_mappings[device].append(id)

        data = {
            'activation': {
                'mode': mode,
                'requested_time': requested_time
            }
        }

        # create bulk data and send to connection api
        for device, receivers in receiver_device_mappings.items():
            bulk_data = {}
            for receiver in receivers:
                bulk_data[receiver] = data
            self.connections[device].set_bulk(bulk_data, 'receivers')

    def verify_compatibility(self, sender_id: str, receiver_id: str) -> None:
        """
        Tests the capabilities of the receiver against the sender format.

        Assume receiver constraints are the same for both legs of a ST2022-7 pair.

        Parameters
        ----------
        sender_id (str) UID of the sender
        receiver_id (str) UID of the receiver

        Returns
        -------

        """

        # get the flow/source id for the sender and the receiver capabilities
        flow = self.db.get_flows(id=self.rds.get_senders('flow_id', id=sender_id))
        source = self.db.get_sources(id=flow['source_id'])

        # if sender does not have an SDP, sdp_transform raises attribute error
        try:
            sdp = sdp_transform.parse(self.rds.get_manifest(sender_id))
        except AttributeError:
            sdp = ''

        receiver = self.db.get_receivers(id=receiver_id)
        
        # If receiver doesn't have constraints python throws a key error
        try:
            rconstraints = receiver['caps']['constraint_sets'][0]
        except KeyError:
            self.log.exception(f'Receiver {receiver["label"]} does not have constraints')
            rconstraints = None

        # Test sender media type is within the receivers capabilities
        if flow['media_type'] not in receiver['caps']['media_types']:
            raise TypeError('Sender media type not in receiver capabilities')

        if rconstraints:
            # test audio capabilities
            if receiver['format'] == 'urn:x-nmos:format:audio':
                self.__test_audio_capabilities(source, flow, sdp, rconstraints)
            elif receiver['format'] == 'urn:x-nmos:format:video':
                self.__test_video_capabilities(source, flow, sdp, rconstraints)

    @staticmethod
    def __test_audio_capabilities(source: dict, flow: dict, sdp: str, receiver_constraints: dict) -> bool:
        """
        Tests the audio parameters from a senders sdp and source/flow models against the constraints of the receiver.

        Only intended to be called from verify compatibility.

        Parameters
        ----------
        source (dict) source model for the sender
        flow (dict) flow model from the sender
        sdp (str) SDP from the sender
        receiver_constraints (dict) constraint set from the receiver. receiver['caps']['constraint_sets']

        Returns
        -------
        True: If capability tests are passed
        ValueError: If a test fails
        """

        # test channel count
        if 'urn:x-nmos:cap:format:channel_count' in receiver_constraints.keys():
            ch_count = receiver_constraints['urn:x-nmos:cap:format:channel_count']
            if not ch_count['minimum'] <= len(source['channels']) <= ch_count['maximum']:
                raise ValueError('Audio sender channel count is not within receiver capabilities')

        # test bit depth
        if 'urn:x-nmos:cap:format:sample_depth' in receiver_constraints.keys():
            if flow['bit_depth'] not in receiver_constraints['urn:x-nmos:cap:format:sample_depth']['enum']:
                raise ValueError('Audio sender bit depth is not within receiver capabilities')

        # test sample rate
        if 'urn:x-nmos:cap:format:sample_rate' in receiver_constraints.keys():

            found = False
            for sample_rate in receiver_constraints['urn:x-nmos:cap:format:sample_rate']['enum']:
                if flow['sample_rate'] == sample_rate:
                    found = True

            if not found:
                raise ValueError('Audio sender sample rate is not within receiver capabilities')

        """
        test packet time
        Currently disabled due to Sony Virtual Node adding a packet time constraint on its receivers that its senders
        do not comply to

        if sdp:
            if 'urn:x-nmos:cap:transport:packet_time' in receiver_constraints.keys():
                # search SDP for ptime
                if sdp['media'][0]['ptime'] not in receiver_constraints['urn:x-nmos:cap:transport:packet_time']['enum']:
                    raise ValueError('Audio sender packet time is not within receiver capabilities')
        else:
            self.log.warning('No SDP for sender, cannot validate packet time capabilities')
        """
        sdp += None

        return True

    def __test_video_capabilities(self, source: dict, flow: dict, sdp: str, receiver_constraints: dict) -> None:
        ###
        # To Be implemented
        #
        # Only intended to be called from verify compatibility.
        #

        pass