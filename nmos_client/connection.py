import urllib.request
from typing import Optional
from nmos_client.utility import *
from nmos_client.service_discovery import ServiceDiscovery


class Connection(NmosCommon, ServiceDiscovery):
    """
    Node control via IS-05
    https://specs.amwa.tv/is-05/

    Intention is that a Connection class is created from a href provided by an instance of the Registry or Node classes
    """

    def __init__(self, href: str, ver: int = 0):
        """
        href: Connection href as found in nodes/devices.controls.href
        ver: (int) version of the connection API to use, uses the highest supported if not provided
        rds: (Registry.class): Registry class instance - allows the Node class to search the Registry for ids/labels etc
        """
        self.log: logging.Logger = logging.getLogger(__name__)
        self.href: str = href
        href_parsed: urlparse = urlparse(self.href)
        self.ip: str = href_parsed.hostname
        self.port: int = href_parsed.port
        self.transport: str = href_parsed.scheme
        self.supported_protocols: list[str] = ['http']
        self.api: str = 'connection'
        self.supported_ver: list[str] = []
        self.url: str = ''
        self.node_id: str = ''

        self.log.info(f'Validating connection href: {self.href}')
        self.test_connection(self.transport, self.ip, self.port)
        self.set_socket(self.transport, self.ip, self.port)

        # Set API version, static assignment used otherwise latest supported API
        # Creates base URL
        self.set_version(ver)
        self.set_url()
        self.base = self.get('')

    ###
    # GET
    #

    def get_sender_ids(self) -> list:
        return [id[:-1] for id in self.get(f'single/senders')]

    def get_receiver_ids(self) -> list:
        return [id[:-1] for id in self.get(f'single/receivers')]

    def get_active(self, id: str, *keys: str) -> Any:
        return self.__search_connection_resources('active', id, *keys)

    def get_staged(self, id: str, *keys: str) -> Any:
        return self.__search_connection_resources('staged', id, *keys)

    def get_constraints(self, id: str) -> Any:
        return self.__search_connection_resources('constraints', id)

    def get_transport_type(self, id: str) -> str:
        return self.__search_connection_resources('transporttype', id)

    def download_transport_file(self, id: str) -> bool | str:
        """
        Downloads a transport file (SDP) to transport_files/
        """

        if not os.path.exists('transport_files'):
            os.makedirs('transport_files')

        try:
            urllib.request.urlretrieve(f'{self.url}single/senders/{id}/transportfile', f'transport_files/{id}.sdp')
        except urllib.error.HTTPError:
            self.log.exception(f'No transport file found for {id}')
            return False

        return f'transport_files/{id}.sdp'

    def get_transport_file(self, id: str) -> bool | str:
        """
        returns the transport file (SDP)
        """

        self.log.info(f'Attempting to retrieve transport file for sender: {id}')
        self.log.debug(f'URL: {self.url}single/senders/{id}/transportfile')
        try:
            resp = urllib.request.urlopen(f'{self.url}single/senders/{id}/transportfile')
        except urllib.error.HTTPError:
            self.log.exception(f'No transport file found for {id}')
            return False

        self.log.info(f'Got transport file for sender: {id}')
        self.log.debug(f'{resp.read().decode("utf-8")}')
        resp = resp.read()
        return resp.decode('utf-8')

    ###
    # PATCH
    #

    def set_sender(self, id: str,
                   red_dest_ip: str = '', blue_dest_ip: str = '',
                   red_dest_port: int = 0, blue_dest_port: int = 0,
                   red_src_ip: str = '', blue_src_ip: str = '',
                   red_src_port: int = 0, blue_src_port: int = 0,
                   rtp_enabled: bool = True, st2022_7: bool = True, stage: bool = True,
                   activate: bool = False, activation_mode: Optional[str] = 'activate_immediate',
                   requested_time: Optional[str] = None, enable: bool = False) -> requests.models.Response | dict:

        """
        Support only for RTP/Multicast

        Any params that are not supplied are removed before flight

        Sets the staged parameters of a sender.

        Args:
            id:                     (str) UID of the sender
            red_dest_ip:            (str) Destination multicast address of the <red/blue> sender
            blue_dest_ip:
            red_dest_port:          (int) Destination UDP Port of the <red/blue> sender
            blue_dest_port:
            red_src_ip:             (str) Originating IP address of the <red/blue> sender
            blue_src_ip:
            red_src_port:           (int) Originating UDP Port of the <red/blue> sender
            blue_src_port:
            rtp_enabled:        (bool) True/False: enables the <red/blue> sender
            id/label:               (str) The id/label of the sender to be acted upon
            st2022_7:               (bool) Whether the sender is ST2022-7 compliant
            stage:                  (bool) If set to True, data is staged on the sender. If False, the dataset is
                                           returned. This can be helpful when creating a bulk data set
            activate:               (bool) Whether the staged parameters should be made active or not
            activation_mode:        (str) Activation mode: 'immediate', 'scheduled_absolute', 'scheduled_relative'
                                          or None
            requested_time:          (str) TAI Timestamp
            enable:                 (bool) Master enable/disable for the sender
        """

        self.__test_staged_inputs([red_dest_ip, red_src_ip, blue_dest_ip, blue_src_ip],
                                  [red_dest_port, red_src_port, blue_dest_port, blue_src_port])

        data = {
            'transport_params': [
                {
                    'destination_ip': red_dest_ip,
                    'destination_port': red_dest_port,
                    'rtp_enabled': rtp_enabled,
                    'source_ip': red_src_ip,
                    'source_port': red_src_port
                },
                {
                    'destination_ip': blue_dest_ip,
                    'destination_port': blue_dest_port,
                    'rtp_enabled': rtp_enabled,
                    'source_ip': blue_src_ip,
                    'source_port': blue_src_port
                }
            ]
        }

        data = self.__format_staged(data, activate, activation_mode, requested_time, enable, st2022_7)

        if stage:
            return self.patch(f'single/senders/{id}/staged', data)
        else:
            return data

    def connect_receiver(self, id: str, sender_id: str = '',
                         red_multicast: str = '', blue_multicast: str = '',
                         red_dest_port: int = 0, blue_dest_port: int = 0,
                         red_src_ip: str = '', blue_src_ip: str = '',
                         red_int_ip: str = '', blue_int_ip: str = '',
                         rtp_enabled_red: bool = True, rtp_enabled_blue: bool = True,
                         st2022_7: bool = True, stage: bool = True, activate: bool = False,
                         activation_mode: Optional[str] = 'activate_immediate', requested_time: Optional[str] = None,
                         enable: bool = False, sdp: str = '') -> requests.models.Response | dict:
        """
        Sets the staged parameters for a receiver.

        Support only for RTP/Multicast

        Any parameters that are not supplied are not sent to the connection API.

        If an SDP is supplied then it takes priority over other arguments

        Args:
            id:                     (str): UID of the NMOS receiver
            sender_id:              (str): UID of the NMOS Sender
            red_multicast:          (str): Destination multicast address of the <red/blue> sender
            blue_multicast:
            red_dest_port:          (int): Destination UDP Port of the <red/blue> sender
            blue_dest_port:
            red_src_ip:             (str) Originating IP address of the <red/blue> sender (required for SSM)
            blue_src_ip
            red_int_ip:             (str) Interface IP address that consumes the <red/blue> sender
            blue_int_ip
            rtp_enabled_red:        (bool) True/False: enables the <red/blue> receiver
            rtp_enabled_blue:
            id/label:               (str) The id/label of the receiver to be acted upon
            st2022_7:               (bool) Whether the receiver is ST2022-7 compliant
            stage:                  (bool) If set to True, data is staged on the receiver. If False, the dataset is
                                           returned. This can be helpful when creating a bulk data set
            activate:               (bool) Whether the staged parameters should be made active or not
            activation_mode:        (str) Activation mode: 'immediate', 'scheduled_absolute', 'scheduled_relative'
                                          or None
            requested_time:          (str) TAI Timestamp
            enable:                 (bool) Master enable/disable for the receiver
            sdp:                    (str) SDP to be patched to the receiver.
                                          Format is either a path to an SDP file or an SDP string
        """

        self.__test_staged_inputs([red_multicast, red_int_ip, red_src_ip, blue_src_ip, blue_multicast, blue_int_ip],
                                  [red_dest_port, blue_dest_port])

        if sdp:
            try:
                with open(sdp, 'r') as sdp_file:
                    sdp = sdp_file.read()
            except FileNotFoundError:
                self.log.info('Supplied SDP is not a path. Assuming it is an SDP string')
            data = {
                "sender_id": sender_id,
                "transport_file": {
                    'data': sdp,
                    'type': 'application/sdp'
                }
            }
        else:
            data = {
                'sender_id': sender_id,
                'transport_params': [
                    {
                        'destination_port': red_dest_port,
                        'interface_ip': red_int_ip,
                        'multicast_ip': red_multicast,
                        'rtp_enabled': rtp_enabled_red,
                        'source_ip': red_src_ip,
                    },
                    {
                        'destination_port': blue_dest_port,
                        'interface_ip': blue_int_ip,
                        'multicast_ip': blue_multicast,
                        'rtp_enabled': rtp_enabled_blue,
                        'source_ip': blue_src_ip,
                    }
                ]
            }

        data = self.__format_staged(data, activate, activation_mode, requested_time, enable, st2022_7)

        if stage:
            return self.patch(f'single/receivers/{id}/staged', data)
        else:
            return data

    def set_master_enable(self, id: str, state: bool) -> requests.models.Response:
        """
        Sets master enable of a sender or receiver
        param: state: bool - intended enable state of the sender/receiver
        """

        io = self.__get_io(id)

        if not isinstance(state, bool):
            raise TypeError(f'State is {state}, should be True/False')

        data = {'master_enable': state}

        return self.patch(f'single/{io}/{id}/staged', data)

    def activate(self, id: str, mode: str = 'activate_immediate',
                 requested_time: Optional[str] = None) -> requests.models.Response:
        """
        Activates a sender or receiver
        """

        io = self.__get_io(id)

        data = {
             'activation': {
                'mode': mode,
                'requested_time': requested_time
             }
        }

        return self.patch(f'single/{io}/{id}/staged', data)

    def disconnect_receiver(self, id: str, activate: bool = False, activation_mode: str = 'activate_immediate',
                            requested_time: Optional[str] = None, enable: bool = False,
                            st2022_7: bool = True) -> requests.models.Response:
        """
        Removes staged configuration from a receiver
        """

        data = {
            "sender_id": None,
            "transport_file": {
                'data': None,
                'type': None
            },
            'transport_params': [
                {
                    'destination_port': 'auto',
                    'multicast_ip': None,
                    'source_ip': None
                },
                {
                    'destination_port': 'auto',
                    'multicast_ip': None,
                    'source_ip': None,
                }
            ]
        }

        data = self.__format_staged(data, activate, activation_mode, requested_time, enable, st2022_7,
                                    remove_unused=False)
        return self.patch(f'single/receivers/{id}/staged', data)

    def set_bulk(self, data: dict[dict], io: str) -> requests.models.Response:
        """
        Posts bulk data to connection API
        Parameters
        ----------
        data (dict of dicts): A dictionary of models. Each model is a dictionary that can be generated
                            from self.set_sender(stage=False) or self.connect_receiver(stage=False).
                            The key for each model is its UID (str).

                            data = { 'UID' : {model},
                                     'UID_n' : {model_n} }
        io (str): 'senders' or 'receivers'

        Returns
        -------
        Requests responds from the bulk post

        """

        bulk_data = [{'id': id, 'params': model} for id, model in data.items()]
        return self.post(f'bulk/{io}', bulk_data)

    ###
    # Utility
    #

    def __test_staged_inputs(self, ip: str | list[str], port: int | list[int]) -> bool:
        # Basic input validation
        for address in ip:
            if address and not self.verify_ip(address):
                raise ValueError(f'{address} is not a valid IP address. Failed validation')
        for p in port:
            if p and not self.verify_port(p):
                raise ValueError(f'{p} is not a valid UDP Port number. Failed validation')
        return True

    def __format_staged(self, data: dict, activate: bool, activation_mode: Optional[str],
                        requested_time: Optional[str], enable: bool, st2022_7: bool,
                        remove_unused: bool = True) -> dict:
        """
        Formats data ready to be patched into the staged API. Filters unset key/value pairs,
        adds optional activation and master enable items and removed blue params if no ST2022-7.
        """

        if remove_unused:
            data = self.__remove_empty_keys(data)

        if activate:
            data['activation'] = {
                'mode': activation_mode,
                'requested_time': requested_time
            }

        if enable:
            data['master_enable'] = True

        # remove blue params if sender is not st2022-7
        if not st2022_7:
            del data['transport_params'][1]

        return data

    def __remove_empty_keys(self, data: dict | list) -> dict | list:
        """
        Takes a list of dictionaries or a dictionary and removes any key with a value of ''
        https://stackoverflow.com/questions/33529312/remove-empty-dicts-in-nested-dictionary-with-recursive-function
        """

        def strip_empties_from_list(d):
            new_d = []
            for v in d:
                if isinstance(v, dict):
                    v = strip_empties_from_dict(v)
                elif isinstance(v, list):
                    v = strip_empties_from_list(v)
                if v not in (None, str(), list(), dict(), 0):
                    new_d.append(v)
            return new_d

        def strip_empties_from_dict(d):
            new_d = {}
            for k, v in d.items():
                if isinstance(v, dict):
                    v = strip_empties_from_dict(v)
                elif isinstance(v, list):
                    v = strip_empties_from_list(v)
                if v not in (None, str(), list(), dict(), 0):
                    new_d[k] = v
            return new_d

        if isinstance(data, dict):
            return_data = strip_empties_from_dict(data)
        elif isinstance(data, list):
            return_data = strip_empties_from_list(data)
        else:
            self.log.error(f'Data to be emptied must be a list or dict. Got {type(data)}')
            raise TypeError(f'Data to be emptied must be a list or dict. Got {type(data)}')

        return return_data

    def __get_io(self, id: str) -> str:
        if id in self.get_sender_ids():
            return 'senders'
        elif id in self.get_receiver_ids():
            return 'receivers'
        else:
            raise LookupError('Could not find id in senders or receivers')

    def __search_connection_resources(self, resource: str, id: str, *keys: str) -> Any:
        """
        resources (str): Which resources to return (active/staged/constraints etc.)
        param: id: (str) uid of the sender/receiver.
        param: label: (str) label of the sender/receiver
        param: io (str): 'senders' or 'receivers'. Determines if the search algorithm is applied to senders or receivers
                        If not given, the provided id or label is searched for and the io is determined on a match
        """

        io = self.__get_io(id)

        path = f'single/{io}/{id}/{resource}'
        return self._filter_data(self.get(path), *keys)
