#!/usr/bin/env python
from nmos_client.registry import Registry
from nmos_client.controller import Controller
import time

controller = Controller(search_domain='yem', nameservers=['192.168.10.11'], db_name='controller', db_user='nmos',
                        db_pass='nmos', db_host='192.168.10.11')
controller.open_registry_connection()



pass

"""
controller.log.warning(f'Number of senders: {len(controller.rds.get_senders("id"))}')
controller.log.warning(f'Number of flows: {len(controller.rds.get_flows("id"))}')
controller.log.warning(f'Number of sources: {len(controller.rds.get_sources("id"))}')
controller.log.warning(f'Number of receivers: {len(controller.rds.get_receivers("id"))}')
"""
"""
rds = Registry(search_domain='yem', nameservers=['192.168.10.11'])
device = rds._search_reg('devices', controls__href='http://192.168.10.23:80/x-nmos/connection/v1.1')

rds2 = Registry(search_domain='yem', nameservers=['192.168.10.11'], search_local=True)
rds2.create_subscription('devices')
device2 = rds2._search_reg('devices', label='easy-nmos-node')
"""
while True:
    pass