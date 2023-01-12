from nmos_client.registry import Registry
from nmos_client.node import Node
from nmos_client.controller import Controller
import pprint

registry = Registry(domain='ladyheton', nameservers=['192.168.10.11'])
node1_id = registry.get_nodes('id', label='easy-nmos-node')
node2_id = registry.get_nodes('id', label='easy-nmos-node2')

node1_href = registry.get_node_href(node1_id)
node2_href = registry.get_node_href(node2_id)

node1 = Node(node1_href)
node2 = Node(node2_href)

pprint.pprint(registry.get_nodes())
pprint.pprint(registry.get_nodes('id'))
pprint.pprint(registry.get_nodes('id', label='nmosreg'))
pprint.pprint(registry.get_nodes(label='nmosreg'))

pprint.pprint(registry.get_devices())
pprint.pprint(registry.get_devices('id'))
pprint.pprint(registry.get_devices('id', label='easy-nmos-node'))
pprint.pprint(registry.get_devices(label='easy-nmos-node'))

pprint.pprint(registry.get_senders())
pprint.pprint(registry.get_senders('id'))
pprint.pprint(registry.get_senders('id', label='easy-nmos-node/sender/a1'))
pprint.pprint(registry.get_senders(label='easy-nmos-node/sender/a1'))

pprint.pprint(registry.get_receivers())
pprint.pprint(registry.get_receivers('id'))
pprint.pprint(registry.get_receivers('id', label='easy-nmos-node/receiver/a1'))
pprint.pprint(registry.get_receivers(label='easy-nmos-node/receiver/a1'))

pprint.pprint(registry.get_sources())
pprint.pprint(registry.get_sources('id'))
pprint.pprint(registry.get_sources('id', label='easy-nmos-node/source/a1'))
pprint.pprint(registry.get_sources(label='easy-nmos-node/source/a1'))

pprint.pprint(registry.get_flows())
pprint.pprint(registry.get_flows('id'))
pprint.pprint(registry.get_flows('id', label='easy-nmos-node/flow/a1'))
pprint.pprint(registry.get_flows(label='easy-nmos-node/flow/a1'))

pprint.pprint(registry.get_devices('id'))
hrefs = []
for id in registry.get_devices('id'):
    hrefs.append(registry.get_connection_href(id))
pprint.pprint(hrefs)

node1_device = registry.get_devices('id', label='easy-nmos-node')
node2_device = registry.get_devices('id', label='easy-nmos-node2')


node1_audio_flows = registry.get_flows('id', format='urn:x-nmos:format:audio', device_id=node1_device)
node1_audio_senders = [registry.get_senders('id', flow_id=flow) for flow in node1_audio_flows]
node2_audio_receivers = registry.get_receivers('id', caps__media_types='audio/L24', device_id=node2_device)

red_ips = [f'239.1.1.{i}' for i in range(1, 11)]
blue_ips = [f'239.2.2.{i}' for i in range(1, 11)]
count = 0

for audio_sender in node1_audio_senders:
    node1.connection[node1_device].set_sender(audio_sender, red_dest_ip=red_ips[count], blue_dest_ip=blue_ips[count],
                                               activate=True, enable=True)
    count += 1

node1_sdps = [node1.connection[node1_device].get_transport_file(sender) for sender in node1_audio_senders]

count = 0
for receiver in node2_audio_receivers:
    node2.connection[node2_device].connect_receiver(receiver, sdp=node1_sdps[count], activate=True, enable=True)
    count += 1

red_dest = [f'239.100.100.{i}' for i in range(1, 11)]
blue_dest = [f'239.200.200.{i}' for i in range(1, 11)]

sender_models = {}
for i, id in enumerate(node1_audio_senders):
    sender_models[id] = node1.connection[node1_device]\
        .set_sender(id, stage=False, red_dest_ip=red_dest[i], red_dest_port=50040, blue_dest_ip=blue_dest[i],
                    blue_dest_port=50060, activate=True, enable=True)

node1.connection[node1_device].set_bulk(sender_models, 'senders')


node1_sdps = [node1.connection[node1_device].get_transport_file(sender) for sender in node1_audio_senders]

receiver_models = {}

for i, id in enumerate(node2_audio_receivers):
    receiver_models[id] = node2.connection[node2_device].\
        connect_receiver(id, sdp=node1_sdps[i], stage=False, activate=True, enable=True)

node2.connection[node2_device].set_bulk(receiver_models, 'receivers')
controller = Controller('ladyheton', nameservers=['192.168.10.11'])

controller.open_registry_connection()

n1_did = controller.rds.get_devices('id', label='easy-nmos-node')
s1 = controller.rds.get_senders('id', label='easy-nmos-node/sender/a0')

n2_did = controller.rds.get_devices('id', label='easy-nmos-node2')
r1 = controller.rds.get_receivers('id', label='easy-nmos-node2/receiver/a0')

n1_did = controller.rds.get_devices('id', label='easy-nmos-node')
s2 = controller.rds.get_senders('id', label='easy-nmos-node/sender/v0')

n2_did = controller.rds.get_devices('id', label='easy-nmos-node2')
r2 = controller.rds.get_receivers('id', label='easy-nmos-node2/receiver/v0')

n1_did = controller.rds.get_devices('id', label='easy-nmos-node')
s3 = controller.rds.get_senders('id', label='easy-nmos-node/sender/d0')

n2_did = controller.rds.get_devices('id', label='easy-nmos-node2')
r3 = controller.rds.get_receivers('id', label='easy-nmos-node2/receiver/d0')

n1_did = controller.rds.get_devices('id', label='easy-nmos-node')
s4 = controller.rds.get_senders('id', label='easy-nmos-node/sender/m0')

n2_did = controller.rds.get_devices('id', label='easy-nmos-node2')
r4 = controller.rds.get_receivers('id', label='easy-nmos-node2/receiver/m0')

n1_did = controller.rds.get_devices('id', label='easy-nmos-node')
s5 = controller.rds.get_senders('id', label='easy-nmos-node/sender/b0')

n2_did = controller.rds.get_devices('id', label='easy-nmos-node2')
r5 = controller.rds.get_receivers('id', label='easy-nmos-node2/receiver/b0')

n1_did = controller.rds.get_devices('id', label='easy-nmos-node2')
s6 = controller.rds.get_senders('id', label='easy-nmos-node2/sender/a0')

n2_did = controller.rds.get_devices('id', label='easy-nmos-node')
r6 = controller.rds.get_receivers('id', label='easy-nmos-node/receiver/a0')


controller.stage_connection(s1, r1) # audio
controller.stage_connection(s2, r2) # video
controller.stage_connection(s3, r3) # data
controller.stage_connection(s4, r4) # mux
controller.stage_connection(s6, r6) # audio

controller.activate_pending_receivers()

sender_device = controller.rds.get_senders('device_id', id=s1)
pprint.pprint(controller.connections[sender_device].get_transport_file(s1))