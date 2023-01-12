from nmos_client.registry import Registry
from nmos_client.node import Node
from nmos_client.connection import Connection

###
# Create registry object via unicast DNS-SD
# registry = Registry(domain='ladyheton.me', nameservers=['192.168.10.11'])

###
# Create registry objext explicitly
registry = Registry(ip='192.168.110.91', port=80)

###
# Create Node object using its Label - this will be use to create an IS-05 connection
easy_nmos_node_id = registry.get_nodes('id', label='easy-nmos-node')
easy_nmos_node_href = registry.get_node_href(easy_nmos_node_id)
easy_nmos_node = Node(easy_nmos_node_href)

###
# GET Node information
#

# Get all node information from the registry
print(registry.get_nodes())

# Get all node IDs from the registry
print(registry.get_nodes('id'))

# Get the node ID for the node with label 'easy-nmos-node'
print(registry.get_nodes('id', label='easy-nmos-node'))

# Get all node information for the node with label 'easy-nmos-node'
print(registry.get_nodes(label='easy-nmos-node'))

###
# GET Device information
#

# Get all device information from the registry
print(registry.get_devices())

# Get all device IDs from the registry
print(registry.get_devices('id'))

# Get the device ID for the device with label 'easy-nmos-node'
print(registry.get_devices('id', label='easy-nmos-node'))

# Get all of information about the device with label 'easy-nmos-nose'
print(registry.get_devices(label='easy-nmos-node'))

###
# GET source information
#

# Get all source information from the registry
print(registry.get_sources())

# Get all source IDs from the registry
print(registry.get_sources('id'))

# Get the ID of the source labeled 'easy-nmos-node/source/a1'
print(registry.get_sources('id', label='easy-nmos-node/source/a1'))

# Get all information form the sourcelabeled 'easy-nmos-node/source/a1'
print(registry.get_sources(label='easy-nmos-node/source/a1'))

###
# GET Flow Information
#

# Get all flow information from the registry
print(registry.get_flows())

# Get all flow IDs from the registry
print(registry.get_flows('id'))

# Get the ID of the flow labeled 'easy-nmos-node/flow/a1'
print(registry.get_flows('id', label='easy-nmos-node/flow/a1'))

# Get all information form the flow labeled 'easy-nmos-node/flow/a1'
print(registry.get_flows(label='easy-nmos-node/flow/a1'))

# Get the IDs of all audio flows
print(registry.get_flows('id', format='urn:x-nmos:format:audio'))

# Get all information of all audio flows
print(registry.get_flows(format='urn:x-nmos:format:audio'))

# Get the IDs of all video flows
print(registry.get_flows('id', format='urn:x-nmos:format:video'))

# Get all information of all video flows
print(registry.get_flows(format='urn:x-nmos:format:video'))

###
# GET Sender Information
# 

# Get all sender information from the registry
print(registry.get_senders())

# Get all sender IDs from the registry
print(registry.get_senders('id'))

# Get the ID of the sender labeled 'easy-nmos-node/sender/a1'
print(registry.get_senders('id', label='easy-nmos-node/sender/a1'))

# Get all information form the sender labeled 'easy-nmos-node/sender/a1'
print(registry.get_senders(label='easy-nmos-node/sender/a1'))

###
# Use the flow format to get senders of a specific format
audio_flows = registry.get_flows('id', format='urn:x-nmos:format:audio')
audio_senders = [registry.get_senders(flow_id=flow) for flow in audio_flows]
audio_sender_ids = [registry.get_senders('id', flow_id=flow) for flow in audio_flows]
print(audio_senders)
print(audio_sender_ids)

###
# GET Receiver information
#

# Get all receiver information from the registry
print(registry.get_receivers())

# Get all receivers IDs from the registry
print(registry.get_receivers('id'))

# Get the ID of the receiver labeled 'easy-nmos-node/receiver/a1'
print(registry.get_receivers('id', label='easy-nmos-node/receiver/a1'))

# Get all information form the receiver labeled 'easy-nmos-node/receiver/a1'
print(registry.get_receivers(label='easy-nmos-node/receiver/a1'))

# Get all receivers capable of receiving a given media type
print(registry.get_receivers('id', caps__media_types='audio/L24'))


###
# GET IS-05 Connection href from a node
# Get pass the node ID to get_connection_href

easy_nmos_node_deviceid = registry.get_devices('id', label='easy-nmos-node')
easy_nmos_node_href = registry.get_connection_href(easy_nmos_node_deviceid)
print(easy_nmos_node_href)

###
# Create a IS-05 Object
# User to interact with the IS-05 API
easy_nmos_node_is05 = Connection(easy_nmos_node_href)

###
# Set transport paramaters of a sender using IS-05

# Get the ID of the sender to change
sender = registry.get_senders('id', label='easy-nmos-node/sender/a1')

# Set the sender transport parameters
# Create a connection to the IS-05 API using the nodes device ID
easy_nmos_node_deviceid = registry.get_devices('id', label='easy-nmos-node')
easy_nmos_node_is05.set_sender(sender, red_dest_ip='239.100.100.101', blue_dest_ip='239.200.200.201', activate=True, enable=True)

###
# GET Sender SDPs
#

sender_sdp = easy_nmos_node_is05.get_transport_file(sender)
print(sender_sdp)

###
# Connect a receiver using an SDP
#

# Get the ID of the receiver to connect
receiver = registry.get_receivers('id', label='easy-nmos-node/receiver/a1')

easy_nmos_node_is05.connect_receiver(receiver, sdp=sender_sdp, activate=True, enable=True)