# pyNMOS 

A Python package for interacting with NMOS IS-04 and IS-05 APIs. Useful for gathering information, troubleshooting and ad hoc testing.

Usage:
------

The modules below are self documenting, read through the various functions to understand full usage. example.py contains example usage.

In general, to get information from the APIs you can add a key and a key value pair to filter the returned result. For example:

~~~
# Get all node information from the registry
print(registry.get_nodes())

# Get all node IDs from the registry
print(registry.get_nodes('id'))

# Get the node ID for the node with label 'easy-nmos-node'
print(registry.get_nodes('id', label='easy-nmos-node'))

# Get all node information for the node with label 'easy-nmos-node'
print(registry.get_nodes(label='easy-nmos-node'))
~~~

Other functions:
- Download SDP files from Senders
- Take a full copy of the registries API Schema
- Set Sender transport parameters
- Connect Senders to Receivers
- Discover Registries using unicast DNS-SD
- Enable/Activate Senders and Receivers
- Set IS-05 bulk parameters
- Create subscriptions to the registry

Main modules:
-------------
<b>registry.py</b> - open a connection to an NMOS registry and interact with the query API<br>
<b>node.py</b> - open a connection to an NMOS node and interact with the node API<br>
<b>connection.py</b> - open an IS-05 connection and connect streams<br>
<b>service_discovery.py</b> - used to discover registration servers via DNS-SD<br>
<b>utility.py</b> - functions that are shared between registry and nodes<br>

Modules for further development:
--------------------------------
<b>controller.py</b> - discovers registries and creates instances to the administrativley highest priority RDS, discovers nodes and creates node/connection instances, creates ws connection to registry and keeps a local postgres database up to date. Idea is that this would run and allow users to interact with the application through a front end/api<br>
<b>db.py</b> - manages the db required by controller.py<br>
<b>api.py</b> - HTTP API to sit in front of controller.py for front end integration<br>

Collaboration
-------------
Please feel free to fork and submit pull requests

