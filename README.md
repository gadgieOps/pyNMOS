# NMOS-pyController
Python based NMOS controller

Work in progress python based NMOS controller

registry.py - open a connection to an NMOS registry and interact with the query API<br>
node.py - open a connection to an NMOS node and interact with the node API<br>
controller.py - open an IS-05 connection and connect streams<br>
service_discovery.py - used to discover registration servers via DNS-SD<br>
utility.py - functions that are shared between registry and nodes>br>
controller.py - discovers registries and creates instances to the elected, discovers nodes and creates connection instances, creates ws connection to registry and keeps a local postgres database up to date<br>
db.py - manages the db required by controller.py<br>
api.py - HTTP API to sit in front of controller.py for front end integration<br>
