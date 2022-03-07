# NMOS-pyController
Python based NMOS controller

Work in progress python based NMOS controller. The classes and functions in nmos_client make an ad hoc frame work for using python to interact with NMOS devices as well as the beginnings of a long running application to act as an NMOS controller. 



<b>registry.py</b> - open a connection to an NMOS registry and interact with the query API<br>

<b>node.py</b> - open a connection to an NMOS node and interact with the node API<br>

<b>controller.py</b> - open an IS-05 connection and connect streams<br>

<b>service_discovery.py</b> - used to discover registration servers via DNS-SD<br>

<b>utility.py</b> - functions that are shared between registry and nodes<br>

<b>controller.py</b> - discovers registries and creates instances to the administrativley highest priority RDS, discovers nodes and creates node/connection instances, creates ws connection to registry and keeps a local postgres database up to date. Idea is that this would run and allow users to interact with the application through a front end/api<br>

<b>db.py</b> - manages the db required by controller.py<br>

<b>api.py</b> - HTTP API to sit in front of controller.py for front end integration<br>

================================================================

Logging is currently hardcoded to: /home/dave/.nmos_client/logs/nmos.log in nmos_client/__init__.py. This needs changing to something more useful.
