from flask import Flask, request
from typing import Optional
import json
from nmos_client.controller import Controller

controller = Controller(search_domain='yem', nameservers=['192.168.10.11'], db_name='controller',
                        db_host='192.168.10.11', db_user='nmos', db_pass='nmos')

api = Flask(__name__)
api.config['DEBUG'] = True


def build_request(resource: str, f: str, karg: dict) -> str:
    """
    Takes request from HTTP API and builds the necessary request to the database for information.
    Returns a JSON formatted string.
    Parameters
    ----------
    resource: the resource that has been requested (senders, receivers, sources, etc.)
    f: filter that filters the record data that is to be returned (id, label etc,)
    karg: a key argument that has been subtracted from the request

    Returns
    -------
    data as a string, formatted in JSON
    """

    if len(karg) > 1:
        raise RuntimeError('Can only process a single key work argument')
    key = [k for k in karg.keys()]
    value = [v for v in karg.values()]

    base = f'controller.db.get_{resource}('

    if f:
        base += f'"{f}"'
    if f and karg:
        base += ', '
    if karg:
        base += f'{key[0]}="{value[0]}"'
    base += ')'

    return json.dumps(eval(base))


@api.route('/nodes', methods=['GET'])
@api.route('/nodes/<string:f>', methods=['GET'])
def nodes(f: Optional[str] = None):
    return build_request('nodes', f, request.args)


@api.route('/devices', methods=['GET'])
@api.route('/devices/<string:f>', methods=['GET'])
def devices(f: Optional[str] = None):
    return build_request('devices', f, request.args)


@api.route('/sources', methods=['GET'])
@api.route('/sources/<string:f>', methods=['GET'])
def sources(f: Optional[str] = None):
    return build_request('sources', f, request.args)


@api.route('/flows', methods=['GET'])
@api.route('/flows/<string:f>', methods=['GET'])
def flows(f: Optional[str] = None):
    return build_request('flows', f, request.args)


@api.route('/senders/', methods=['GET'])   
@api.route('/senders/<string:f>', methods=['GET'])
def senders(f: Optional[str] = None):
    return build_request('senders', f, request.args)


@api.route('/receivers', methods=['GET'])
@api.route('/receivers/<string:f>', methods=['GET'])
def receivers(f: Optional[str] = None):
    return build_request('receivers', f, request.args)


@api.route('/connection_href/<string:id>', methods=['GET'])
def connection_href(id):
    return json.dumps(controller.db.get_connection_href(id))


@api.route('/manifest/<string:id>', methods=['GET'])
def manifest(id):
    return controller.db.get_manifest(id)


api.run()
