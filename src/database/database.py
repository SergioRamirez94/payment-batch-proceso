from immudb.client import ImmudbClient
import os
import grpc



IMMUDB_USER = os.getenv("IMMUDB_USER")
IMMUDB_PASSWORD = os.getenv("IMMUDB_PASSWORD")
IMMUDB_HOST = os.getenv("IMMUDB_HOST")
DATABASE_BEU = os.getenv("DATABASE_BEU")
DATABASE_TIKIN = os.getenv("DATABASE_TIKIN")

DICT_DATABASE = {
    "beu": DATABASE_BEU,
    "tikin": DATABASE_TIKIN,
}


DICT_CLIENTS = {}

def get_or_reconnect_client(integration: str, reconect:bool=False) -> ImmudbClient:
    global DICT_CLIENTS
    
    client = DICT_CLIENTS.get(integration)

    
    if not client:
        client = ImmudbClient(IMMUDB_HOST)
        client.login(IMMUDB_USER, IMMUDB_PASSWORD)
        client.useDatabase(DICT_DATABASE[integration])
        DICT_CLIENTS[integration] = client
        return client
    
    if reconect:
        client = ImmudbClient(IMMUDB_HOST)
        client.login(IMMUDB_USER, IMMUDB_PASSWORD)
        client.useDatabase(DICT_DATABASE[integration])
        DICT_CLIENTS[integration] = client
        return client

    try:
        client.healthCheck()
        return client
    except Exception:
        client = ImmudbClient(IMMUDB_HOST)
        client.login(IMMUDB_USER, IMMUDB_PASSWORD)
        client.useDatabase(DICT_DATABASE[integration])
        DICT_CLIENTS[integration] = client
        return client
    
def execute_sql(sql, integration):

    try:
        client = get_or_reconnect_client(integration)
        response = client.sqlExec(sql)
        return response
    except Exception as e:
        if isinstance(e, grpc._channel._MultiThreadedRendezvous) or isinstance(e, grpc._channel._InactiveRpcError):
            client = get_or_reconnect_client(integration, reconect=True)
            response = client.sqlExec(sql)
            return response

def query_executer(sql, integration):
    try:
        client = get_or_reconnect_client(integration)
        response = client.sqlQuery(sql)
        return response
    except Exception as e:
        if isinstance(e, grpc._channel._MultiThreadedRendezvous) or isinstance(e, grpc._channel._InactiveRpcError):
            client = get_or_reconnect_client(integration, reconect=True)
            response = client.sqlQuery(sql)
            return response