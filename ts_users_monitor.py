# !/usr/bin/python3
import os
import uuid

import boto3
import ts3
from datetime import datetime, timedelta

from boto3.dynamodb.types import TypeSerializer

client = boto3.client('dynamodb')

TS_IP = os.environ['TEAMSPEAK_SERVER_HOST_STRING']  # "telnet://18.189.233.187:10011"
TS_USER = os.environ['TEAMSPEAK_CLIENT_LOGIN_NAME']  # monitoraccount
TS_PASS = os.environ['TEAMSPEAK_CLIENT_LOGIN_PASSWORD']  # XFrQlbxT

USER_CONNECTION_TABLE_NAME = os.environ['DYNAMODB_USERCONNECTIONTABLE_NAME']  # UserConnection3
USERUID_TABLE_NAME = os.environ['DYNAMODB_USERIDTABLE_NAME']  # UserId
TIMEZONE_OFFSET = os.environ['TIMEZONE_OFFSET']  # -4

known_clients = {}


def current_date():
    return (datetime.utcnow() + timedelta(hours=(int(TIMEZONE_OFFSET) - 12))).isoformat()[0:10]


def current_time():
    return (datetime.utcnow() + timedelta(hours=-4)).strftime("%H:%M:%S")


# def get_current_users():
#     users_dict = {}
#     with ts3.query.TS3ServerConnection(TS_IP) as ts3conn:
#         ts3conn.exec_("login",  client_login_name=TS_USER, client_login_password=TS_PASS)
#         ts3conn.exec_("use", sid=1)
#         for user in ts3conn.query("clientlist").all():
#             if user['client_type'] != '1':
#                 users_dict[user['client_database_id']] = user['client_nickname']
#     return users_dict
#
#
# def diff_user_dicts(past_dict, new_dict):
#     left_dict = {}
#     joined_dict = {}
#     for uid in past_dict:
#         if uid not in new_dict:
#             left_dict[uid] = past_dict[uid]
#     for uid in new_dict:
#         if uid not in past_dict:
#             joined_dict[uid] = new_dict[uid]
#     return left_dict, joined_dict


# def list_user_connections():
#     res = client.query(
#         TableName=USER_CONNECTION_TABLE_NAME,
#         IndexName="date-index",
#         KeyConditionExpression="#date = :date",
#         ExpressionAttributeNames={
#             "#date": "date"
#         },
#         ExpressionAttributeValues={
#             ':date': {'S': current_date()}
#         }
#     )
#     return res['Items']


def user_left(user_id):
    res = client.query(
        TableName=USER_CONNECTION_TABLE_NAME,
        IndexName="user-off-index",
        KeyConditionExpression="#user = :user AND #off = :null",
        ExpressionAttributeNames={
            "#user": "user",
            "#off": "off"
        },
        ExpressionAttributeValues={
            ':user': {'N': user_id},
            ':null': {'S': 'null'}
        }
    )
    if len(res['Items']) > 0:
        user_connection_key = res['Items'][0]['uuid']
        client.update_item(
            TableName=USER_CONNECTION_TABLE_NAME,
            Key={'uuid': user_connection_key},
            UpdateExpression="SET off = :current_time",
            ExpressionAttributeValues={
                ':current_time': {'S': current_time()}
            }
        )


def user_joined(user_id):
    new_connection = {
        'uuid': str(uuid.uuid4()),
        'date': current_date(),
        'on': current_time(),
        'off': 'null',
        'user': int(user_id)
    }

    serialized = {}
    ser = TypeSerializer()
    for key, val in new_connection.items():
        serialized[key] = ser.serialize(val)

    client.put_item(
        TableName=USER_CONNECTION_TABLE_NAME,
        Item=serialized
    )


def register_user(user_id, name):
    new_user = {
        'uid': int(user_id),
        'name': name,
    }

    serialized = {}
    ser = TypeSerializer()
    for key, val in new_user.items():
        serialized[key] = ser.serialize(val)

    client.put_item(
        TableName=USERUID_TABLE_NAME,
        Item=serialized
    )


def connection_tracking_bot(ts3conn):
    # Register for the event.
    ts3conn.exec_("servernotifyregister", event="server")
    print("Listening for events...")

    while True:
        ts3conn.send_keepalive()

        try:
            # This method blocks, but we must sent the keepalive message at
            # least once in 10 minutes. So we set the timeout parameter to
            # 1 minutes, just to be ultra safe.
            event = ts3conn.wait_for_event(timeout=60)
        except ts3.query.TS3TimeoutError:
            pass
        else:
            if event[0]["reasonid"] == "0":
                # Entered
                known_clients[event[0]['clid']] = {'dbid': event[0]['client_database_id'], 'name': event[0]['client_nickname']}
                register_user(known_clients[event[0]['clid']]['dbid'], known_clients[event[0]['clid']]['name'])
                user_joined(known_clients[event[0]['clid']]['dbid'])
                print(f"User joined: {known_clients[event[0]['clid']]['dbid']} {known_clients[event[0]['clid']]['name']}")
            elif event[0]["reasonid"] == "8":
                # Left
                if event[0]['clid'] in known_clients:
                    user_left(known_clients[event[0]['clid']]['dbid'])
                    print(f"User left: {known_clients[event[0]['clid']]['dbid']} {known_clients[event[0]['clid']]['name']}")
                else:
                    print(f"Unkown user left, clid: {event[0]['clid']}")


if __name__ == "__main__":
    with ts3.query.TS3ServerConnection(TS_IP) as ts3conn:
        ts3conn.exec_("login", client_login_name=TS_USER, client_login_password=TS_PASS)
        ts3conn.exec_("use", sid=1)
        connection_tracking_bot(ts3conn)