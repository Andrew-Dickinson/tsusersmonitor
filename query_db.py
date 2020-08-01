import os
from datetime import datetime, timedelta
import boto3
from boto3.dynamodb.types import TypeDeserializer
import json

USER_CONNECTION_TABLE_NAME = os.environ['DYNAMODB_USERCONNECTIONTABLE_NAME']  # UserConnection
USERUID_TABLE_NAME = os.environ['DYNAMODB_USERIDTABLE_NAME']  # UserId
TIMEZONE_OFFSET = os.environ['TIMEZONE_OFFSET']  # -4

client = boto3.client('dynamodb')


def current_date():
    return (datetime.utcnow() + timedelta(hours=(int(TIMEZONE_OFFSET) - 12))).isoformat()[0:10]


def list_user_connections():
    res = client.query(
        TableName=USER_CONNECTION_TABLE_NAME,
        IndexName="date-index",
        KeyConditionExpression="#date = :date",
        ExpressionAttributeNames={
            "#date": "date"
        },
        ExpressionAttributeValues={
            ':date': {'S': current_date()}
        }
    )

    deserialized_list = []
    desr = TypeDeserializer()
    for item in res['Items']:
        deserialized = {}
        for key, val in item.items():
            deser_val = desr.deserialize(val)
            if key == 'user':
                deserialized[key] = int(deser_val)
            elif key == 'off':
                deserialized[key] = None if deser_val == 'null' else deser_val
            elif key == 'uuid':
                pass
            else:
                deserialized[key] = deser_val
        deserialized_list.append(deserialized)

    return deserialized_list


def list_users():
    res = client.scan(
        TableName=USERUID_TABLE_NAME,
    )

    deserialized_list = []
    desr = TypeDeserializer()
    for item in res['Items']:
        deserialized = {}
        for key, val in item.items():
            deser_val = desr.deserialize(val)
            if key == 'uid':
                deserialized[key] = int(deser_val)
            else:
                deserialized[key] = deser_val
        deserialized_list.append(deserialized)

    users_dict = {}
    for user in deserialized_list:
        users_dict[user['uid']] = user['name']

    return users_dict


def handler(event, context):
    result = {"connections": list_user_connections(), "users": list_users()}
    return result
