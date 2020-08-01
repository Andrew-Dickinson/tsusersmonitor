import os
import ts3

from ts_users_monitor import register_user, user_joined

TS_IP = os.environ['TEAMSPEAK_SERVER_HOST_STRING']  # "telnet://18.189.233.187:10011"
TS_USER = os.environ['TEAMSPEAK_CLIENT_LOGIN_NAME']  # monitoraccount
TS_PASS = os.environ['TEAMSPEAK_CLIENT_LOGIN_PASSWORD']  # XFrQlbxT

def get_current_users():
    users_dict = {}
    with ts3.query.TS3ServerConnection(TS_IP) as ts3conn:
        ts3conn.exec_("login",  client_login_name=TS_USER, client_login_password=TS_PASS)
        ts3conn.exec_("use", sid=1)
        for user in ts3conn.query("clientlist").all():
            if user['client_type'] != '1':
                users_dict[user['client_database_id']] = user['client_nickname']
    return users_dict


def handler(event, context):
    users_dict = get_current_users()
    for uid, name in users_dict.items():
        register_user(uid, name)
        user_joined(uid)