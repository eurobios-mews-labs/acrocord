# Copyright 2023 Eurobios
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.#
import getpass
import glob
import os
import subprocess
import sys
from os.path import expanduser, abspath

import asyncpg
import pandas as pd
from sqlalchemy import create_engine

home = expanduser("~")
local_path = "%s/.postgresql/" % home


def load_available_connections(path=local_path):
    import configparser
    default = configparser.ConfigParser()
    path_ = abspath(path)
    dir_path = "%s/connections.cfg" % path_
    default.read(dir_path)
    get = default._sections
    try:
        _ = pd.DataFrame(get).drop("password").T
    except:
        pass
    for connection in get.keys():
        try:
            if get[connection]["password"] == '':
                get[connection]["password"] = ' '
        except KeyError:
            pass
    return get


def password_auth():
    path = os.path.abspath(__file__).replace(os.path.basename(__file__), "")

    cmd = "python " + path + "getpassword.py " + local_path

    if not os.path.exists(local_path):
        os.mkdir(local_path)
    if sys.platform == "win32":
        with subprocess.Popen(cmd, shell=True,
                              stderr=subprocess.PIPE,
                              stdout=subprocess.PIPE):
            pass
    elif sys.platform == 'linux':
        subprocess.call(['xterm', "-e", cmd])
    list_of_files = glob.glob(local_path + '*')
    latest_file = max(list_of_files, key=os.path.getctime)

    with open(latest_file) as file:
        password = file.read()
        file.close()
    os.remove(latest_file)
    return password


def connect_psql_server(username=getpass.getuser(), async_=False, **kwargs):

    connection_param = kwargs["connection"].copy()

    if "password" not in connection_param.keys():
        connection_param["password"] = ""

    if username != getpass.getuser():
        connection_param["user"] = username

    if connection_param[
            "password"] == "" and "sslmode" not in connection_param.keys():
        connection_param["password"] = password_auth()

    if "sslmode" in connection_param.keys():
        connect_args = {'sslmode': connection_param["sslmode"]}

    else:
        connect_args = {}

    sql_cmd = 'postgresql+psycopg2://'
    sql_cmd += connection_param["user"] + ':' + connection_param[
        "password"] + '@' + connection_param["host"] + ':'
    sql_cmd += str(connection_param["port"]) + '/' + connection_param["dbname"]
    if async_:
        return connect_async(sql_cmd)
    print(sql_cmd)
    return create_engine(sql_cmd, connect_args=connect_args)


async def connect_async(sql_cmd):
    conn = await asyncpg.connect(
        sql_cmd.replace("postgresql+psycopg2", "postgresql"))
    return conn
