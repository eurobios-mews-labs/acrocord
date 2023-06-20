# Copyright 2023 Eurobios
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#     http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and limitations under the License.

import asyncio
import pytest


def test_password_auth(get_connection):
    from acrocord.utils.connect import password_auth
    try:
        assert isinstance(password_auth(), str)
    except (PermissionError, FileNotFoundError):
        pass


def test_connect_literal():
    from acrocord.utils.connect import connect_psql_server
    str_ = "Engine(postgresql+psycopg2://user:***@localhost:5432/name)"

    assert str(connect_psql_server(
        username="user",
        connection=dict(
            password="pass",
            sslmode="require",
            host="localhost",
            port="5432",
            ssh=False, dbname="name"))) == str_


@pytest.mark.asyncio
async def test_connect_async_main(postgresql):
    from acrocord.utils.connect import connect_psql_server
    connect_psql_server(username=postgresql.info.user,
                        connection=dict(
                            password=" ",
                            sslmode="require",
                            host=postgresql.info.host,
                            port=postgresql.info.port,
                            ssh=False, dbname=postgresql.info.dbname), async_=True)
